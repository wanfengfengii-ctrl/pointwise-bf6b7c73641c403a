"""Durable RPA export importer."""
import codecs
import hashlib
import sqlite3
from csvstream import CSVStream
from gzipstream import MemberGzipStream


class Injected(RuntimeError):
    pass


class Collector:
    def __init__(self, db, portal, job):
        self.db = sqlite3.connect(db) if isinstance(db, str) else db
        self.portal, self.job = portal, job
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS progress(
            job TEXT PRIMARY KEY, next_seq INTEGER, finished INTEGER,
            csv TEXT, error TEXT);
        CREATE TABLE IF NOT EXISTS inbox(
            job TEXT, seq INTEGER, body BLOB, final INTEGER, PRIMARY KEY(job,seq));
        CREATE TABLE IF NOT EXISTS records(
            job TEXT, id TEXT, rev INTEGER, station TEXT, reading TEXT, deleted INTEGER,
            PRIMARY KEY(job,id));
        CREATE TABLE IF NOT EXISTS segments(
            job TEXT, member INTEGER, pos INTEGER, body BLOB NOT NULL,
            PRIMARY KEY(job,member,pos));
        CREATE TABLE IF NOT EXISTS staging(
            job TEXT, member INTEGER, ord INTEGER, id TEXT, rev INTEGER,
            station TEXT, reading TEXT, deleted INTEGER,
            PRIMARY KEY(job,member,ord));
        CREATE TABLE IF NOT EXISTS frontier(
            job TEXT PRIMARY KEY, utf8 BLOB NOT NULL, member INTEGER NOT NULL);
        """)
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO progress VALUES(?,0,0,?,NULL)",
                            (job, CSVStream().checkpoint()))
        state = self.db.execute("SELECT csv FROM progress WHERE job=?", (job,)).fetchone()[0]
        row = self.db.execute("SELECT utf8,member FROM frontier WHERE job=?",
                              (job,)).fetchone()
        self.utf8_pending = bytes(row[0]) if row else b""
        # csv_base/utf8_pending/member describe the durable frontier: the
        # decoder and lexer state exactly after the last verified member.
        # Live objects are rebuilt from that frontier plus the retained raw
        # bytes of the current unverified member, never from the task start
        # and never refetched.
        self.csv_base = state
        self.csv = CSVStream(state)
        self.text = codecs.getincrementaldecoder("utf-8")("strict")
        if self.utf8_pending:
            self.text.decode(self.utf8_pending)
        self.gzip = MemberGzipStream()
        self.member = row[1] if row else 0
        self.staged = 0
        segments = self.db.execute(
            "SELECT member,pos,body FROM segments WHERE job=? ORDER BY member,pos",
            (self.job,))
        with self.db:
            self.db.execute("DELETE FROM staging WHERE job=?", (self.job,))
            for member, pos, body in segments:
                if member != self.member:
                    raise RuntimeError("RESUME_GAP")
                self._consume(self.gzip.feed(bytes(body)), None)

    def _apply(self, rows):
        for ident, rev, station, reading, deleted in rows:
            old = self.db.execute(
                "SELECT rev,station,reading,deleted FROM records WHERE job=? AND id=?",
                (self.job, ident)).fetchone()
            value = (rev, station, str(reading), int(deleted))
            if old is not None and old[0] > rev:
                continue
            if old is not None and old[0] == rev:
                if old != value:
                    raise ValueError("REVISION_CONFLICT")
                continue
            self.db.execute("INSERT OR REPLACE INTO records VALUES(?,?,?,?,?,?)",
                            (self.job, ident, *value))

    def _consume(self, events, collect):
        """Feed gzip events through UTF-8/CSV; stage rows of the open member.

        Rows completed by a member stay in staging until that member's CRC and
        length verify; only then are they published.  collect, when given,
        accumulates the raw bytes still belonging to the unverified member.
        """
        for raw, payload, member_end in events:
            if collect is not None:
                collect += raw
            if payload:
                text = self.text.decode(payload)
                for row in self.csv.feed(text):
                    self.db.execute("INSERT INTO staging VALUES(?,?,?,?,?,?,?,?)",
                                    (self.job, self.member, self.staged, row[0], row[1],
                                     row[2], str(row[3]), int(row[4])))
                    self.staged += 1
            if member_end:
                self._publish_member()
                if collect is not None:
                    # Verified bytes are no longer needed for recovery.
                    del collect[:]

    def _publish_member(self):
        """A member's CRC/length just verified: publish the rows it completed."""
        rows = self.db.execute(
            "SELECT id,rev,station,reading,deleted FROM staging"
            " WHERE job=? AND member=? ORDER BY ord",
            (self.job, self.member))
        self.db.execute("SAVEPOINT member_publish")
        try:
            self._apply(rows)
        except Exception:
            # A failed member stays invisible: none of its rows, tombstones or
            # revisions may leak, including lines applied before the error.
            self.db.execute("ROLLBACK TO member_publish")
            self.db.execute("RELEASE member_publish")
            raise
        self.db.execute("RELEASE member_publish")
        self.db.execute("DELETE FROM staging WHERE job=? AND member=?",
                        (self.job, self.member))
        self.db.execute("DELETE FROM segments WHERE job=? AND member=?",
                        (self.job, self.member))
        self.member += 1
        self.staged = 0
        self.csv_base = self.csv.checkpoint()
        self.utf8_pending = bytes(self.text.getstate()[0])

    def _parse(self, seq, body, final, fail_at):
        """Fully process one fetched response inside the caller's transaction."""
        pending = bytearray()
        self._consume(self.gzip.feed(body), pending)
        if final:
            self.gzip.finish()
            tail = self.text.decode(b"", final=True)
            for row in self.csv.feed(tail, final=True):
                self.db.execute("INSERT INTO staging VALUES(?,?,?,?,?,?,?,?)",
                                (self.job, self.member, self.staged, row[0], row[1],
                                 row[2], str(row[3]), int(row[4])))
                self.staged += 1
            if self.staged:
                raise ValueError("CSV_UNTERMINATED")
        if pending:
            pos = self.db.execute(
                "SELECT coalesce(max(pos)+1,0) FROM segments WHERE job=? AND member=?",
                (self.job, self.member)).fetchone()[0]
            self.db.execute("INSERT INTO segments VALUES(?,?,?,?)",
                            (self.job, self.member, pos, bytes(pending)))
        if final:
            self.db.execute("DELETE FROM segments WHERE job=?", (self.job,))
        self.db.execute("UPDATE progress SET next_seq=?,finished=?,csv=? WHERE job=?",
                        (seq + 1, int(final), self.csv_base, self.job))
        self.db.execute("INSERT OR REPLACE INTO frontier VALUES(?,?,?)",
                        (self.job, self.utf8_pending, self.member))
        self.db.execute("DELETE FROM inbox WHERE job=? AND seq=?", (self.job, seq))
        if fail_at == "after_apply":
            raise Injected(fail_at)

    def _process(self, seq, body, final, fail_at, lose_ack):
        # Remote calls never run while a local write transaction is open; the
        # ack is idempotent, so a recovered response can be confirmed again.
        self.portal.ack(self.job, seq, hashlib.sha256(body).hexdigest(),
                        lose_reply=lose_ack)
        if fail_at == "after_ack":
            raise Injected(fail_at)
        error = None
        with self.db:
            try:
                self._parse(seq, body, final, fail_at)
            except Injected:
                raise
            except Exception as exc:
                # Commit the valid prefix: members of this response verified
                # before the failure stay published; only the failed member's
                # staged rows and retained bytes are dropped, and the failure
                # itself becomes durable in the same transaction.
                error = exc
                self.db.execute("DELETE FROM staging WHERE job=?", (self.job,))
                self.db.execute("DELETE FROM segments WHERE job=?", (self.job,))
                self.db.execute(
                    "UPDATE progress SET next_seq=?,csv=?,error=? WHERE job=?",
                    (seq + 1, self.csv_base, str(exc), self.job))
                self.db.execute("INSERT OR REPLACE INTO frontier VALUES(?,?,?)",
                                (self.job, self.utf8_pending, self.member))
                self.db.execute("DELETE FROM inbox WHERE job=? AND seq=?",
                                (self.job, seq))
        if error is not None:
            raise error
        if fail_at == "after_commit":
            raise Injected(fail_at)

    def step(self, fail_at=None, lose_ack=False):
        state = self.db.execute("SELECT next_seq,finished,error FROM progress WHERE job=?",
                                (self.job,)).fetchone()
        if state[2]:
            raise RuntimeError("JOB_FAILED:" + state[2])
        if state[1]:
            return False
        item = self.db.execute(
            "SELECT seq,body,final FROM inbox WHERE job=? ORDER BY seq LIMIT 1",
            (self.job,)).fetchone()
        if item is not None:
            # A previous step stopped between remote ack and local commit:
            # resume from the local copy instead of re-fetching confirmed bytes.
            seq, body, final = item
            self._process(seq, bytes(body), bool(final), fail_at, lose_ack)
            return True
        remote = self.portal.fetch(self.job)
        if remote is None:
            raise RuntimeError("ACKED_GAP")
        item = (remote["seq"], remote["body"], int(remote["final"]))
        with self.db:
            self.db.execute("INSERT INTO inbox VALUES(?,?,?,?)", (self.job, *item))
        if fail_at == "after_store":
            raise Injected(fail_at)
        seq, body, final = item
        if seq != state[0]:
            raise RuntimeError("LOCAL_GAP")
        self._process(seq, body, bool(final), fail_at, lose_ack)
        return True

    def read(self):
        rows = self.db.execute(
            "SELECT id,rev,station,reading,deleted FROM records WHERE job=? AND deleted=0 ORDER BY id",
            (self.job,)).fetchall()
        return [[a, b, c, int(d)] for a, b, c, d, _ in rows]

    def status(self):
        seq, done, error = self.db.execute(
            "SELECT next_seq,finished,error FROM progress WHERE job=?", (self.job,)).fetchone()
        return {"job": self.job, "next_seq": seq, "finished": bool(done), "error": error}
