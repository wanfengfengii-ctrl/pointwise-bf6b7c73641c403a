"""Durable RPA export importer."""
import codecs
import hashlib
import json
import sqlite3
from csvstream import CSVStream
from gzipstream import GzipStream


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
        """)
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO progress VALUES(?,0,0,?,NULL)",
                            (job, CSVStream().checkpoint()))
        state = self.db.execute("SELECT csv FROM progress WHERE job=?", (job,)).fetchone()[0]
        self.csv = CSVStream(state)
        self.text = codecs.getincrementaldecoder("utf-8")("strict")
        self.gzip = GzipStream()

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
        if item is None:
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
        self.portal.ack(self.job, seq, hashlib.sha256(body).hexdigest(), lose_reply=lose_ack)
        if fail_at == "after_ack":
            raise Injected(fail_at)
        try:
            decoded = self.gzip.feed(body, bool(final))
            text = self.text.decode(decoded, final=bool(final))
            rows = self.csv.feed(text, final=bool(final))
            with self.db:
                self._apply(rows)
                self.db.execute("UPDATE progress SET next_seq=?,finished=?,csv=? WHERE job=?",
                                (seq + 1, final, self.csv.checkpoint(), self.job))
                self.db.execute("DELETE FROM inbox WHERE job=? AND seq=?", (self.job, seq))
                if fail_at == "after_apply":
                    raise Injected(fail_at)
        except Injected:
            raise
        except Exception as exc:
            with self.db:
                self.db.execute("UPDATE progress SET error=? WHERE job=?", (str(exc), self.job))
            raise
        if fail_at == "after_commit":
            raise Injected(fail_at)
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
