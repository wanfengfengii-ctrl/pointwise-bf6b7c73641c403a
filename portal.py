"""A recorded RPA export endpoint. ACK really deletes the delivered bytes."""
import hashlib
import sqlite3


class LostReply(RuntimeError):
    pass


class Portal:
    def __init__(self, db):
        self.db = sqlite3.connect(db) if isinstance(db, str) else db
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS jobs(
            job TEXT PRIMARY KEY, cursor INTEGER NOT NULL, done INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS chunks(
            job TEXT, seq INTEGER, body BLOB NOT NULL, final INTEGER NOT NULL,
            PRIMARY KEY(job,seq));
        CREATE TABLE IF NOT EXISTS acks(
            job TEXT, seq INTEGER, digest TEXT NOT NULL, PRIMARY KEY(job,seq));
        """)

    def install(self, job, payload, widths=(64,)):
        if not payload or not widths or any(type(n) is not int or not 1 <= n <= 65536 for n in widths):
            raise ValueError("BAD_RECORDING")
        with self.db:
            self.db.execute("INSERT INTO jobs VALUES(?,0,0)", (job,))
            offset = seq = 0
            while offset < len(payload):
                end = min(len(payload), offset + widths[seq % len(widths)])
                self.db.execute("INSERT INTO chunks VALUES(?,?,?,?)",
                                (job, seq, payload[offset:end], int(end == len(payload))))
                offset, seq = end, seq + 1

    def fetch(self, job):
        state = self.db.execute("SELECT cursor,done FROM jobs WHERE job=?", (job,)).fetchone()
        if state is None:
            raise ValueError("UNKNOWN_JOB")
        if state[1]:
            return None
        row = self.db.execute("SELECT body,final FROM chunks WHERE job=? AND seq=?",
                              (job, state[0])).fetchone()
        if row is None:
            raise RuntimeError("REMOTE_GAP")
        return {"seq": state[0], "body": bytes(row[0]), "final": bool(row[1])}

    def ack(self, job, seq, digest, lose_reply=False):
        with self.db:
            old = self.db.execute("SELECT digest FROM acks WHERE job=? AND seq=?",
                                  (job, seq)).fetchone()
            if old is not None:
                if old[0] != digest:
                    raise ValueError("ACK_CONFLICT")
            else:
                row = self.db.execute("SELECT body,final FROM chunks WHERE job=? AND seq=?",
                                      (job, seq)).fetchone()
                cursor = self.db.execute("SELECT cursor FROM jobs WHERE job=?", (job,)).fetchone()
                if row is None or cursor != (seq,) or hashlib.sha256(row[0]).hexdigest() != digest:
                    raise ValueError("ACK_CONFLICT")
                self.db.execute("INSERT INTO acks VALUES(?,?,?)", (job, seq, digest))
                self.db.execute("DELETE FROM chunks WHERE job=? AND seq=?", (job, seq))
                self.db.execute("UPDATE jobs SET cursor=?,done=? WHERE job=?",
                                (seq + 1, row[1], job))
        if lose_reply:
            raise LostReply("ACK_RESPONSE_LOST")

    def stats(self, job):
        state = self.db.execute("SELECT cursor,done FROM jobs WHERE job=?", (job,)).fetchone()
        remaining = self.db.execute("SELECT coalesce(sum(length(body)),0) FROM chunks WHERE job=?",
                                    (job,)).fetchone()[0]
        return {"acknowledged": state[0], "done": bool(state[1]), "remote_bytes": remaining}
