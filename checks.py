import json
import sqlite3
import sys
from collector import Collector, Injected
from portal import Portal, LostReply
import samples


def clone(db):
    # Connection replacement destroys all live decoder/lexer objects.
    other = sqlite3.connect(":memory:")
    db.backup(other)
    db.close()
    return other


def setup(case):
    remote, local = sqlite3.connect(":memory:"), sqlite3.connect(":memory:")
    portal = Portal(remote)
    payload, widths = case()
    portal.install("export", payload, widths)
    return local, portal


def drain(worker):
    while worker.step():
        pass
    return worker.read()


def normal():
    local, portal = setup(samples.normal)
    worker = Collector(local, portal, "export")
    assert drain(worker) == samples.NORMAL_EXPECTED
    assert worker.status()["finished"]
    assert portal.stats("export")["remote_bytes"] == 0
    assert not worker.step()
    # Reopening an already completed job is a valid idempotent read.
    reopened = Collector(clone(local), portal, "export")
    assert reopened.read() == samples.NORMAL_EXPECTED
    assert not reopened.step()
    local, portal = setup(samples.split_members)
    worker = Collector(local, portal, "export")
    assert drain(worker) == [["u", 1, "南\n井", 8]]
    print("NORMAL_REGRESSION_OK")


def repair():
    problems = []
    # Every transport boundary is a possible restart, including a header split.
    local, portal = setup(samples.restart)
    worker = Collector(local, portal, "export")
    try:
        while worker.step():
            local = clone(local)
            portal = Portal(clone(portal.db))
            worker = Collector(local, portal, "export")
        assert worker.read() == samples.RESTART_EXPECTED, worker.read()
    except Exception as exc:
        problems.append(["restart", type(exc).__name__, str(exc)])

    # ACK has committed remotely, its response was lost, both processes reopen.
    local, portal = setup(samples.normal)
    worker = Collector(local, portal, "export")
    try:
        try:
            worker.step(lose_ack=True)
        except LostReply:
            pass
        local = clone(local)
        portal = Portal(clone(portal.db))
        worker = Collector(local, portal, "export")
        assert drain(worker) == samples.NORMAL_EXPECTED
    except Exception as exc:
        problems.append(["lost_ack", type(exc).__name__, str(exc)])

    # Inject a durable-boundary interruption after at least one earlier response.
    for boundary in ("after_store", "after_ack", "after_apply", "after_commit"):
        local, portal = setup(samples.restart)
        worker = Collector(local, portal, "export")
        try:
            assert worker.step()
            try:
                worker.step(fail_at=boundary)
            except Injected:
                pass
            local = clone(local)
            portal = Portal(clone(portal.db))
            worker = Collector(local, portal, "export")
            assert drain(worker) == samples.RESTART_EXPECTED
        except Exception as exc:
            problems.append([boundary, type(exc).__name__, str(exc)])

    local, portal = setup(samples.corrupt)
    worker = Collector(local, portal, "export")
    try:
        assert worker.step()
        assert worker.read() == [["good", 1, "east", 7]]
        assert worker.step()
        assert worker.read() == [["good", 1, "east", 7]], worker.read()
        try:
            worker.step()
        except Exception:
            pass
        assert worker.status()["error"], worker.status()
        assert worker.read() == [["good", 1, "east", 7]], worker.read()
    except Exception as exc:
        problems.append(["integrity", type(exc).__name__, str(exc)])

    # One response contains both a valid member and an invalid-revision member.
    local, portal = setup(samples.conflict)
    worker = Collector(local, portal, "export")
    try:
        try:
            drain(worker)
        except Exception:
            pass
        assert worker.status()["error"], worker.status()
        assert worker.read() == [["good", 1, "east", 7]], worker.read()
    except Exception as exc:
        problems.append(["member_atomicity", type(exc).__name__, str(exc)])

    if problems:
        print("CSV_RESTART_INTEGRITY_MISMATCH " + json.dumps(problems, ensure_ascii=False))
        return 1
    print("REPAIR_REGRESSION_OK")
    return 0


def demo():
    local, portal = setup(samples.normal)
    worker = Collector(local, portal, "export")
    drain(worker)
    print(json.dumps({"records": worker.read(), "local": worker.status(),
                      "portal": portal.stats("export")}, ensure_ascii=False))


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) == 2 else "verify"
    if mode == "normal":
        normal()
    elif mode == "repair":
        sys.exit(repair())
    elif mode == "verify":
        normal()
        sys.exit(repair())
    elif mode == "demo":
        demo()
    else:
        raise SystemExit("normal | repair | verify | demo")
