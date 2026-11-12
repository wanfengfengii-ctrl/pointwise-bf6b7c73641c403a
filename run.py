"""Disk-backed replay entry; see CONTRACT.md."""
import json
import sys
from collector import Collector
from portal import Portal
import samples


def main(argv):
    if len(argv) != 4 or argv[0] not in ("install", "step", "read", "status"):
        raise ValueError("USAGE: run.py install|step|read|status LOCAL_DB PORTAL_DB CASE")
    command, local, remote, case = argv
    if case not in ("normal", "restart", "corrupt", "conflict", "split_members"):
        raise ValueError("UNKNOWN_CASE")
    portal = Portal(remote)
    if command == "install":
        payload, widths = getattr(samples, case)()
        portal.install(case, payload, widths)
        result = {"job": case, "installed": True}
    else:
        worker = Collector(local, portal, case)
        if command == "step":
            result = {"advanced": worker.step(), "status": worker.status(), "records": worker.read()}
        elif command == "read":
            result = worker.read()
        else:
            result = {"local": worker.status(), "portal": portal.stats(case)}
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except Exception as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False))
        sys.exit(2)
