"""Streaming CSV lexer. Its state is deliberately plain JSON data."""
import json

HEADER = ["id", "rev", "station", "reading", "deleted"]


class CSVStream:
    def __init__(self, checkpoint=None):
        self.s = json.loads(checkpoint) if checkpoint else {
            "mode": "start", "field": "", "fields": [], "skip_lf": False, "header": False,
            "bom": True}

    def checkpoint(self):
        return json.dumps(self.s, ensure_ascii=False, sort_keys=True)

    def _field(self):
        self.s["fields"].append(self.s["field"])
        self.s["field"] = ""
        self.s["mode"] = "start"
        if len(self.s["fields"]) > 5:
            raise ValueError("CSV_WIDTH")

    def _row(self):
        self._field()
        row = self.s["fields"]
        self.s["fields"] = []
        if len(row) != 5:
            raise ValueError("CSV_WIDTH")
        if not self.s["header"]:
            if row != HEADER:
                raise ValueError("CSV_HEADER")
            self.s["header"] = True
            return None
        ident, rev, station, reading, deleted = row
        if (not ident or not station or not rev.isascii() or not rev.isdecimal()
                or len(rev) > 19 or not 1 <= int(rev) <= 9223372036854775807 or deleted not in ("0", "1")):
            raise ValueError("CSV_VALUE")
        if not reading or (reading[0] == "-" and not reading[1:]) or not reading.lstrip("-").isascii():
            raise ValueError("CSV_VALUE")
        if not reading.lstrip("-").isdecimal() or reading.count("-") > 1 or "-" in reading[1:]:
            raise ValueError("CSV_VALUE")
        if len(reading) > 20 or not -9223372036854775808 <= int(reading) <= 9223372036854775807:
            raise ValueError("CSV_VALUE")
        return [ident, int(rev), station, int(reading), deleted == "1"]

    def feed(self, text, final=False):
        result = []
        for char in text:
            if self.s["bom"]:
                self.s["bom"] = False
                if char == "\ufeff":
                    continue
            if self.s["skip_lf"]:
                self.s["skip_lf"] = False
                if char == "\n":
                    continue
            mode = self.s["mode"]
            if mode == "quoted":
                if char == '"':
                    self.s["mode"] = "after_quote"
                else:
                    self.s["field"] += char
            elif mode == "after_quote" and char == '"':
                self.s["field"] += char
                self.s["mode"] = "quoted"
            elif char in ("\r", "\n"):
                row = self._row()
                if row is not None:
                    result.append(row)
                self.s["skip_lf"] = char == "\r"
            elif char == ",":
                self._field()
            elif mode == "after_quote":
                raise ValueError("CSV_AFTER_QUOTE")
            elif char == '"':
                if mode != "start":
                    raise ValueError("CSV_QUOTE")
                self.s["mode"] = "quoted"
            else:
                self.s["field"] += char
                self.s["mode"] = "plain"
            if len(self.s["field"].encode("utf-8")) > 65536:
                raise ValueError("CSV_FIELD_LIMIT")
        if final and (not self.s["header"] or self.s["fields"] or self.s["mode"] != "start"):
            raise ValueError("CSV_UNTERMINATED")
        return result
