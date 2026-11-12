"""Incremental concatenated-gzip reader used by the importer."""
import zlib


class GzipStream:
    def __init__(self):
        self.decoder = zlib.decompressobj(31)
        self.at_boundary = False
        self.members = 0

    def feed(self, data, final=False):
        parts = []
        while data:
            if self.at_boundary:
                self.decoder = zlib.decompressobj(31)
                self.at_boundary = False
            parts.append(self.decoder.decompress(data))
            if self.decoder.eof:
                self.members += 1
                self.at_boundary = True
                data = self.decoder.unused_data
            else:
                data = b""
        if final and (not self.at_boundary or not self.members):
            raise ValueError("TRUNCATED_GZIP")
        return b"".join(parts)
