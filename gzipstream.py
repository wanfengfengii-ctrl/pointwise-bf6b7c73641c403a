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


class MemberGzipStream:
    """Incremental concatenated-gzip reader with explicit member boundaries.

    feed() returns (raw, payload, member_end) triples in input order: raw is
    the exact compressed input slice the event consumed, payload the
    decompressed byte piece (possibly empty), and member_end marks the point
    where a member's CRC and length have just been verified.  The caller can
    therefore publish data only after the member that completes it validates,
    and retain exactly the raw bytes of the still unverified member.
    """

    def __init__(self):
        self.decoder = zlib.decompressobj(31)
        self.at_boundary = False
        self.members = 0

    def feed(self, data):
        # Decompression is chunked by output size and events are yielded
        # lazily, so one response never materializes more than a bounded
        # piece of a member in memory, however compressible the payload is.
        while data:
            if self.at_boundary:
                self.decoder = zlib.decompressobj(31)
                self.at_boundary = False
            buf, data = data, b""
            while buf:
                out = self.decoder.decompress(buf, 262144)
                if self.decoder.eof:
                    consumed = len(buf) - len(self.decoder.unused_data)
                    yield buf[:consumed], out, True
                    self.members += 1
                    self.at_boundary = True
                    data = self.decoder.unused_data
                    break
                consumed = len(buf) - len(self.decoder.unconsumed_tail)
                yield buf[:consumed], out, False
                buf = self.decoder.unconsumed_tail

    def finish(self):
        if not self.at_boundary or not self.members:
            raise ValueError("TRUNCATED_GZIP")
