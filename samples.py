"""Concrete recorded exports; gzip construction is deterministic."""
import gzip

HEADER = "id,rev,station,reading,deleted\r\n"
NORMAL = HEADER + 'a,1,east,10,0\r\nb,1,west,4,0\r\na,2,east,12,0\r\nb,2,west,0,1\r\n'
NORMAL_EXPECTED = [["a", 2, "east", 12]]
RESTART = HEADER + "".join(
    '%s,1,"north,%s\nwell",%s,0\r\n' % ("r%02d" % i, "水源", i)
    for i in range(18))
RESTART_EXPECTED = [["r%02d" % i, 1, "north,水源\nwell", i] for i in range(18)]
GOOD_FIRST = HEADER + "good,1,east,7,0\r\n"
BAD_NEXT = "poison,1,west,99,0\r\n"
CONFLICT_NEXT = "temp,1,west,5,0\r\ngood,1,east,8,0\r\n"


def pack(text):
    return gzip.compress(text.encode("utf-8"), compresslevel=6, mtime=0)


def normal():
    return pack(NORMAL), (11, 7, 23)


def restart():
    return pack(RESTART), (13, 9, 17)


def corrupt():
    first = pack(GOOD_FIRST)
    second = bytearray(pack(BAD_NEXT))
    second[-8] ^= 1
    # Deliver the trailer separately, so data emitted before verification is observable.
    return first + second, (len(first), len(second) - 8, 8)


def conflict():
    return pack(GOOD_FIRST) + pack(CONFLICT_NEXT), (65536,)


def split_members():
    raw = (HEADER + 'u,1,"南\n井",8,0\r\n').encode("utf-8")
    marker = raw.index("南".encode("utf-8")) + 1
    # A single UTF-8 character and a quoted CSV row cross gzip boundaries.
    return b"".join(gzip.compress(x, mtime=0)
                    for x in (raw[:marker], raw[marker:marker + 4], raw[marker + 4:])), (1, 5, 19)
