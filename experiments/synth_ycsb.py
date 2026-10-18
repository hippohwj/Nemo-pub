#!/usr/bin/env python3
"""Synthesize obj-100g YCSB rows byte-identically to the C++ loader (plan A).

The engine's rows differ only in the primary key: LE uint64 at bytes [0,8),
"init" at [8,12), zeros elsewhere (g_record_size=1008); RowKey is
"0" + zero-padded 8-digit decimal (SearchKey::ToString, non-PGBUF path);
PartitionKey is str(key // 10000). Nothing here hardcodes those facts blindly:
learn() derives the template and the PK encoding from a reference TSV produced
by the real C++ dump hook (dump_ycsb_ref.sh) and gen() is only trusted after
selftest() reproduces every reference row byte-for-byte. Run as the S3 gate:

  synth_ycsb.py --selftest ref_head.tsv ref_mid.tsv ref_tail.tsv
"""
import struct
import sys

PART_SZ = 10000  # g_partition_sz; must match the engine and the dump


class Synth:
    def __init__(self, template, tbl_id):
        self.template = template
        self.tbl_id = tbl_id

    @staticmethod
    def rowkey(key):
        return "0%08d" % key  # SearchKey::ToString: "0" + 8-char decimal

    def gen(self, key):
        """-> (partition, rowkey, data_bytes) for one key."""
        data = struct.pack("<Q", key) + self.template[8:]
        return str(key // PART_SZ), self.rowkey(key), data

    def tsv_line(self, key):
        part, rk, data = self.gen(key)
        return "%d\t%s\t%s\t%s" % (self.tbl_id, part, rk, data.hex())


def _parse(path):
    rows = []
    with open(path) as fh:
        for ln in fh:
            tbl, part, rk, hx = ln.rstrip("\n").split("\t")
            rows.append((int(tbl), part, rk, hx, ln.rstrip("\n")))
    return rows


def learn(ref_path):
    """Derive template + invariants from one reference TSV."""
    rows = _parse(ref_path)
    tbl, part0, rk0, hx0, _ = rows[0]
    data0 = bytes.fromhex(hx0)
    key0 = int(rk0)  # rowkey embeds the key in decimal
    # PK encoding hypothesis: LE uint64 in the first 8 bytes
    if struct.unpack("<Q", data0[:8])[0] != key0:
        raise AssertionError("PK is not LE uint64 at [0,8) — layout changed, "
                             "plan A invalid; fall back to plan B (dump tsv)")
    template = b"\x00" * 8 + data0[8:]
    return Synth(template, tbl)


def selftest(ref_paths):
    synth = learn(ref_paths[0])
    total = 0
    for path in ref_paths:
        for tbl, part, rk, hx, raw in _parse(path):
            key = int(rk)
            if synth.tsv_line(key) != raw:
                print("MISMATCH at key %d in %s" % (key, path))
                print("  ref:   %s..." % raw[:80])
                print("  synth: %s..." % synth.tsv_line(key)[:80])
                return False
            total += 1
    print("selftest OK: %d reference rows reproduced byte-exactly "
          "(template %d bytes, tbl %d)" % (total, len(synth.template),
                                           synth.tbl_id))
    return True


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--selftest":
        sys.exit(0 if selftest(sys.argv[2:]) else 1)
    print(__doc__)
    sys.exit(2)
