"""Recordings damaged by a bot killed mid-write: an unfinished gzip member followed by the next start's member.

Found in real data: a standard gzip reader fails at the join ("invalid block type") and read_records used to crash on
it, or stop at the break and lose the rest of the day.
"""
import gzip
import json
import tempfile
import unittest
import zlib
from pathlib import Path

from arbbot.recorder import read_records


def member(records, finished=True) -> bytes:
    """One gzip member as the recorder writes it; finished=False is a bot killed after a flush, before closing."""
    raw = "".join(json.dumps(r) + "\n" for r in records).encode()
    if finished:
        return gzip.compress(raw)
    c = zlib.compressobj(5, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
    return c.compress(raw) + c.flush(zlib.Z_SYNC_FLUSH)          # flushed, but no gzip end


def recs(a, b):
    return [{"t": float(k), "s": f"r{k}"} for k in range(a, b)]


class DamagedRecordingTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())

    def write(self, name, *parts) -> Path:
        p = self.dir / name
        p.write_bytes(b"".join(parts))
        return p

    def test_a_cut_off_member_in_the_middle_loses_nothing_after_it(self):
        p = self.write("quotes-2026-01-01.jsonl.gz", member(recs(0, 50)), member(recs(50, 80), finished=False),
                       member(recs(80, 120)))
        with self.assertRaises(zlib.error):                            # what the old reader ran into
            with gzip.open(p, "rt") as fh:
                for _ in fh:
                    pass
        got = [r["t"] for r in read_records([p])]
        self.assertEqual(got, [float(k) for k in range(120)])         # every flushed record, in order

    def test_several_breaks_and_a_file_still_being_written(self):
        p = self.write("quotes-2026-01-02.jsonl.gz", member(recs(0, 10), finished=False), member(recs(10, 20)),
                       member(recs(20, 30), finished=False), member(recs(30, 40), finished=False))
        self.assertEqual(len(list(read_records([p]))), 40)

    def test_clean_files_and_appended_restarts_read_as_before(self):
        p = self.write("quotes-2026-01-03.jsonl.gz", member(recs(0, 5)), member(recs(5, 9)))
        self.assertEqual([r["t"] for r in read_records([p])], [float(k) for k in range(9)])

    def test_a_half_written_line_is_skipped_not_fatal(self):
        raw = (json.dumps({"t": 1.0}) + "\n" + '{"t": 2.0, "s": "cut').encode()
        c = zlib.compressobj(5, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
        p = self.write("quotes-2026-01-04.jsonl.gz", c.compress(raw) + c.flush(zlib.Z_SYNC_FLUSH), member(recs(3, 5)))
        self.assertEqual([r["t"] for r in read_records([p])], [1.0, 3.0, 4.0])

    def test_files_are_read_in_date_order(self):
        b = self.write("quotes-2026-01-06.jsonl.gz", member(recs(10, 12)))
        a = self.write("quotes-2026-01-05.jsonl.gz", member(recs(0, 2)))
        self.assertEqual([r["t"] for r in read_records([b, a])], [0.0, 1.0, 10.0, 11.0])


if __name__ == "__main__":
    unittest.main()
