import io
import tempfile
import unittest
from pathlib import Path
from mpi_rank_logs import Collector, decode_line

class ParserTests(unittest.TestCase):
    def test_formats(self):
        cases = [
            (b'[1,2]<stdout>:hello\n', (2, 'stdout', b'hello')),
            (b'[2]<stderr>:oops\n', (2, 'stderr', b'oops')),
            (b'[job.namespace,10][node001:345]<stdout>:x\n', (10, 'stdout', b'x')),
            (b'[2026-10-30T12:30:00][1,3]<stdout>:t\n', (3, 'stdout', b't')),
            (b'not tagged\n', None),
        ]
        for line, expected in cases:
            with self.subTest(line=line):
                self.assertEqual(decode_line(line), expected)

    def test_rank_sort_and_channel(self):
        with tempfile.TemporaryDirectory() as folder:
            c = Collector(Path(folder))
            for line in (b'[1,2]<stdout>:C1\n', b'[1,0]<stdout>:A1\n',
                         b'[1,2]<stderr>:warn\n', b'[1,0]<stdout>:A2\n'):
                c.feed(line)
            out = io.BytesIO()
            c.finish(out, True, False)
            result = out.getvalue()
            self.assertLess(result.index(b'RANK 0'), result.index(b'RANK 2'))
            self.assertIn(b'A1\nA2', result)
            self.assertIn(b'C1\n[stderr] warn', result)

if __name__ == '__main__':
    unittest.main()
