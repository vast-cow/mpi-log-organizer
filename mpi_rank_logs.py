#!/usr/bin/env python3
"""Collect Open MPI tagged output and print it sequentially by rank.

Modes:
  parse: analyze an existing tagged log (file or stdin)
  run:   launch a tagged MPI job and collect its output
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import ExitStack
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from typing import BinaryIO

# Open MPI: [job,rank]<stdout>:text, [rank]<stderr>:text,
# or [namespace,rank][hostname:pid]<stdout>:text.
# Optional timestamp (e.g. [2026-10-30 12:00:00]) may precede the tag.
TAIL = re.compile(
    rb"^\[(?P<identity>[^\]\r\n]+)\]"
    rb"(?:\[(?P<node>[^\]\r\n]+)\])?"
    rb"<(?P<stream>stdout|stderr|stddiag)>:(?P<message>.*)$",
    re.IGNORECASE,
)
TIME_PREFIX = re.compile(rb"^\[[^\]\r\n]+\](?:<\w+>:)?")


def decode_line(line: bytes):
    line = line.rstrip(b"\r\n")
    # Prefer a rank-bearing tag at the start; timestamps are detected only if needed.
    match = TAIL.match(line)
    if match is None or not match.group("identity").rsplit(b",", 1)[-1].strip().isdigit():
        prefix = TIME_PREFIX.match(line)
        match = TAIL.match(line[prefix.end():]) if prefix else None
    if match is None:
        return None
    ident = match.group("identity")
    rank_part = ident.rsplit(b",", 1)[-1].strip()
    if not rank_part.isdigit():
        return None
    return int(rank_part), match.group("stream").decode("ascii").lower(), match.group("message")


class Collector:
    def __init__(self, tmpdir: Path):
        self.tmpdir = tmpdir
        self.handles: dict[int, BinaryIO] = {}
        self.lines = Counter()
        self.streams = Counter()
        self.unparsed = 0
        self.unknown = open(tmpdir / 'unparsed.bin', 'wb')

    def feed(self, line: bytes):
        result = decode_line(line)
        if result is None:
            self.unknown.write(line)
            self.unparsed += 1
            return
        rank, stream, message = result
        if rank not in self.handles:
            self.handles[rank] = open(self.tmpdir / f'rank_{rank}.bin', 'wb')
        # Keep stderr identification, while stripping MPI runtime tags.
        prefix = b'' if stream == 'stdout' else f'[{stream}] '.encode()
        self.handles[rank].write(prefix + message + b'\n')
        self.lines[rank] += 1
        self.streams[rank, stream] += 1

    def finish(self, out: BinaryIO, show_unparsed: bool, show_stats: bool):
        for fp in self.handles.values():
            fp.close()
        self.unknown.close()
        for rank in sorted(self.handles):
            out.write(f'===== RANK {rank} ({self.lines[rank]} lines) =====\n'.encode())
            with open(self.tmpdir / f'rank_{rank}.bin', 'rb') as src:
                shutil.copyfileobj(src, out)
            out.write(b'\n')
        if show_unparsed and self.unparsed:
            out.write(f'===== UNTAGGED ({self.unparsed} lines) =====\n'.encode())
            with open(self.tmpdir / 'unparsed.bin', 'rb') as src:
                shutil.copyfileobj(src, out)
        if show_stats:
            print(f'ranks={len(self.handles)}, tagged_lines={sum(self.lines.values())}, '
                  f'untagged_lines={self.unparsed}', file=sys.stderr)
            for rank in sorted(self.handles):
                print(f'rank {rank}: {self.lines[rank]} lines '
                      f'(stdout={self.streams[rank,"stdout"]}, '
                      f'stderr={self.streams[rank,"stderr"]}, '
                      f'stddiag={self.streams[rank,"stddiag"]})', file=sys.stderr)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-o', '--output', type=Path, help='write combined result to file (default: stdout)')
    parser.add_argument('--stats', action='store_true', help='write rank / stream counts to stderr')
    parser.add_argument('--hide-untagged', action='store_true', help='suppress non-tagged lines (normally shown at end)')
    sub = parser.add_subparsers(dest='mode', required=True)
    p = sub.add_parser('parse', help='read an existing Open MPI tagged log')
    p.add_argument('input', nargs='?', type=Path, help='log file; default: stdin')
    r = sub.add_parser('run', help='execute an MPI command (tag option must be included)')
    r.add_argument('command', nargs=argparse.REMAINDER, help='e.g. -- mpirun --output tag -np 4 ./app')
    args = parser.parse_args(argv)

    with tempfile.TemporaryDirectory(prefix='mpi-rank-logs-') as folder, ExitStack() as stack:
        collector = Collector(Path(folder))
        if args.mode == 'parse':
            src = stack.enter_context(open(args.input, 'rb')) if args.input else sys.stdin.buffer
            for line in src:
                collector.feed(line)
            code = 0
        else:
            cmd = args.command
            if cmd and cmd[0] == '--':
                cmd = cmd[1:]
            if not cmd:
                parser.error('run requires a command after --')
            try:
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            except OSError as exc:
                parser.error(str(exc))
            assert proc.stdout is not None
            for line in proc.stdout:
                collector.feed(line)
            proc.stdout.close()
            code = proc.wait()
        output = stack.enter_context(open(args.output, 'wb')) if args.output else sys.stdout.buffer
        collector.finish(output, not args.hide_untagged, args.stats)
        output.flush()
        if args.mode == 'run' and code != 0:
            print(f'MPI command exited with status {code}; inspect the UNTAGGED section for launcher diagnostics', file=sys.stderr)
        return code


if __name__ == '__main__':
    sys.exit(main())
