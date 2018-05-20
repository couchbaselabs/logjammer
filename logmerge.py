#!/usr/bin/env python
# -*- mode: Python;-*-

from dateutil import parser

import argparse
import glob
import heapq
import os
import re
import sys


def main(argv):
    ap = argparse.ArgumentParser(
       description='merges entries from log files by timestamp to stdout',
       epilog="""An entry in a log file may take more than one line,
where entries are determined via heuristic timestamp parsing.  The log
file entries in each log file must be sorted by timestamp, as logmerge
operates by performing a heap merge.""")

    ap.add_argument('--suffix', type=str, default=".log",
                    help="""when expanding directory paths,
                    find log files that have this glob suffix""")
    ap.add_argument('--max_lines_per_entry', type=int, default=100,
                    help="""max number of lines in an entry before clipping""")
    ap.add_argument('--out', type=str, default="--",
                    help="""write to file instead of to stdout,
                    showing a progress bar on stdout""")
    ap.add_argument('path', nargs='*',
                    help="""log file or directory of log files""")

    args = ap.parse_args(argv[1:])

    process(args.path,
            glob_suffix="/*" + args.suffix,
            max_lines_per_entry=args.max_lines_per_entry,
            out=args.out)


def process(paths,
            glob_suffix="/*.log",
            max_lines_per_entry=100,  # Entries that are too long are clipped.
            seeks=None,
            out='--'):                # dict[path] => initial seek() positions.
    # Find log files.
    paths = expand_paths(paths, glob_suffix)

    # Prepare heap entry for each log file.
    heap_entries = prepare_heap_entries(
        paths, max_lines_per_entry, seeks=seeks)

    # By default, emit to stdout with no progressbar.
    w = sys.stdout
    b = None

    # Otherwise, when emitting to a file, display progress on stdout.
    if out and out != '--':
        w = open(out, 'w')

        total_size = 0
        for path in paths:
            total_size += os.path.getsize(path)

        # See progressbar2 from https://github.com/WoLpH/python-progressbar
        import progressbar

        b = progressbar.ProgressBar().start(max_value=total_size)

    # Print heap entries until all entries are consumed.
    emit_heap_entries(w, os.path.commonprefix(paths), heap_entries, bar=b)

    if w != sys.stdout:
        w.close()


def expand_paths(paths, glob_suffix):
    rv = []

    for path in paths:
        if os.path.isdir(path):
            rv = rv + glob.glob(path + glob_suffix)
        else:
            rv.append(path)

    return rv


def prepare_heap_entries(paths, max_lines_per_entry, seeks=None):
    heap_entries = []

    for path in paths:
        f = open(path, 'r')
        r = EntryReader(f, path, max_lines_per_entry)

        if seeks:
            seek_to = seeks.get(path)
            if seek_to:
                f.seek(seek_to)
                r.read()  # Discard as it's in the middle of a entry.

        entry = r.read()
        if entry:
            heap_entries.append((parse_entry_timestamp(entry[0]), entry, r))

    heapq.heapify(heap_entries)

    return heap_entries


def emit_heap_entries(w, path_prefix, heap_entries, bar=None):
    i = 0  # Total entries seen so far.
    n = 0  # Total bytes of lines seen so far.

    while heap_entries:
        timestamp, entry, r = heapq.heappop(heap_entries)

        w.write(r.path[len(path_prefix):])
        w.write(' ')
        w.write("".join(entry))

        entry = r.read()
        if entry:
            heapq.heappush(heap_entries,
                           (parse_entry_timestamp(entry[0]), entry, r))

            if bar:
                for line in entry:
                    n += len(line)
                if i % 5000 == 0:
                    bar.update(n)
                i += 1


class EntryReader(object):
    def __init__(self, f, path, max_lines_per_entry):
        self.f = f
        self.path = path
        self.last_line = None
        self.max_lines_per_entry = max_lines_per_entry

    def read(self):
        """Read lines from the file until we see the next entry"""

        entry = []
        if self.last_line:
            entry.append(self.last_line)

        while self.f:
            self.last_line = self.f.readline()
            if self.last_line == "":
                self.f.close()
                self.f = None
                return entry

            if parse_entry_timestamp(self.last_line):
                return entry

            if len(entry) < self.max_lines_per_entry:
                entry.append(self.last_line)


# Non-whitespace chars followed by "YYYY-MM-DDThh:mm:ss.sss".
re_entry_timestamp = re.compile(
    r"^\S*(\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\d)")

# For parsing http log timestamps, example...
# 172.23.211.28 - Admin [07/May/2018:16:45:33
re_http_timestamp = re.compile(
    r"^\S+ - \S+ \[(\d\d/\w\w\w/\d\d\d\d:\d\d:\d\d:\d\d) ")


def parse_entry_timestamp(line):
    """Returns the timestamp found in an entry's first line"""

    m = re_entry_timestamp.match(line)
    if m:
        return m.group(1)

    m = re_http_timestamp.match(line)
    if m:
        d = parser.parse(m.group(1), fuzzy=True)
        return d.strftime("%Y-%m-%dT%H%M%S")


if __name__ == '__main__':
    sys.exit(main(sys.argv))
