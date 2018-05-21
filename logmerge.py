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
       description='%(prog)s merges entries from log files by timestamp',
       epilog="""An entry in a log file may span more than one line,
where the start of the next entry is determined via heuristics
(mainly, looking for timestamps).  The log file entries in each log
file are expected to be ordered by timestamp, as %(prog)s operates by
performing a heap merge.""")

    ap.add_argument('--max-lines-per-entry', type=int, default=100,
                    help="""max number of lines in an entry before clipping,
                    where 0 means no limit (default: %(default)s)""")
    ap.add_argument('--out', type=str, default="--",
                    help="""write to an OUT file instead
                    of by default to stdout, showing a progress bar
                    instead on stdout""")
    ap.add_argument('--single-line', type=bool, default=False,
                    help="""collapse multi-line entries into a single line
                    (default: %(default)s)""")
    ap.add_argument('--suffix', type=str, default=".log",
                    help="""when expanding directory paths,
                    find log files that match this glob suffix
                    (default: %(default)s)""")
    ap.add_argument('--start', type=str,
                    help="""emit only entries that come at or after this
                    timestamp, like YYYY-MM-DD or YYYY-MM-DDThh-mm-ss""")
    ap.add_argument('path', nargs='*',
                    help="""a log file or directory of log files""")

    args = ap.parse_args(argv[1:])

    process(args.path,
            max_lines_per_entry=args.max_lines_per_entry,
            out=args.out,
            single_line=args.single_line,
            start=args.start,
            suffix=args.suffix)


def process(paths,
            max_lines_per_entry=100,  # Entries that are too long are clipped.
            out='--',                 # Output file path, or '--' for stdout.
            single_line=False,        # dict[path] => initial seek() positions.
            start=None,               # Start timestamp for binary search.
            suffix=".log"):           # Suffix used with directory glob'ing.
    # Find log files.
    paths = expand_paths(paths, "/*" + suffix)

    total_size = 0
    for path in paths:
        total_size += os.path.getsize(path)

    # Prepare heap entry for each log file.
    heap_entries = prepare_heap_entries(paths, max_lines_per_entry, start)

    # By default, emit to stdout with no progress display.
    w = sys.stdout
    b = None

    # Otherwise, when emitting to a file, display progress on stdout.
    if out and out != '--':
        w = open(out, 'w')

        # See progressbar2 from https://github.com/WoLpH/python-progressbar
        import progressbar

        b = progressbar.ProgressBar().start(max_value=total_size)

    # Print heap entries until all entries are consumed.
    emit_heap_entries(w, os.path.commonprefix(paths), heap_entries,
                      single_line=single_line, bar=b)

    if w != sys.stdout:
        w.close()

    if b:
        b.update(total_size)
        print >>sys.stderr, "\ndone"


def expand_paths(paths, glob_suffix):
    rv = []

    for path in paths:
        if os.path.isdir(path):
            rv = rv + glob.glob(path + glob_suffix)
        else:
            rv.append(path)

    return rv


def prepare_heap_entries(paths, max_lines_per_entry, start):
    heap_entries = []

    for path in paths:
        f = open(path, 'r')
        r = EntryReader(f, path, max_lines_per_entry)

        if start:  # Optional start timestamp to find with binary search.
            i = 0
            j = os.path.getsize(path)

            while i < j:
                mid = int((i + j) / 2)

                f.seek(mid)

                r2 = EntryReader(f, path, 1, close_when_done=False)
                r2.read()  # Discard this read as it's likely mid-entry.

                entry, entry_size = r2.read()
                if entry:
                    if start > parse_entry_timestamp(entry[0]):
                        i = mid + 1
                    else:
                        j = mid
                else:
                    i = j

            f.seek(i)

            r = EntryReader(f, path, max_lines_per_entry)
            r.read()  # Discard this read as it's likely mid-entry.

        entry, entry_size = r.read()
        if entry:
            heap_entries.append(
                (parse_entry_timestamp(entry[0]), entry, entry_size, r))

    heapq.heapify(heap_entries)

    return heap_entries


def emit_heap_entries(w, path_prefix, heap_entries,
                      single_line=False,
                      bar=None):
    i = 0  # Total entries seen so far.
    n = 0  # Total bytes of lines seen so far.

    while heap_entries:
        timestamp, entry, entry_size, r = heapq.heappop(heap_entries)

        if bar:
            n += entry_size
            if i % 2000 == 0:
                bar.update(n)
            i += 1

        w.write(r.path[len(path_prefix):])
        w.write(' ')

        for line in entry:
            if single_line:
                w.write(line[:-1])  # Clip trailing newline.
            else:
                w.write(line)

        if single_line:
            w.write("\n")

        entry, entry_size = r.read()
        if entry:
            heapq.heappush(
               heap_entries,
               (parse_entry_timestamp(entry[0]), entry, entry_size, r))


class EntryReader(object):
    def __init__(self, f, path, max_lines_per_entry, close_when_done=True):
        self.f = f
        self.path = path
        self.max_lines_per_entry = max_lines_per_entry
        self.close_when_done = close_when_done
        self.last_line = None

    def read(self):
        """Read lines from the file until we see the next entry"""

        entry = []
        entry_size = 0

        if self.last_line:
            entry.append(self.last_line)
            entry_size += len(self.last_line)

        while self.f:
            self.last_line = self.f.readline()
            if self.last_line == "":
                if self.close_when_done:
                    self.f.close()
                self.f = None
                return entry, entry_size

            if parse_entry_timestamp(self.last_line):
                return entry, entry_size

            if not self.max_lines_per_entry:
                entry.append(self.last_line)
            elif len(entry) < self.max_lines_per_entry:
                entry.append(self.last_line)
            elif len(entry) == self.max_lines_per_entry:
                entry.append(" ...CLIPPED...\n")

            entry_size += len(self.last_line)

        return None, 0


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
        return d.strftime("%Y-%m-%dT%H:%M:%S")


if __name__ == '__main__':
    sys.exit(main(sys.argv))
