#!/usr/bin/env python2.7
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
    ap.add_argument('path', nargs='*',
                    help="""log file or directory of log files""")

    args = ap.parse_args(argv[1:])

    process(args.path,
            glob_suffix="/*" + args.suffix,
            max_lines_per_entry=args.max_lines_per_entry)


def process(paths,
            glob_suffix="/*.log",
            max_lines_per_entry=100,  # Entries that are too long are clipped.
            seeks=None):              # dict[path] => seek() positions.
    # Find log files.
    paths = expand_paths(paths, glob_suffix)

    # Prepare heap entry for each log file.
    heap_entries = prepare_heap_entries(
       paths,
       max_lines_per_entry=max_lines_per_entry,
       seeks=seeks)

    # Consume heap, emitting each heap entry.
    print_heap_entries(os.path.commonprefix(paths), heap_entries)


def expand_paths(paths, glob_suffix):
    rv = []

    for path in paths:
        if os.path.isdir(path):
            rv = rv + glob.glob(path + glob_suffix)
        else:
            rv.append(path)

    return rv


def prepare_heap_entries(paths,
                         max_lines_per_entry=100,
                         seeks=None):
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


def print_heap_entries(path_prefix, heap_entries):
    while heap_entries:
        timestamp, entry, r = heapq.heappop(heap_entries)

        print r.path[len(path_prefix):], "".join(entry)

        entry = r.read()
        if entry:
            heapq.heappush(heap_entries,
                           (parse_entry_timestamp(entry[0]), entry, r))


class EntryReader(object):
    def __init__(self, f, path, max_lines_per_entry):
        self.f = f
        self.path = path
        self.last_line = None
        self.max_lines_per_entry = max_lines_per_entry

    def read(self):
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

# Example...
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
