#!/usr/bin/env python2.7
# -*- mode: Python;-*-

import glob
import heapq
import os
import re
import sys


def main(argv):
    """logmerge merges log files by timestamp to stdout

Example usage:

  ./logmerge.py cbcollect-*
  ./logmerge.py cbcollect-*/*.log
  ./logmerge.py this.log that.log path/to/directoryOfLogFiles
"""

    paths = expand_paths(argv[1:])

    seek_default = 1 # Skip first line.

    heap_entries = prepare_heap_entries(paths, seek_default=seek_default)

    print_heap_entries(heap_entries)


def expand_paths(paths, glob_suffix="/*.log"):
    rv = []

    for path in paths:
        if os.path.isdir(path):
            rv = rv + glob.glob(path + glob_suffix)
        else:
            rv.append(path)

    return rv


def prepare_heap_entries(paths, seeks=None, seek_default=0, max_entry_len=100):
    heap_entries = []

    for path in paths:
        f = open(path, 'r')
        r = EntryReader(f, path)

        seek_to = seek_default
        if seeks:
            seek_to = seeks.get(path)

        if seek_to:
            f.seek(seek_to)
            r.read() # Discard as it's in the middle of a entry.

        entry = r.read()
        if entry and len(entry) >= 1 and len(entry) < max_entry_len:
            heap_entries.append((parse_first_timestamp(entry[0]), entry, r))

    heapq.heapify(heap_entries)

    return heap_entries


def print_heap_entries(heap_entries, max_entry_len=100):
    while heap_entries:
        timestamp, entry, r = heapq.heappop(heap_entries)

        print "".join(entry)

        entry = r.read()
        if entry and len(entry) >= 1 and len(entry) < max_entry_len:
            heapq.heappush(heap_entries,
                           (parse_first_timestamp(entry[0]), entry, r))


class EntryReader(object):
    def __init__(self, f, path):
        self.f = f
        self.path = path
        self.last_line = None

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

            if parse_first_timestamp(self.last_line):
                return entry

            entry.append(self.last_line)


# Non-whitespace chars followed by "YYYY-MM-DDThh:mm:ss.sss".
re_first_timestamp = re.compile(r"^\S*(\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\d)")

def parse_first_timestamp(line):
    """Returns the first timestamp of a log entry"""
    m = re_first_timestamp.match(line)
    if m:
        return m.group(1)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
