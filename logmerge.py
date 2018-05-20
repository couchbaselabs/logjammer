#!/usr/bin/env python2.7
# -*- mode: Python;-*-

from dateutil import parser

import glob
import heapq
import os
import re
import sys


def main(argv):
    """logmerge merges log file entries by timestamp to stdout

Example usage:

  ./logmerge.py this.log that.log other.log
  ./logmerge.py path/dirA path/dirB one.log two.log

The entries in each log file must already be sorted by timestamp, as
logmerge operates by performing a heap merge.
"""
    # When expanding a directory, log files have this suffix.
    glob_suffix = "/*.log"

    seeks = None         # Optional dict of path => seek() positions.
    max_entry_len = 100  # Entries that are too long aren't entries.
    invert = False       # Emit non-entries instead of entries.

    # Find log files.

    paths = expand_paths(argv[1:], glob_suffix=glob_suffix)

    # Prepare heap entry for each log file.

    heap_entries = prepare_heap_entries(paths,
                                        seeks=seeks,
                                        max_entry_len=max_entry_len,
                                        invert=invert)

    # Consume heap, emitting each heap entry.

    path_prefix = os.path.commonprefix(paths)

    print_heap_entries(path_prefix, heap_entries,
                       max_entry_len=max_entry_len, invert=invert)


def expand_paths(paths, glob_suffix="/*.log"):
    rv = []

    for path in paths:
        if os.path.isdir(path):
            rv = rv + glob.glob(path + glob_suffix)
        else:
            rv.append(path)

    return rv


def prepare_heap_entries(paths,
                         seeks=None,
                         max_entry_len=100,
                         invert=False):
    heap_entries = []

    for path in paths:
        f = open(path, 'r')
        r = EntryReader(f, path, max_entry_len)

        if seeks:
            seek_to = seeks.get(path)
            if seek_to:
                f.seek(seek_to)
                r.read()  # Discard as it's in the middle of a entry.

        entry = r.read()
        if entry_ok(entry, max_entry_len, invert):
            heap_entries.append((parse_entry_timestamp(entry[0]), entry, r))

    heapq.heapify(heap_entries)

    return heap_entries


def entry_ok(entry, max_entry_len, invert):
    if entry and len(entry) >= 1:
        if len(entry) <= max_entry_len:
            return not invert
        else:
            return invert


def print_heap_entries(path_prefix, heap_entries,
                       max_entry_len=100, invert=False):
    while heap_entries:
        timestamp, entry, r = heapq.heappop(heap_entries)

        print r.path[len(path_prefix):], "".join(entry)

        entry = r.read()
        if entry_ok(entry, max_entry_len, invert):
            heapq.heappush(heap_entries,
                           (parse_entry_timestamp(entry[0]), entry, r))


class EntryReader(object):
    def __init__(self, f, path, max_entry_len):
        self.f = f
        self.path = path
        self.last_line = None
        self.max_entry_len = max_entry_len

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
