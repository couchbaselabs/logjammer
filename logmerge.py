#!/usr/bin/env python
# -*- mode: Python;-*-

import argparse
import datetime
import glob
import heapq
import os
import re
import sys
import zipfile

from dateutil import parser


main_description = '%(prog)s merges entries from log files by timestamp'

main_epilog = """
An entry in a log file may span more than one line,
where the start of the next entry is determined via heuristics
(mainly, looking for timestamps).  The log file entries in each log
file should already be ordered by timestamp.  See also:
github.com/couchbaselabs/logmerge
"""

# Standard timestamp format used for comparing log entries.
timestamp_format = "%Y-%m-%dT%H:%M:%S"

# Non-whitespace chars followed by "YYYY-MM-DDThh:mm:ss.sss".
re_entry_timestamp = re.compile(
    r"^\S*(\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\d)")

# For parsing http log timestamps, example...
# 172.23.211.28 - Admin [07/May/2018:16:45:33
re_http_timestamp = re.compile(
    r"^\S+ - \S+ \[(\d\d/\w\w\w/\d\d\d\d:\d\d:\d\d:\d\d) ")


def main(argv, argument_parser=None, visitor=None):
    if not argument_parser:
        argument_parser = argparse.ArgumentParser(
            description=main_description,
            epilog=main_epilog)

    argument_parser = add_arguments(argument_parser)

    args = argument_parser.parse_args(argv[1:])

    main_with_args(args, visitor=visitor)


def main_with_args(args, visitor=None, path_prefix=None, bar=None):
    start, end = parse_near(args.near, args.start, args.end)

    process(args.path,
            path_prefix=path_prefix,
            fields=args.fields,
            match=args.match, match_not=args.match_not,
            max_entries=args.max_entries,
            max_lines_per_entry=args.max_lines_per_entry,
            out=args.out,
            single_line=args.single_line,
            start=start, end=end,
            scan_start=args.scan_start,
            scan_length=args.scan_length,
            suffix=args.suffix,
            timestamp_prefix=args.timestamp_prefix,
            visitor=visitor,
            wrap=args.wrap,
            wrap_indent=args.wrap_indent,
            bar=bar)

    if args.out != '--' and args.out != os.devnull:
        print >>sys.stderr, "\ndone"


def add_arguments(ap):
    ap.add_argument('--out', type=str, default="--",
                    help="""write to an OUT file instead
                    of by default to stdout; when an OUT file is specified,
                    a progress bar is shown instead on stdout""")

    add_path_arguments(ap)
    add_match_arguments(ap)
    add_time_range_arguments(ap)
    add_scan_range_arguments(ap)
    add_advanced_arguments(ap)

    return ap


def add_path_arguments(ap):
    ap.add_argument('--suffix', type=str, default="log",
                    help="""when expanding directory paths,
                    find log files that match this glob suffix
                    (default: %(default)s)""")

    ap.add_argument('path', nargs='*',
                    help="""a log file, directory of log files, or zip file""")


def add_match_arguments(ap):
    g = ap.add_argument_group('regexp arguments',
                              'filtering of log entries by regexp')
    g.add_argument('--match', type=str,
                   help="""log entries that match this optional
                   regexp will be emitted""")
    g.add_argument('--match-not', type=str,
                   help="""log entries that do not match this optional
                   regexp will be emitted""")


def add_time_range_arguments(ap):
    g = ap.add_argument_group('time range arguments',
                              'filtering of log entries by time range')
    g.add_argument('--start', type=str,
                   help="""emit only entries that come at or after this
                   timestamp, like YYYY-MM-DD or YYYY-MM-DDThh:mm:ss""")
    g.add_argument('--end', type=str,
                   help="""emit only entries that come at or before this
                   timestamp, like YYYY-MM-DD or YYYY-MM-DDThh:mm:ss""")
    g.add_argument('--near', type=str,
                   help="""emit log entries that are near the given
                   timestamp, by providing defaults to the start/end
                   arguments, like YYYY-MM-DDThh:mm:ss[+/-MINUTES],
                   where the optional MINUTES defaults to 1 minute;
                   example: 2018-01-31T17:15:00+/-10""")


def add_scan_range_arguments(ap):
    g = ap.add_argument_group('scan range arguments',
                              'scan only in this byte range')
    g.add_argument('--scan-start', type=int,
                   help="""emit only entries that come at or after this
                   seek position""")
    g.add_argument('--scan-length', type=int,
                   help="""scan these many bytes max""")


def add_advanced_arguments(ap):
    g = ap.add_argument_group('advanced arguments')

    g.add_argument('--fields', type=str,
                   help="""when specified, heuristically parse key=value
                   data from the log entries and emit those in CSV
                   format instead of log entry lines; the FIELDS is
                   a comma-separated list of key names""")
    g.add_argument('--max-entries', type=int,
                   help="""stop after this many entries are emitted
                   (default: %(default)s)""")
    g.add_argument('--max-lines-per-entry', type=int, default=100,
                   help="""max number of lines in an entry before clipping,
                   where 0 means no limit (default: %(default)s)""")
    g.add_argument('--single-line', type=bool, default=False,
                   help="""collapse multi-line entries into a single line
                   (default: %(default)s)""")
    g.add_argument('--timestamp-prefix', type=bool,
                   help="""a normalized timestamp will be emitted first
                   for each entry, to allow for easier post-processing,
                   often used with --single-line
                   (default: %(default)s)""")
    g.add_argument('--wrap', type=int,
                   help="""wrap long lines to this many chars
                   (default: %(default)s)""")
    g.add_argument('--wrap-indent', type=int, default=2,
                   help="""when wrapping long lines, secondary lines
                   will have this # of indentation space chars
                   (default: %(default)s)""")


# Optional near param might look like "2018-12-25T03:00:00+/-5",
# and works by providing defaults for the start/end params.
def parse_near(near, start, end):
    if near:
        near = near.split("+/-")
        base = parser.parse(near[0])

        minutes = datetime.timedelta(minutes=1)
        if len(near) == 2:
            minutes = datetime.timedelta(minutes=int(near[1]))

        if not start:
            start = (base - minutes).strftime(timestamp_format)

        if not end:
            end = (base + minutes).strftime(timestamp_format)

    return start, end


def process(paths,
            path_prefix=None,         # Optional common prefix for all paths.
            fields=None,              # Optional fields to parse & emit as CSV.
            match=None,
            match_not=None,
            max_entries=None,
            max_lines_per_entry=100,  # Entries that are too long are clipped.
            out='--',                 # Output file path, or '--' for stdout.
            single_line=False,        # dict[path] => initial seek() positions.
            start=None,               # Start timestamp for binary search.
            end=None,                 # End timestamp for filtering.
            scan_start=None,          # Optional scan seek start byte.
            scan_length=None,         # Optional scan max number of bytes.
            suffix="log",             # Suffix used with directory glob'ing.
            timestamp_prefix=False,   # Emit normalized timestamp prefix.
            visitor=None,             # Optional entry visitor callback.
            wrap=None,                # Wrap long lines to this many chars.
            wrap_indent=None,         # Indentation of wrapped secondary lines.
            w=None,                   # Optional output stream.
            bar=None):                # Optional progress bar.
    # Find log files.
    paths, total_size, path_sizes = expand_paths(paths, suffix)

    if not path_prefix:
        if len(paths) > 1:
            path_prefix = os.path.commonprefix(paths)
        else:
            path_prefix = ""

    # Prepare heap entry for each log file.
    heap_entries = prepare_heap_entries(paths, path_prefix,
                                        scan_start, scan_length,
                                        max_lines_per_entry, start, end)

    # By default, emit to stdout with no progress display.
    if not w:
        w, bar = prepare_out(out, bar)

    if bar:
        bar.start(max_value=total_size)

    # If fields are specified, provide a visitor that emits to a CSV writer.
    if fields:
        visitor, w = prepare_fields_filter(fields.split(","), visitor, w)

    # Emit heap entries until all entries are consumed.
    n = emit_heap_entries(w, path_prefix,
                          heap_entries, max_entries,
                          end=end, match=match, match_not=match_not,
                          single_line=single_line,
                          timestamp_prefix=timestamp_prefix, visitor=visitor,
                          wrap=wrap, wrap_indent=wrap_indent, bar=bar)

    if w and w != sys.stdout:
        w.close()

    if bar:
        bar.update(n)


def expand_paths(paths, suffix):
    globbed = []

    for path in paths:
        if os.path.isdir(path):
            for s in suffix.split(","):
                globbed = globbed + glob.glob(path + "/*." + s)
        else:
            globbed.append(path)

    rv = []
    total_size = 0
    path_sizes = {}

    for path in globbed:
        if path.endswith(".zip"):
            zf = zipfile.ZipFile(path, 'r')
            for info in zf.infolist():
                zpath = path + "/" + info.filename
                rv.append(zpath)
                total_size += info.file_size
                path_sizes[zpath] = info.file_size
        else:
            rv.append(path)
            size = os.path.getsize(path)
            total_size += size
            path_sizes[path] = size

    rv.sort()

    return rv, total_size, path_sizes


def prepare_heap_entries(paths, path_prefix,
                         scan_start, scan_length,
                         max_lines_per_entry, start, end):

    heap_entries = []

    zfs = {}  # Key is path, value is zipfile.ZipFile.

    for path in paths:
        zip_suffix = path.find(".zip/")
        if zip_suffix > 0:
            zp = path[0:zip_suffix+4]

            zf = zfs.get(zp)
            if not zf:
                zf = zipfile.ZipFile(zp, 'r')
                zfs[zp] = zf

            f = zf.open(path[zip_suffix+5:], 'r')
        else:
            f = open(path, 'r')

        scan_start = scan_start or 0
        if scan_start:
            f.seek(scan_start)

        r = EntryReader(f, path, path[len(path_prefix):],
                        max_lines_per_entry, scan_length)

        if start:  # Optional start timestamp.
            scan_length = seek_to_timestamp(f, path, path_prefix,
                                            scan_start, scan_length, start)

            r = EntryReader(f, path, path[len(path_prefix):],
                            max_lines_per_entry, scan_length)

            r.read()  # Discard this read as it's likely mid-entry.

        entry, entry_size = r.read()
        if entry:
            timestamp = parse_entry_timestamp(entry[0])
            if (not end) or timestamp <= end:
                heap_entries.append([timestamp, entry, entry_size, r])

    heapq.heapify(heap_entries)

    return heap_entries


def seek_to_timestamp(f, path, path_prefix,
                      scan_start, scan_length, start_timestamp):
    """Binary search the log file entries for the start_timestamp,
       leaving the file at the right seek position."""

    i = scan_start

    j = os.path.getsize(path)
    if scan_length and i + scan_length < j:
        j = i + scan_length

    j_start = j

    while i < j:
        mid = int((i + j) / 2)

        f.seek(mid)

        r2 = EntryReader(f, path, path[len(path_prefix):],
                         1, scan_length, close_when_done=False)
        r2.read()  # Discard this read as it's likely mid-entry.

        entry, entry_size = r2.read()
        if entry:
            if start_timestamp > parse_entry_timestamp(entry[0]):
                i = mid + 1
            else:
                j = mid
        else:
            i = j

    f.seek(i)

    return j_start - i


def prepare_out(out, bar):
    w = sys.stdout

    if out and out != '--':
        if out == os.devnull:
            w = None
        else:
            w = open(out, 'w')

        if not bar:
            # When emitting to a file, display progress on stdout.
            # See progressbar2 https://github.com/WoLpH/python-progressbar
            import progressbar
            bar = progressbar.ProgressBar()

    return w, bar


def prepare_fields_filter(fields, visitor, w):
    """Prepare a visitor that filters key=value data from each entry,
       emitting CSV to the given writer."""

    field_names = ['timestamp', 'dir', 'file'] + fields

    import csv
    csv_writer = csv.writer(w)
    csv_writer.writerow(field_names)

    row = [None] * len(field_names)

    field_patterns = []
    for field in fields:
        field_patterns.append(
            re.compile(r"\"?" + field + r"\"?[=:,]([\-\d\.]+)"))

    def fields_filter(path_short, timestamp, entry, entry_size):
        if visitor:  # Wrap the optional, input visitor.
            visitor(path_short, timestamp, entry, entry_size)

        # Search for field_patterns in any line in the entry.
        matched = False

        for idx, field in enumerate(fields):
            row[idx+3] = None

            for line in entry:
                m = field_patterns[idx].search(line)
                if m:
                    row[idx+3] = m.group(1)
                    matched = True
                    break

        if matched:
            row[0] = timestamp
            row[1] = os.path.dirname(path_short)
            row[2] = os.path.basename(path_short)
            if row[2].endswith(".log"):
                row[2] = row[2][:-4]

            csv_writer.writerow(row)

    return fields_filter, NoopWriter(w)


def emit_heap_entries(w, path_prefix, heap_entries, max_entries,
                      end=None, match=None, match_not=None,
                      single_line=False,
                      timestamp_prefix=False,
                      visitor=None,
                      wrap=None, wrap_indent=None,
                      bar=None):
    re_match = match and re.compile(match)

    re_match_not = match_not and re.compile(match_not)

    text_wrapper = prepare_text_wrapper(wrap, wrap_indent)

    class Emitter(object):
        def __init__(self):
            self.i = 0  # Total entries seen so far.
            self.n = 0  # Total bytes of lines seen so far.
            self.e = 0  # Total entries emitted.

        def emit_heap_entry(self, timestamp, entry, entry_size, r):
            self.i += 1
            self.n += entry_size

            if entry_allowed(entry, re_match, re_match_not):
                if visitor:
                    visitor(r.path_short, timestamp, entry, entry_size)

                if w:
                    entry_emit(w, r.path_short, timestamp, entry,
                               single_line, timestamp_prefix, text_wrapper)

                self.e += 1
                if max_entries and self.e > max_entries:
                    return False

            if bar and self.i % 2000 == 0:
                bar.update(self.n)

            return True

    emitter = Emitter()

    def process_remaining_entries(timestamp, entry, entry_size, r):
        # Use a direct loop instead of a heap when only a single r left.
        while emitter.emit_heap_entry(timestamp, entry, entry_size, r):
            entry, entry_size = r.read()
            if not entry:
                return

            timestamp = parse_entry_timestamp(entry[0])
            if end and timestamp > end:
                return

    while heap_entries:
        heap_entry = heapq.heappop(heap_entries)

        timestamp, entry, entry_size, r = heap_entry

        ok = emitter.emit_heap_entry(timestamp, entry, entry_size, r)
        if not ok:
            break

        entry, entry_size = r.read()
        if entry:
            timestamp = parse_entry_timestamp(entry[0])
            if (not end) or timestamp <= end:
                if heap_entries:
                    heap_entry[0] = timestamp
                    heap_entry[1] = entry
                    heap_entry[2] = entry_size

                    heapq.heappush(heap_entries, heap_entry)
                else:
                    # Don't need a heap anymore and can just use a loop.
                    process_remaining_entries(timestamp, entry, entry_size, r)

    return emitter.n

def prepare_text_wrapper(wrap, wrap_indent):
    if not wrap:
        return None

    import textwrap

    subsequent_indent = ''
    if wrap_indent:
        subsequent_indent = ' ' * wrap_indent

    text_wrapper = textwrap.TextWrapper(
        width=wrap, break_long_words=False,
        subsequent_indent=subsequent_indent)

    # From https://github.com/python/cpython/blob/2.7/Lib/textwrap.py
    text_wrapper.wordsep_re = \
        re.compile(
            r'(\s+|'                                  # any whitespace
            r',|'                                     # commas
            r'[^\s\w]*\w+[^0-9\W]-(?=\w+[^0-9\W])|'   # hyphenated words
            r'(?<=[\w\!\"\'\&\.\,\?])-{2,}(?=\w))')   # em-dash

    return text_wrapper


def entry_allowed(entry, re_match, re_match_not):
    allowed = True

    if re_match:
        allowed = False
        for line in entry:
            if re_match.search(line):
                allowed = True
                break

    if re_match_not:  # Inspired by 'grep -v'.
        for line in entry:
            if re_match_not.search(line):
                allowed = False
                break

    return allowed


def entry_emit(w, path_short, timestamp, entry,
               single_line, timestamp_prefix, text_wrapper):
    if timestamp_prefix:
        w.write(timestamp or "0000-00-00T00:00:00")
        w.write(' ')

    w.write(path_short)
    w.write(' ')

    for line in entry:
        if single_line:
            w.write(line[:-1])  # Clip trailing newline.
        elif text_wrapper:
            w.write(text_wrapper.fill(line))
            w.write("\n")
        else:
            w.write(line)

    if single_line:
        w.write("\n")


class EntryReader(object):
    def __init__(self, f, path, path_short,
                 max_lines_per_entry, max_bytes, close_when_done=True):
        self.f = f
        self.path = path
        self.path_short = path_short
        self.max_lines_per_entry = max_lines_per_entry
        self.max_bytes = max_bytes
        self.close_when_done = close_when_done
        self.last_line = None
        self.num_bytes = 0

    def read(self):
        """Read lines from the file until we see the next entry"""

        entry = []
        entry_size = 0

        if self.last_line:
            entry.append(self.last_line)

            entry_size += len(self.last_line)
            self.last_line = None

        while self.f:
            if self.max_bytes and self.num_bytes > self.max_bytes:
                return entry, entry_size

            self.last_line = self.f.readline()
            if not self.last_line:
                if self.close_when_done:
                    self.f.close()
                self.f = None
                return entry, entry_size

            self.num_bytes += len(self.last_line)

            if parse_entry_timestamp(self.last_line):
                return entry, entry_size

            if not self.max_lines_per_entry:
                entry.append(self.last_line)
            elif len(entry) < self.max_lines_per_entry:
                entry.append(self.last_line)
            elif len(entry) == self.max_lines_per_entry:
                entry.append(" ...CLIPPED...\n")

            entry_size += len(self.last_line)
            self.last_line = None

        return None, 0


def parse_entry_timestamp(line):
    """Returns the timestamp found in an entry's first line"""

    m = re_entry_timestamp.match(line)
    if m:
        return m.group(1)

    m = re_http_timestamp.match(line)
    if m:
        d = parser.parse(m.group(1), fuzzy=True)
        return d.strftime(timestamp_format)


class NoopWriter(object):
    def __init__(self, w): self.w = w

    def write(self, *_): pass

    def close(self):
        if self.w:
            return self.w.close()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
