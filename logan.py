#!/usr/bin/env python
# -*- mode: Python;-*-

import argparse
import collections
import json
import os
import re
import subprocess
import sys

from dateutil import parser

from PIL import Image, ImageDraw

import SimpleHTTPServer
import SocketServer
import urlparse

import logmerge


timestamp_gutter_width = 1  # In pixels.

max_image_height = 0  # 0 means unlimited height.


def main(argv):
    set_argv_default(argv, "out", "/dev/null")

    args = logmerge.add_arguments(
        new_argument_parser()).parse_args(argv[1:])

    if args.http_only is not None:
        print "\n============================================"
        http_server(argv, args.http_only)
        return

    # Scan the logs to build the pattern info's.
    paths, file_pos_term_counts, file_patterns, timestamp_info = \
        scan_patterns(argv)

    # Process the pattern info's to find similar pattern info's.
    mark_similar_pattern_infos(file_patterns)

    if False:
        print "\n============================================"

        print "len(file_pos_term_counts)", len(file_pos_term_counts)

        for file_name, pos_term_counts in file_pos_term_counts.iteritems():
            print "  ", file_name
            print "    len(pos_term_counts)", \
                len(pos_term_counts)
            print "    sum(pos_term_counts.values)", \
                sum(pos_term_counts.values())
            print "    most common", \
                pos_term_counts.most_common(10)
            print "    least common", \
                pos_term_counts.most_common()[:-10:-1]
            print "    ------------------"

    print "\n============================================"

    print "len(file_patterns)", len(file_patterns)

    num_entries = 0

    num_pattern_infos = 0
    num_pattern_infos_base = 0
    num_pattern_infos_base_none = 0

    # The unique "file_name: pattern_tuple"'s when shared
    # pattern_tuple_base's are also considered.  The value is the
    # total number of entries seen.
    pattern_uniques = {}

    first_timestamp = None

    for file_name, patterns in file_patterns.iteritems():
        num_pattern_infos += len(patterns)

        print "  ", file_name
        print "    len(patterns)", len(patterns)

        pattern_tuples = patterns.keys()
        pattern_tuples.sort()

        num_entries_file = 0

        for i, pattern_tuple in enumerate(pattern_tuples):
            pattern_info = patterns[pattern_tuple]

            num_entries += pattern_info.total
            num_entries_file += pattern_info.total

            if pattern_info.pattern_tuple_base:
                pattern_tuple = pattern_info.pattern_tuple_base

                num_pattern_infos_base += 1
            else:
                num_pattern_infos_base_none += 1

            k = file_name + ": " + str(pattern_tuple)

            pattern_uniques[k] = \
                pattern_uniques.get(k, 0) + \
                pattern_info.total

            if (not first_timestamp) or \
               (first_timestamp > pattern_info.first_timestamp):
                first_timestamp = pattern_info.first_timestamp

        pattern_uniques[file_name] = num_entries_file

    print "\n============================================"

    print "num_entries", num_entries

    print "num_pattern_infos", num_pattern_infos
    print "num_pattern_infos_base", num_pattern_infos_base
    print "num_pattern_infos_base_none", num_pattern_infos_base_none

    print "len(pattern_uniques)", len(pattern_uniques)

    print "first_timestamp", first_timestamp

    print "\n============================================"

    pattern_ranks = {}

    pattern_unique_keys = pattern_uniques.keys()
    pattern_unique_keys.sort()

    print "pattern_uniques..."

    for i, k in enumerate(pattern_unique_keys):
        pattern_ranks[k] = i

        print "  ", pattern_uniques[k], "-", k

    print "\n============================================"

    print "writing out.json"

    with open("out.json", 'w') as f:
        o = {
            "argv":                        argv,
            "paths":                       paths,
            "num_entries":                 num_entries,
            "num_pattern_infos":           num_pattern_infos,
            "num_pattern_infos_base":      num_pattern_infos_base,
            "num_pattern_infos_base_none": num_pattern_infos_base_none,
            "pattern_ranks":               pattern_ranks,
            "first_timestamp":             first_timestamp,
            "num_unique_timestamps":       timestamp_info.num_unique,
            "timestamp_gutter_width":      timestamp_gutter_width
        }

        f.write(json.dumps(o))

    print "\n============================================"

    scan_to_plot(argv, file_patterns, pattern_ranks,
                 timestamp_info.num_unique, first_timestamp)

    if args.http is not None:
        print "\n============================================"
        http_server(argv, args.http)
        return


# Modify argv with a default for the --name=val argument.
def set_argv_default(argv, name, val):
    prefix = "--" + name + "="

    for arg in argv:
        if arg.startswith(prefix):
            return

    argv.insert(1, prefix + val)


def new_argument_parser():
    ap = argparse.ArgumentParser(
        description="""%(prog)s provides log analysis
                       (extends logmerge.py feature set)""")

    ap.add_argument('--http', type=str,
                    help="""at the end of processing,
                    start a web-server on the given HTTP port number
                    """)

    ap.add_argument('--http-only', type=str,
                    help="""don't do any processing, but only
                    start a web-server on the given HTTP port number
                    """)

    return ap


# Need 32 hex chars for a uid pattern.
pattern_uid = "[a-f0-9]" * 32

# An example rev to initialize pattern_rev.
ex_rev = \
    "g2wAAAABaAJtAAAAIDJkZTgzNjhjZTNlMjQ0Y2Q" + \
    "3ZDE0MWE2OGI0ODE3ZDdjaAJhAW4FANj8ddQOag"

pattern_rev = "[a-zA-Z90-9]" * len(ex_rev)

# Some number-like patterns such as dotted or dashed or slashed or
# colon'ed numbers.  Patterns like YYYY-MM-DD, HH:MM:SS and IP
# addresses are also matched.
pattern_num_ish = [
    ("hex", r"0x[a-f0-9][a-f0-9]+"),
    ("hex", r"0x[A-F0-9][A-F0-9]+"),
    ("uid", pattern_uid),
    ("rev", pattern_rev),
    ("ymd", r"\d\d\d\d-\d\d-\d\d"),
    ("dmy", r"\d\d/[JFMASOND][a-z][a-z]/\d\d\d\d"),
    ("hms", r"T?\d\d:\d\d:\d\d -\d\d\d\d"),
    ("hms", r"T?\d\d:\d\d:\d\d\.\d\d\d\d\d\dZ"),
    ("hms", r"T?\d\d:\d\d:\d\d\.\d\d\d-\d\d:\d\d"),
    ("hms", r"T?\d\d:\d\d:\d\d\.\d\d\d-\d\d"),
    ("hms", r"T?\d\d:\d\d:\d\d\.\d\d\d"),
    ("hms", r"T?\d\d:\d\d:\d\d"),
    ("ip4", r"\d+\.\d+\.\d+\.\d+"),
    ("idn", r"[a-zA-Z][a-zA-Z\-_]+\d+"),  # Numbered identifier, like "vb8".
    ("neg", r"-\d[\d\.]*"),               # Negative dotted number.
    ("pos", r"\d[\d\.]*")]                # Positive dotted number.

pattern_num_ish_joined = "(" + \
                         "|".join(["(" + p[1] + ")"
                                   for p in pattern_num_ish]) + \
                         ")"

re_num_ish = re.compile(pattern_num_ish_joined)

re_section_split = re.compile(r"[^a-zA-z0-9_\-/]+")

re_erro = re.compile(r"[^A-Z]ERRO")

# Used to group entries by timestamp into bins or buckets.
timestamp_prefix = "YYYY-MM-DDTHH:MM:SS"
timestamp_prefix_len = len(timestamp_prefix)


# Scan the log files to build up pattern info's.
def scan_patterns(argv):
    argument_parser = logmerge.add_arguments(new_argument_parser())

    args = argument_parser.parse_args(argv[1:])

    # Custom visitor.
    visitor, file_pos_term_counts, file_patterns, timestamp_info = \
        scan_patterns_visitor()

    # Main driver of visitor callbacks is reused from logmerge.
    logmerge.main_with_args(args, visitor=visitor)

    return args.path, file_pos_term_counts, file_patterns, timestamp_info


def scan_patterns_visitor():
    # Keyed by file name, value is collections.Counter.
    file_pos_term_counts = {}

    # Keyed by file name, value is dict of pattern => PatternInfo.
    file_patterns = {}

    timestamp_info = TimestampInfo()

    def v(path, timestamp, entry, entry_size):
        if (not timestamp) or (not entry):
            return

        file_name = os.path.basename(path)

        pos_term_counts = file_pos_term_counts.get(file_name)
        if pos_term_counts is None:
            pos_term_counts = collections.Counter()
            file_pos_term_counts[file_name] = pos_term_counts

        pattern = entry_to_pattern(entry,
                                   pos_term_counts=pos_term_counts)
        if not pattern:
            return

        # Register into patterns dict if it's a brand new pattern.
        patterns = file_patterns.get(file_name)
        if patterns is None:
            patterns = {}
            file_patterns[file_name] = patterns

        pattern_tuple = tuple(pattern)

        pattern_info = patterns.get(pattern_tuple)
        if not pattern_info:
            pattern_info = PatternInfo(pattern_tuple, timestamp, entry)
            patterns[pattern_tuple] = pattern_info

        # Increment the total count of instances of this pattern.
        pattern_info.total += 1

        timestamp_bin = timestamp[:timestamp_prefix_len]
        if timestamp_info.last != timestamp_bin:
            timestamp_info.last = timestamp_bin
            timestamp_info.num_unique += 1

    return v, file_pos_term_counts, file_patterns, timestamp_info


class TimestampInfo:
    def __init__(self):
        self.last = None
        self.num_unique = 0


def entry_to_pattern(entry, pos_term_counts=None):
    # Only look at the first line of the entry.
    entry_first_line = entry[0].strip()

    # Split the first line into num'ish and non-num'ish sections.
    sections = re.split(re_num_ish, entry_first_line)

    # Build up the current pattern from the sections.
    pattern = []

    i = 0
    while i < len(sections):
        # First, handle a non-num'ish section.
        section = sections[i]

        i += 1

        # Split the non-num'ish section into terms.
        for term in re.split(re_section_split, section):
            if not term:
                continue

            # A "positioned term" encodes a term position with a term.
            pos_term = str(len(pattern)) + ">" + term

            if pos_term_counts:
                pos_term_counts.update([pos_term])

            pattern.append(pos_term)

        # Next, handle a num-ish section, where re.split()
        # produces as many items as there were capture groups.
        if i < len(sections):
            num_ish_kind = None
            j = 0
            while j < len(pattern_num_ish):
                if sections[i + 1 + j]:
                    num_ish_kind = j  # The capture group that fired.
                    break
                j += 1

            pattern.append("#" + pattern_num_ish[num_ish_kind][0])

            i += 1 + len(pattern_num_ish)

    return pattern


class PatternInfo(object):
    def __init__(self, pattern_tuple, first_timestamp, first_entry):
        self.pattern_tuple = pattern_tuple
        self.pattern_tuple_base = None
        self.first_timestamp = first_timestamp
        self.first_entry = first_entry
        self.total = 0


# Find and mark similar pattern info's.
def mark_similar_pattern_infos(file_patterns, scan_distance=10):
    for file_name, patterns in file_patterns.iteritems():
        pattern_tuples = patterns.keys()
        pattern_tuples.sort()  # Sort so similar patterns are nearby.

        for i, pattern_tuple in enumerate(pattern_tuples):
            curr_pattern_info = patterns[pattern_tuple]

            scan_idx = i - 1
            scan_until = i - scan_distance

            while scan_idx >= 0 and scan_idx > scan_until:
                prev_pattern_tuple = pattern_tuples[scan_idx]
                prev_pattern_info = patterns[prev_pattern_tuple]

                if mark_similar_pattern_info_pair(curr_pattern_info,
                                                  prev_pattern_info):
                    break

                scan_idx -= 1


# Examine a new and old pattern info, and if they're similar (only
# differing by a single part), mark them and return True.
def mark_similar_pattern_info_pair(new, old):
    new_tuple = new.pattern_tuple
    old_tuple = old.pattern_tuple

    if len(new_tuple) != len(old_tuple):
        return False

    if old.pattern_tuple_base:
        old_tuple = old.pattern_tuple_base

    for i in range(len(new_tuple)):
        if new_tuple[i] == old_tuple[i]:
            continue

        # See if remaining tuple parts are different.
        if new_tuple[i+1:] != old_tuple[i+1:]:
            return False

        # Here, new & old differ by only a single part, so set their
        # pattern_tuple_base's with a '$' at that differing part.
        # Optimize by reusing old's pattern_tuple_base.
        if not old.pattern_tuple_base:
            old_list = list(old_tuple)
            old_list[i] = "$"
            old.pattern_tuple_base = tuple(old_list)

        new.pattern_tuple_base = old.pattern_tuple_base

        return True


# Scan the log entries, plotting them based on the pattern info's.
def scan_to_plot(argv, file_patterns, pattern_ranks,
                 num_unique_timestamps, first_timestamp):
    argument_parser = logmerge.add_arguments(new_argument_parser())

    args = argument_parser.parse_args(argv[1:])

    # Sort the dir names, with any common prefix already stripped.
    dirs, dirs_sorted = \
        rank_dirs(logmerge.expand_paths(args.path, "/*" + args.suffix))

    # Initialize plotter.
    width_dir = len(pattern_ranks) + 1  # Width of a single dir.

    width = timestamp_gutter_width + \
        width_dir * len(dirs)  # First pixel is encoded seconds.

    height = 1 + num_unique_timestamps
    if height > max_image_height and max_image_height > 0:
        height = max_image_height

    height_text = 15

    datetime_base = parser.parse(first_timestamp, fuzzy=True)

    datetime_2010 = parser.parse("2010-01-01 00:00:00")

    start_minutes_since_2010 = \
        int((datetime_base - datetime_2010).total_seconds() / 60.0)

    def on_start_image(p):
        # Encode the start_minutes_since_2010 at line 0.
        p.draw.line((0, 0, timestamp_gutter_width - 1, 0),
                    fill=to_rgb(start_minutes_since_2010))
        p.cur_y = 1

        # Draw background of vertical lines to demarcate each file in
        # each dir, and draw dir and file_name text.
        for d, dir in enumerate(dirs_sorted):
            x_base = width_dir * d

            x = timestamp_gutter_width + \
                x_base + (width_dir - 1)

            p.draw.line([x, 0, x, height], fill="red")

            y_text = 0

            p.draw.text((timestamp_gutter_width + x_base, y_text),
                        dir, fill="#669")
            y_text += height_text

            file_names = file_patterns.keys()
            file_names.sort()

            for file_name in file_names:
                x = timestamp_gutter_width + \
                    x_base + pattern_ranks[file_name]

                p.draw.line([x, 0, x, height], fill="#363")

                p.draw.text((x, y_text),
                            file_name, fill="#336")
                y_text += height_text

    p = Plotter(width, height, on_start_image)

    p.start_image()

    def plot_visitor(path, timestamp, entry, entry_size):
        if (not timestamp) or (not entry):
            return

        pattern = entry_to_pattern(entry)
        if not pattern:
            return

        pattern_tuple = tuple(pattern)

        file_name = os.path.basename(path)

        patterns = file_patterns.get(file_name)
        if not patterns:
            return

        pattern_info = patterns[pattern_tuple]

        if pattern_info.pattern_tuple_base:
            pattern_tuple = pattern_info.pattern_tuple_base

        rank = pattern_ranks[file_name + ": " + str(pattern_tuple)]

        rank_dir = dirs[os.path.dirname(path)]

        x = (rank_dir * width_dir) + rank

        timestamp_changed, im_changed = \
            p.plot(timestamp[:timestamp_prefix_len], x)

        if timestamp_changed:
            datetime_cur = parser.parse(timestamp, fuzzy=True)

            delta_seconds = int((datetime_cur - datetime_base).total_seconds())

            p.draw.line((0, p.cur_y, timestamp_gutter_width - 1, p.cur_y),
                        fill=to_rgb(delta_seconds))

        if (not im_changed) and (re_erro.search(entry[0]) is not None):
            # Mark ERRO with a red triangle.
            p.draw.polygon((x, p.cur_y,
                            x+2, p.cur_y+3,
                            x-2, p.cur_y+3),
                           fill="#933")

    # Driver for visitor callbacks comes from logmerge.
    logmerge.main_with_args(args, visitor=plot_visitor)

    p.finish_image()

    print "len(dirs)", len(dirs)
    print "len(file_patterns)", len(file_patterns)
    print "len(pattern_ranks)", len(pattern_ranks)
    print "num_unique_timestamps", num_unique_timestamps
    print "first_timestamp", first_timestamp
    print "p.im_num", p.im_num
    print "p.plot_num", p.plot_num


class Plotter(object):
    white = "white"

    def __init__(self, width, height, on_start_image):
        self.width = width
        self.height = height
        self.on_start_image = on_start_image

        self.im = None
        self.im_num = 0
        self.draw = None
        self.cur_y = 0
        self.cur_timestamp = None
        self.plot_num = 0

    def start_image(self):
        self.im = Image.new("RGB", (self.width, self.height))
        self.draw = ImageDraw.Draw(self.im)
        self.cur_y = 0
        self.cur_timestamp = None

        if self.on_start_image:
            self.on_start_image(self)

    def finish_image(self):
        self.im.save("out-" + "{0:0>3}".format(self.im_num) + ".png")
        self.im.close()
        self.im_num += 1
        self.draw = None
        self.cur_y = None

    def plot(self, timestamp, x):
        cur_timestamp_changed = False
        if self.cur_timestamp != timestamp:
            cur_timestamp_changed = True

            self.cur_y += 1  # Move to next line.

        cur_im_changed = False
        if self.cur_y > self.height:
            cur_im_changed = True

            self.finish_image()
            self.start_image()

        self.draw.point((timestamp_gutter_width + x, self.cur_y),
                        fill=self.white)

        self.cur_timestamp = timestamp

        self.plot_num += 1

        return cur_timestamp_changed, cur_im_changed


def rank_dirs(paths):
    path_prefix = os.path.commonprefix(paths)  # Strip common prefix.

    dirs = {}
    for path in paths:
        dirs[os.path.dirname(path[len(path_prefix):])] = True

    dirs_sorted = dirs.keys()
    dirs_sorted.sort()

    for i, dir in enumerate(dirs_sorted):
        dirs[dir] = i

    return dirs, dirs_sorted


def to_rgb(v):
    b = v & 255
    g = (v >> 8) & 255
    r = (v >> 16) & 255

    return (r, g, b)


def http_server(argv, port):
    argv = [arg for arg in argv
            if (not arg.startswith("--http")) and
               (not arg.startswith("--out"))]

    class Handler(SimpleHTTPServer.SimpleHTTPRequestHandler):
        def do_GET(self):
            p = urlparse.urlparse(self.path)

            if p.path == '/logan-drill':
                return handle_drill(self, p, argv)

            if p.path == '/':
                self.path = '/logan.html'

            return SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

    port_num = int(port)

    SocketServer.TCPServer.allow_reuse_address = True

    server = SocketServer.TCPServer(('0.0.0.0', port_num), Handler)

    print "http server started - http://localhost:" + port

    server.serve_forever()


def handle_drill(req, p, argv):
    q = urlparse.parse_qs(p.query)

    if not q.get("start"):
        req.send_response(404)
        req.end_headers()
        req.wfile.close()
        return

    start = q.get("start")[0]

    near = start
    if q.get("near"):
        near = q.get("near")[0]

    req.send_response(200)
    req.send_header("Content-type", "text/plain")
    req.end_headers()

    cmd = ["./logmerge.py", "--out=--",
           "--start=" + start, "--near=" + near] + argv[1:]

    req.wfile.write(" ".join(cmd))
    req.wfile.write("\n\n")

    subprocess.call(cmd, stdout=req.wfile)

    req.wfile.close()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
