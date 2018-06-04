#!/usr/bin/env python
# -*- mode: Python;-*-

import __builtin__
import argparse
import collections
import json
import keyword
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

max_image_height = 0  # 0 means unlimited plot image height.


def main(argv):
    args = new_argument_parser().parse_args(argv[1:])

    args.out = "/dev/null"  # For any invocations of logmerge.

    if (args.steps is None) and args.http:
        args.steps = "http"

    if (args.steps is None):
        args.steps = "scan,save,plot"

    steps = args.steps.split(",")

    scan_info = None

    if "load" in steps:
        print "\n============================================"
        print "loading scan info file:", args.scan_file
        with open(args.scan_file, 'r') as f:
            scan_info = json.load(f)

    if "scan" in steps:
        print "\n============================================"
        print "scanning..."
        scan_info = scan(argv, args)

    if "save" in steps:
        print "\n============================================"
        print "saving scan info file:", args.scan_file
        with open(args.scan_file, 'w') as f:
            f.write(json.dumps(scan_info))

        print "wrote", args.scan_file

    if "plot" in steps:
        print "\n============================================"
        print "plotting..."
        plot(argv, args, scan_info)

        plot_info = dict(scan_info)  # Copy before modifying.
        del plot_info["file_patterns"]

        plot_file = args.plot_prefix + ".json"
        with open(plot_file, 'w') as f:
            f.write(json.dumps(plot_info))

        print "wrote", plot_file

    if "http" in steps:
        print "\n============================================"
        http_server(argv, args)
        return


def new_argument_parser():
    ap = argparse.ArgumentParser(
        description="""%(prog)s provides log analysis
                       (extends logmerge.py feature set)""")

    ap.add_argument('--http', type=str,
                    help="""when specified, this option overrides
                    the default processing steps to be 'http'
                    in order to allow the analysis / plot to be
                    interactively browsed;
                    the HTTP is the port number to listen on""")

    ap.add_argument('--scan-file', type=str, default="scan.json",
                    help="""when the processing steps include
                    'load' or 'save', the scan info will be
                    loaded from and/or saved to this file
                    (default: %(default)s)""")

    ap.add_argument('--plot-prefix', type=str, default="out",
                    help="""when the processing steps include
                    'plot', the plot images will be saved to
                    files named like $(plot-prefix)-000.png and
                    $(plot-prefix).json (default: %(default)s)""")

    ap.add_argument('--repo', type=str,
                    help="""optional directory to source code repo""")

    ap.add_argument('--steps', type=str,
                    help="""processing steps are a comma separated list,
                    where valid steps are: load, scan, save, plot, http;
                    (default: scan,save,plot)""")

    # Subset of arguments shared with logmerge.

    logmerge.add_path_arguments(ap)
    logmerge.add_match_arguments(ap)
    logmerge.add_timerange_arguments(ap)
    logmerge.add_advanced_arguments(ap)

    return ap


def scan(argv, args):
    # Scan the logs to build the pattern info's with a custom visitor.
    visitor, file_pos_term_counts, file_patterns, timestamp_info = \
        scan_patterns_visitor()

    # Main driver of visitor callbacks is reused from logmerge.
    logmerge.main_with_args(args, visitor=visitor)

    # Process the pattern info's to find similar pattern info's.
    mark_similar_pattern_infos(file_patterns)

    print "\n============================================"

    print "len(file_patterns)", len(file_patterns)

    num_entries = 0

    num_pattern_infos = 0
    num_pattern_infos_base = 0
    num_pattern_infos_base_none = 0

    # For categorizing the unique "file_name: pattern_key"'s when
    # shared pattern_base's are also considered.  The value is the
    # total number of entries seen.
    pattern_uniques = {}

    first_timestamp = None

    for file_name, patterns in file_patterns.iteritems():
        num_pattern_infos += len(patterns)

        print "  ", file_name
        print "    len(patterns)", len(patterns)

        pattern_keys = patterns.keys()
        pattern_keys.sort()

        num_entries_file = 0

        for i, pattern_key in enumerate(pattern_keys):
            pattern_info = patterns[pattern_key]

            pattern_info_total = pattern_info["total"]

            num_entries += pattern_info_total
            num_entries_file += pattern_info_total

            if pattern_info["pattern_base"]:
                pattern_key = str(pattern_info["pattern_base"])

                num_pattern_infos_base += 1
            else:
                num_pattern_infos_base_none += 1

            k = file_name + ": " + pattern_key

            pattern_uniques[k] = \
                pattern_uniques.get(k, 0) + \
                pattern_info_total

            if (not first_timestamp) or \
               (first_timestamp > pattern_info["first_timestamp"]):
                first_timestamp = pattern_info["first_timestamp"]

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

    scan_info = {
        "argv":                        argv,
        "paths":                       args.path,
        "file_patterns":               file_patterns,
        "num_entries":                 num_entries,
        "num_pattern_infos":           num_pattern_infos,
        "num_pattern_infos_base":      num_pattern_infos_base,
        "num_pattern_infos_base_none": num_pattern_infos_base_none,
        "pattern_ranks":               pattern_ranks,
        "first_timestamp":             first_timestamp,
        "num_unique_timestamps":       timestamp_info.num_unique,
        "timestamp_gutter_width":      timestamp_gutter_width
    }

    return scan_info


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

        pattern_key = str(pattern)

        pattern_info = patterns.get(pattern_key)
        if not pattern_info:
            pattern_info = make_pattern_info(pattern, timestamp)
            patterns[pattern_key] = pattern_info

        # Increment the total count of instances of this pattern.
        pattern_info["total"] += 1

        timestamp_bin = timestamp[:timestamp_prefix_len]
        if timestamp_info.last != timestamp_bin:
            timestamp_info.last = timestamp_bin
            timestamp_info.num_unique += 1

    return v, file_pos_term_counts, file_patterns, timestamp_info


class TimestampInfo:
    def __init__(self):
        self.last = None
        self.num_unique = 0  # Number of unique timestamp bins.


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


def make_pattern_info(pattern, first_timestamp):
    return {
        "pattern": pattern,
        "pattern_base": None,
        "first_timestamp": first_timestamp,
        "total": 0
    }


# Find and mark similar pattern info's.
def mark_similar_pattern_infos(file_patterns, scan_distance=10):
    for file_name, patterns in file_patterns.iteritems():
        pattern_keys = patterns.keys()
        pattern_keys.sort()  # Sort so similar patterns are nearby.

        for i, pattern_key in enumerate(pattern_keys):
            curr_pattern_info = patterns[pattern_key]

            scan_idx = i - 1
            scan_until = i - scan_distance

            while scan_idx >= 0 and scan_idx > scan_until:
                prev_pattern_key = pattern_keys[scan_idx]
                prev_pattern_info = patterns[prev_pattern_key]

                if mark_similar_pattern_info_pair(curr_pattern_info,
                                                  prev_pattern_info):
                    break

                scan_idx -= 1


# Examine a new and old pattern info, and if they're similar (only
# differing by a single part), mark them and return True.
def mark_similar_pattern_info_pair(new, old):
    new_pattern = new["pattern"]
    old_pattern = old["pattern"]

    if len(new_pattern) != len(old_pattern):
        return False

    if old["pattern_base"]:
        old_pattern = old["pattern_base"]

    for i in range(len(new_pattern)):
        if new_pattern[i] == old_pattern[i]:
            continue

        # See if remaining pattern parts are different.
        if new_pattern[i+1:] != old_pattern[i+1:]:
            return False

        # Here, new & old differ by only a single part, so set their
        # pattern_base's with a '$' at that differing part.  Optimize
        # by reusing old's pattern_base.
        if not old["pattern_base"]:
            p = list(old_pattern)  # Copy.
            p[i] = "$"
            old["pattern_base"] = p

        new["pattern_base"] = old["pattern_base"]

        return True


# Scan the log entries, plotting them based on the given scan info.
def plot(argv, args, scan_info):
    file_patterns = scan_info["file_patterns"]
    pattern_ranks = scan_info["pattern_ranks"]
    first_timestamp = scan_info["first_timestamp"]
    num_unique_timestamps = scan_info["num_unique_timestamps"]

    # Sort the dir names, with any common prefix already stripped.
    dirs, dirs_sorted = \
        sort_dirs(logmerge.expand_paths(args.path, args.suffix))

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

    image_files = []

    def on_start_image(p):
        image_files.append(p.im_name)

        # Encode the start_minutes_since_2010 at line 0's timestamp gutter.
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

    p = Plotter(args.plot_prefix, width, height, on_start_image)

    p.start_image()

    def plot_visitor(path, timestamp, entry, entry_size):
        if (not timestamp) or (not entry):
            return

        pattern = entry_to_pattern(entry)
        if not pattern:
            return

        pattern_key = str(pattern)

        file_name = os.path.basename(path)

        patterns = file_patterns.get(file_name)
        if not patterns:
            return

        pattern_info = patterns[pattern_key]

        if pattern_info["pattern_base"]:
            pattern_key = str(pattern_info["pattern_base"])

        rank = pattern_ranks.get(file_name + ": " + pattern_key)
        if rank is None:
            return

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
    print "len(pattern_ranks)", len(pattern_ranks)
    print "num_unique_timestamps", num_unique_timestamps
    print "first_timestamp", first_timestamp
    print "p.im_num", p.im_num
    print "p.plot_num", p.plot_num
    print "image_files", image_files

    return image_files


class Plotter(object):
    white = "white"

    def __init__(self, prefix, width, height, on_start_image):
        self.prefix = prefix
        self.width = width
        self.height = height
        self.on_start_image = on_start_image

        self.im = None
        self.im_num = 0
        self.im_name = None
        self.draw = None
        self.cur_y = 0
        self.cur_timestamp = None
        self.plot_num = 0

    def start_image(self):
        self.im = Image.new("RGB", (self.width, self.height))
        self.im_name = self.prefix + "-" + \
            "{0:0>3}".format(self.im_num) + ".png"
        self.draw = ImageDraw.Draw(self.im)
        self.cur_y = 0
        self.cur_timestamp = None

        if self.on_start_image:
            self.on_start_image(self)

    def finish_image(self):
        self.im.save(self.im_name)
        self.im.close()
        self.im = None
        self.im_num += 1
        self.im_name = None
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


def sort_dirs(paths):
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


def http_server(argv, args):
    strip = ["--http", "--info-file", "--plot-file", "--repo", "--steps"]

    clean_argv = []
    for arg in argv:
        found = [s for s in strip if arg.startswith(s)]
        if not found:
            clean_argv.append(arg)

    class Handler(SimpleHTTPServer.SimpleHTTPRequestHandler):
        def translate_path(self, path):
            if self.path == "/logan.html":
                return os.path.dirname(os.path.realpath(__file__)) + \
                    "/logan.html"

            return SimpleHTTPServer.SimpleHTTPRequestHandler.translate_path(
                self, path)

        def do_GET(self):
            p = urlparse.urlparse(self.path)

            if p.path == '/logan-drill':
                return handle_drill(self, p, clean_argv, args.repo)

            if p.path == '/':
                self.path = '/logan.html'

            return SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

    port_num = int(args.http)

    SocketServer.TCPServer.allow_reuse_address = True

    server = SocketServer.TCPServer(('0.0.0.0', port_num), Handler)

    print "http server started..."

    print "  http://localhost:" + str(port_num)

    server.serve_forever()


re_term_disallowed = re.compile(r"[^a-zA-Z0-9\-_/]")


def handle_drill(req, p, argv, repo):
    q = urlparse.parse_qs(p.query)

    if not q.get("start"):
        req.send_response(404)
        req.end_headers()
        req.wfile.close()
        return

    start = q.get("start")[0]

    max_entries = "1000"
    if q.get("max_entries"):
        max_entries = q.get("max_entries")[0]

    req.send_response(200)
    req.send_header("Content-type", "text/plain")
    req.end_headers()

    # Have logmerge.py emit to stdout.
    req.wfile.write("q: " + str(q))
    req.wfile.write("\n")

    req.wfile.write("\n=============================================\n")
    cmd = [os.path.dirname(os.path.realpath(__file__)) + "/logmerge.py",
           "--out=--", "--max-entries=" + max_entries, "--start=" + start] + \
        argv[1:]

    req.wfile.write(" ".join(cmd))
    req.wfile.write("\n\n")

    subprocess.call(cmd, stdout=req.wfile)

    if repo and q.get("terms"):
        req.wfile.write("\n=============================================\n")

        terms = q.get("terms")[0].split(',')

        req.wfile.write("searching repo for terms: ")
        req.wfile.write(" ".join(terms))
        req.wfile.write("\n\n")

        terms = [re.sub(re_term_disallowed, '', term) for term in terms]
        terms = [term for term in terms if not keyword.iskeyword(term)]
        terms = [term for term in terms if not hasattr(__builtin__, term)]
        terms = [term for term in terms if len(term) >= 4]

        req.wfile.write("searching repo for terms (pre-filtered): ")
        req.wfile.write(" ".join(terms))
        req.wfile.write("\n\n")

        cmd, out = repo_grep_terms(repo, terms)

        req.wfile.write("filtered ")
        req.wfile.write(" ".join(cmd))
        req.wfile.write("\n\n")
        req.wfile.write(out)
        req.wfile.write("\n")

    req.wfile.close()


def repo_grep_terms(repo, terms):
    regexp = "|".join(terms)

    cmd = ["repo", "grep", "-n", "-E", regexp]

    if not terms:
        return cmd, "(not enough terms to repo grep)"

    repo = os.path.expanduser(repo)

    print repo
    print cmd

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, cwd=repo)

    rv = []

    best = []
    best_terms_left_len = len(terms)

    curr = None
    curr_terms_left = None
    curr_line_num = None

    for line in p.stdout:
        if len(line) > 1000:
            continue

        # A line looks like "fileName:lineNum:lineContent".
        line_parts = line.split(":")
        if len(line_parts) < 3:
            continue

        line_num = int(line_parts[1])

        # If we're still in the same fileName, on the very next
        # lineNum, and some new terms are now matching in the
        # lineContent, then extend the curr info.
        if (curr and
            curr[0][0] == line_parts[0] and
            curr_line_num + 1 == line_num and
            remove_matching(curr_terms_left,
                            line_parts[2:])):
            curr.append(line_parts)
            curr_line_num = line_num
        else:
            # Else start a new curr info.
            curr = [line_parts]
            curr_terms_left = list(terms)
            remove_matching(curr_terms_left, line_parts[2:])
            curr_line_num = line_num

        # See if we have a new best scoring curr.
        if best_terms_left_len >= len(curr_terms_left):
            best = list(curr)  # Copy.
            best_terms_left_len = len(curr_terms_left)

            rv.append("".join([":".join(x) for x in best]))
            if len(rv) > 10:
                rv = list(rv[-10:])

        if best_terms_left_len <= 0:
            break

    rv.reverse()

    return cmd, "\n".join(rv[:5])


def remove_matching(terms, parts):
    removed = False
    for part in parts:
        for part_term in re.split(re_term_disallowed, part):
            for i, term in enumerate(terms):
                if term == part_term:
                    del terms[i]
                    removed = True
                    break
    return removed


if __name__ == '__main__':
    sys.exit(main(sys.argv))
