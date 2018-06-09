#!/usr/bin/env python
# -*- mode: Python;-*-

import __builtin__
import argparse
import json
import keyword
import multiprocessing
import os
import re
import signal
import subprocess
import sys

from dateutil import parser

from PIL import Image, ImageDraw

import SimpleHTTPServer
import SocketServer
import urlparse

# See progressbar2 https://github.com/WoLpH/python-progressbar
# Ex: pip install progressbar2
import progressbar

import logmerge


timestamp_gutter_width = 1  # In pixels.

max_image_height = 0  # 0 means unlimited plot image height.


def main(argv):
    args = new_argument_parser().parse_args(argv[1:])

    if (args.steps is None) and args.http:
        args.steps = "http"

    if (args.steps is None):
        args.steps = "scan,save,plot"

    # Since logan invokes logmerge, set any args needed by logmerge.

    args.out = os.devnull
    args.fields = None
    args.max_entries = None
    args.max_lines_per_entry = 0.5  # Only need first lines for logan.
    args.scan_start = None
    args.scan_length = None
    args.single_line = None
    args.timestamp_prefix = None
    args.wrap = None
    args.wrap_indent = None

    main_steps(argv, args)


# These are args known by logan but not by logmerge.
arg_names = ["chunk-size", "http", "multiprocessing",
             "out-prefix", "repo", "steps"]


def new_argument_parser():
    ap = argparse.ArgumentParser(
        description="""%(prog)s provides log analysis
                       (extends logmerge.py feature set)""")

    ap.add_argument('--chunk-size', type=int, default=2,
                    help="""split large log files into smaller chunks
                    (in MB) when multiprocessing; use 0 for no chunking
                    (default: %(default)s MB)""")

    ap.add_argument('--http', type=str,
                    help="""when specified, this option overrides
                    the default processing steps to be 'http'
                    in order to allow the analysis / plot to be
                    interactively browsed;
                    the HTTP is the port number to listen on""")

    ap.add_argument('--multiprocessing', type=int, default=0,
                    help="""number of processes to use for multiprocessing;
                    use 0 for default cpu count,
                    use -1 to disable multiprocessing
                    (default: %(default)s)""")

    ap.add_argument('--out-prefix', type=str, default="out-logan",
                    help="""when the processing steps include
                    'load', 'save' or 'plot', the persisted files
                    will have this prefix, like $(out-prefix)-000.png,
                    $(out-prefix).json and $(out-prefix)-scan.json
                    (default: %(default)s)""")

    ap.add_argument('--repo', type=str,
                    help="""optional directory to source code repo""")

    ap.add_argument('--steps', type=str,
                    help="""processing steps are a comma separated list,
                    where valid steps are: load, scan, save, plot, http;
                    (default: scan,save,plot)""")

    # Subset of arguments shared with logmerge.

    logmerge.add_path_arguments(ap)
    logmerge.add_match_arguments(ap)
    logmerge.add_time_range_arguments(ap)

    return ap


def main_steps(argv, args, scan_info=None):
    if args.multiprocessing >= 0:
        signal.signal(signal.SIGINT, on_sigint)

    steps = args.steps.split(",")

    if "load" in steps:
        print "\n============================================"
        scan_file = args.out_prefix + "-scan.json"
        print "loading scan info file:", scan_file
        with open(scan_file, 'r') as f:
            scan_info = json.load(f, object_hook=byteify)

    if "scan" in steps:
        print "\n============================================"
        print "scanning..."
        scan_info = scan(argv, args)

    if "save" in steps:
        print "\n============================================"
        scan_file = args.out_prefix + "-scan.json"
        print "saving scan info file:", scan_file
        with open(scan_file, 'w') as f:
            json.dump(scan_info, f, separators=(',', ':'))

        print "\nwrote", scan_file

    if "plot" in steps:
        print "\n============================================"
        print "plotting..."
        plot(argv, args, scan_info)

        plot_info = dict(scan_info)  # Copy before modifying.
        del plot_info["file_patterns"]  # Too big / unused for plot_info.

        plot_file = args.out_prefix + ".json"
        with open(plot_file, 'w') as f:
            f.write(json.dumps(plot_info))

        print "wrote", plot_file

    if "http" in steps:
        print "\n============================================"
        http_server(argv, args)


def scan(argv, args):
    if args.multiprocessing >= 0:
        file_patterns, num_unique_timestamps = scan_multiprocessing(args)
    else:
        file_patterns, num_unique_timestamps = scan_file_patterns(args)

    # Process the pattern info's to find similar pattern info's.
    mark_similar_pattern_infos(file_patterns)

    print "\n============================================"

    print "len(file_patterns)", len(file_patterns)

    num_entries = 0

    num_pattern_infos = 0
    num_pattern_infos_base = 0
    num_pattern_infos_base_none = 0

    # Categorize the unique "file_name: pattern_key"'s, also
    # considering shared pattern_base's.  The value is the total
    # number of entries seen.
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
                pattern_uniques.get(k, 0) + pattern_info_total

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
        "git_describe_long":           git_describe_long(),
        "argv":                        argv,
        "paths":                       args.path,
        "file_patterns":               file_patterns,
        "num_entries":                 num_entries,
        "num_pattern_infos":           num_pattern_infos,
        "num_pattern_infos_base":      num_pattern_infos_base,
        "num_pattern_infos_base_none": num_pattern_infos_base_none,
        "pattern_ranks":               pattern_ranks,
        "first_timestamp":             first_timestamp,
        "num_unique_timestamps":       num_unique_timestamps,
        "timestamp_gutter_width":      timestamp_gutter_width
    }

    return scan_info


def scan_multiprocessing(args):
    paths, total_size, path_sizes = \
        logmerge.expand_paths(args.path, args.suffix)

    chunks = chunkify_path_sizes(path_sizes,
                                 (args.chunk_size or 0) * 1024 * 1024)

    q = multiprocessing.Manager().Queue()

    pool_processes = args.multiprocessing or multiprocessing.cpu_count()

    pool = multiprocessing.Pool(processes=pool_processes)

    results = pool.map_async(
        scan_multiprocessing_worker,
        [(chunk, args, q) for chunk in chunks])

    pool.close()

    multiprocessing_wait(q, len(chunks), total_size)

    pool.join()

    return scan_multiprocessing_join(results.get())


# Joins all the results received from workers.
def scan_multiprocessing_join(results):
    file_patterns = {}

    path_unique_timestamps = {}

    for result in results:
        for file_name, r_patterns in result["file_patterns"].iteritems():
            patterns = file_patterns.get(file_name)
            if not patterns:
                file_patterns[file_name] = r_patterns
            else:
                for pattern_key, r_pattern_info in r_patterns.iteritems():
                    pattern_info = patterns.get(pattern_key)
                    if not pattern_info:
                        patterns[pattern_key] = r_pattern_info
                    else:
                        r_ft = r_pattern_info["first_timestamp"]
                        if pattern_info["first_timestamp"] > r_ft:
                            pattern_info["first_timestamp"] = r_ft

                        pattern_info["total"] += r_pattern_info["total"]

        path = result["path"]
        path_unique_timestamps[path] = \
            path_unique_timestamps.get(path, 0) + \
            result["num_unique_timestamps"]

    # Estimate the overall num_unique_timestamps heuristically.
    sum_unique_timestamps = sum(path_unique_timestamps.itervalues())

    max_unique_timestamps = max(path_unique_timestamps.itervalues())

    num_unique_timestamps = max(int(sum_unique_timestamps /
                                    len(path_unique_timestamps)) + 1,
                                int(max_unique_timestamps * 1.5))

    return file_patterns, num_unique_timestamps


# Worker that scans a single chunk.
def scan_multiprocessing_worker(work):
    chunk, args, q = work

    path, scan_start, scan_length = chunk

    patterns = {}

    timestamp_file_name = \
        args.out_prefix + "-chunk-" + \
        str(chunk).replace('/', '_').replace('-', '_').replace(' ', '') + \
        "-timestamps.txt"

    timestamp_info = TimestampInfo(timestamp_file_name)

    # Optimize to ignore a path check, as the path should equal path_ignored.
    def v(path_ignored, timestamp, entry, entry_size):
        if (not timestamp) or (not entry):
            return

        update_patterns_with_entry(patterns, timestamp, entry, timestamp_info)

    # Main driver of visitor callbacks is reused from logmerge.
    args.path = [path]
    args.scan_start = scan_start
    args.scan_length = scan_length

    logmerge.main_with_args(args, visitor=v, bar=QueueBar(chunk, q))

    timestamp_info.flush()
    timestamp_info.close()

    file_patterns = {}

    if patterns:
        file_patterns[os.path.basename(path)] = patterns

    q.put("done", False)

    return {
        "path": path,
        "chunk": chunk,
        "file_patterns": file_patterns,
        "num_unique_timestamps": timestamp_info.num_unique
    }


# Single-threaded scan of all the logs to build file_patterns.
def scan_file_patterns(args, bar=None):
    visitor, file_patterns, timestamp_info = scan_file_patterns_visitor(args)

    # Main driver of visitor callbacks is reused from logmerge.
    logmerge.main_with_args(args, visitor=visitor, bar=bar)

    timestamp_info.flush()
    timestamp_info.close()

    return file_patterns, timestamp_info.num_unique


# Returns a visitor that can categorize entries from different files.
def scan_file_patterns_visitor(args):
    # Keyed by file name, value is dict of pattern => PatternInfo.
    file_patterns = {}

    timestamp_info = TimestampInfo(args.out_prefix + "-timestamps.txt")

    def v(path, timestamp, entry, entry_size):
        if (not timestamp) or (not entry):
            return

        file_name = os.path.basename(path)

        patterns = file_patterns.get(file_name)
        if patterns is None:
            patterns = {}
            file_patterns[file_name] = patterns

        update_patterns_with_entry(patterns, timestamp, entry, timestamp_info)

    return v, file_patterns, timestamp_info


# Updates the patterns dict with the timestamp / entry.
def update_patterns_with_entry(patterns, timestamp, entry, timestamp_info):
    pattern = entry_to_pattern(entry)
    if not pattern:
        return

    pattern_key = str(pattern)

    pattern_info = patterns.get(pattern_key)
    if not pattern_info:
        pattern_info = make_pattern_info(pattern, timestamp)
        patterns[pattern_key] = pattern_info

    # Increment the total count of instances of this pattern.
    pattern_info["total"] += 1

    timestamp_info.update(timestamp)


class TimestampInfo(object):
    def __init__(self, file_name):
        self.file_name = file_name

        self.last = None
        self.num_unique = 0  # Number of unique timestamp bins.

        self.f = None

        self.recent_num = 0
        self.recent = 1000 * [None]

    def close(self):
        if self.f:
            self.f.close()
            self.f = None

    def flush(self):
        if self.recent_num > 0:
            if not self.f:
                self.f = open(self.file_name, "wb")

            self.f.write("\n".join(self.recent[0:self.recent_num]))
            self.f.write("\n")

            self.recent_num = 0

    def update(self, timestamp):
        timestamp = timestamp[:timestamp_prefix_len]

        if self.last != timestamp:
            self.last = timestamp
            self.num_unique += 1

            self.recent[self.recent_num] = timestamp
            self.recent_num += 1
            if self.recent_num >= len(self.recent):
                self.flush()


# Need 32 hex chars for a uid pattern.
pattern_uid = "[a-f0-9]" * 32

ex_uid1 = "572527a076445ff8_6ddbfb54"

pattern_uid1a = "[\-_]?" + ("[a-f0-9]" * len(ex_uid1.split("_")[0]))

pattern_uid1b = "[\-_]?" + ("[a-f0-9]" * len(ex_uid1.split("_")[1]))

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
    ("rev", pattern_rev),
    ("uid", pattern_uid),
    ("uid", pattern_uid1a),
    ("uid", pattern_uid1b),
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


def entry_to_pattern(entry):
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
    plot_scan_info(argv, args, scan_info)


# Single-threaded plot of the scan_info.
def plot_scan_info(argv, args, scan_info):
    file_patterns = scan_info["file_patterns"]
    pattern_ranks = scan_info["pattern_ranks"]
    first_timestamp = scan_info["first_timestamp"]
    num_unique_timestamps = scan_info["num_unique_timestamps"]

    if not (file_patterns and pattern_ranks and
            first_timestamp and num_unique_timestamps):
        return

    dirs, width_dir, datetime_base, image_files, p = \
        plot_init(args.path, args.suffix, args.out_prefix, scan_info)

    def plot_visitor(path, timestamp, entry, entry_size):
        if (not timestamp) or (not entry):
            return

        pattern = entry_to_pattern(entry)
        if not pattern:
            return

        file_name = os.path.basename(path)

        patterns = file_patterns.get(file_name)
        if not patterns:
            return

        pattern_key = str(pattern)

        pattern_info = patterns[pattern_key]

        if pattern_info["pattern_base"]:
            pattern_key = str(pattern_info["pattern_base"])

        rank = pattern_ranks.get(file_name + ": " + pattern_key)
        if rank is None:
            return

        rank_dir = dirs.get(os.path.dirname(path))
        if rank_dir is None:
            return

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


def plot_init(paths_in, suffix, out_prefix, scan_info):
    file_patterns = scan_info["file_patterns"]
    pattern_ranks = scan_info["pattern_ranks"]
    first_timestamp = scan_info["first_timestamp"]
    num_unique_timestamps = scan_info["num_unique_timestamps"]

    # Sort the dir names, with any common prefix already stripped.
    paths, total_size, path_sizes = \
        logmerge.expand_paths(paths_in, suffix)

    dirs, dirs_sorted, path_prefix = sort_dirs(paths)

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

    p = Plotter(out_prefix, width, height, on_start_image)

    p.start_image()

    return dirs, width_dir, datetime_base, image_files, p


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

        self.min_x = self.width
        self.min_y = self.height

        self.max_x = -1
        self.max_y = -1

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

        cur_x = timestamp_gutter_width + x
        cur_y = self.cur_y

        self.draw.point((cur_x, cur_y), fill=self.white)

        self.min_x = min(self.min_x, cur_x)
        self.min_y = min(self.min_y, cur_y)

        self.max_x = max(self.max_x, cur_x)
        self.max_y = max(self.max_y, cur_y)

        self.cur_timestamp = timestamp

        self.plot_num += 1

        return cur_timestamp_changed, cur_im_changed


def to_rgb(v):
    b = v & 255
    g = (v >> 8) & 255
    r = (v >> 16) & 255

    return (r, g, b)


def sort_dirs(paths):
    path_prefix = os.path.commonprefix(paths)  # Strip common prefix.

    dirs = {}
    for path in paths:
        dirs[os.path.dirname(path[len(path_prefix):])] = True

    dirs_sorted = dirs.keys()
    dirs_sorted.sort()

    for i, dir in enumerate(dirs_sorted):
        dirs[dir] = i

    return dirs, dirs_sorted, path_prefix


def chunkify_path_sizes(path_sizes, default_chunk_size):
    chunks = []

    for path, size in path_sizes.iteritems():
        chunk_size = default_chunk_size or size

        x = 0
        while size and x < size and chunk_size:
            chunks.append((path, x, chunk_size))
            x += chunk_size

    chunks.sort()

    return chunks


# Allows the parent process to wait until there are enough done worker
# messages, while also keeping a progress bar updated.
def multiprocessing_wait(q, num_chunks, total_size):
    bar = progressbar.ProgressBar(max_value=total_size)

    num_done = 0
    progress = {}

    while num_done < num_chunks:
        bar.update(sum(progress.itervalues()))

        x = q.get()
        if x == "done":
            num_done += 1
        else:
            chunk, amount = x
            progress[chunk] = amount


def http_server(argv, args):
    clean_argv = []
    for arg in argv:
        found = [s for s in arg_names if arg.startswith("--" + s)]
        if not found:
            clean_argv.append(arg)

    class Handler(SimpleHTTPServer.SimpleHTTPRequestHandler):
        def translate_path(self, path):
            if self.path in ["/logan.html", "/logan-vr.html"]:
                return os.path.dirname(os.path.realpath(__file__)) + self.path

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

    req.wfile.write("\n\n=============================================\n")

    if repo:
        if q.get("terms"):
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
        else:
            req.wfile.write("(no terms for source code grep)\n\n""")
    else:
        req.wfile.write("(please provide --repo=/path/to/source/repo" +
                        " for source code grep)\n\n""")

    req.wfile.close()


def repo_grep_terms(repo, terms):
    if not terms:
        return ["error"], "(not enough terms to repo grep)"

    regexp = "|".join(terms)

    cmd = ["repo", "grep", "-n", "-E", regexp]

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


def git_describe_long():
    return subprocess.check_output(
        ['git', 'describe', '--long'],
        cwd=os.path.dirname(os.path.realpath(__file__))).strip()


# QueueBar implements a subset of progress bar methods, forwarding
# update() invocations to a queue.
class QueueBar(object):
    def __init__(self, chunk, q):
        self.chunk = chunk
        self.q = q

    def start(self, max_value=None):
        pass  # Ignore since parent has an aggregate max_value.

    def update(self, amount):
        self.q.put((self.chunk, amount), False)


# See: https://stackoverflow.com/questions/956867/
#      how-to-get-string-objects-instead-of-unicode-from-json
def byteify(data, ignore_dicts=False):
    if isinstance(data, unicode):
        return data.encode('utf-8')

    if isinstance(data, list):
        return [byteify(item, ignore_dicts=True) for item in data]

    if isinstance(data, dict) and not ignore_dicts:
        return {
            byteify(key, ignore_dicts=True): byteify(value, ignore_dicts=True)
            for key, value in data.iteritems()
        }

    return data


def on_sigint(signum, frame):
    print("SIGINT received")
    os._exit(1)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
