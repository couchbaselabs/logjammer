#!/usr/bin/env python
# -*- mode: Python;-*-

import json
import multiprocessing
import os
import re
import signal
import subprocess
import sys

from dateutil import parser

from PIL import Image, ImageDraw

# See progressbar2 https://github.com/WoLpH/python-progressbar
# Ex: pip install progressbar2
import progressbar

import logmerge

from logan_args import new_argument_parser
from logan_http import http_server


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

        print "\nwrote", plot_file

    if "http" in steps:
        print "\n============================================"
        http_server(argv, args)


def scan(argv, args):
    if args.multiprocessing >= 0:
        file_patterns, timestamps_num_unique, timestamps_file_name = \
            scan_multiprocessing(args)
    else:
        file_patterns, timestamps_num_unique, timestamps_file_name = \
            scan_file_patterns(args)

    # Associate similar pattern info's.
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

    timestamp_first = None

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

            if (not timestamp_first) or \
               (timestamp_first > pattern_info["timestamp_first"]):
                timestamp_first = pattern_info["timestamp_first"]

        pattern_uniques[file_name] = num_entries_file

    print "\n============================================"

    print "num_entries", num_entries

    print "num_pattern_infos", num_pattern_infos
    print "num_pattern_infos_base", num_pattern_infos_base
    print "num_pattern_infos_base_none", num_pattern_infos_base_none

    print "len(pattern_uniques)", len(pattern_uniques)

    print "timestamp_first", timestamp_first

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
        "timestamp_first":             timestamp_first,
        "timestamps_num_unique":       timestamps_num_unique,
        "timestamps_file_name":        timestamps_file_name,
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

    return scan_multiprocessing_join(results.get(), args.out_prefix)


# Joins all the results received from workers.
def scan_multiprocessing_join(results, out_prefix):
    file_patterns = {}

    timestamps_file_names = []

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
                        r_ft = r_pattern_info["timestamp_first"]
                        if pattern_info["timestamp_first"] > r_ft:
                            pattern_info["timestamp_first"] = r_ft

                        pattern_info["total"] += r_pattern_info["total"]

        timestamps_file_name = result.get("timestamps_file_name")
        if timestamps_file_name:
            timestamps_file_names.append(timestamps_file_name)

    timestamps_file_name = out_prefix + "-timestamps.txt"

    subprocess.check_output(
        ['sort', '--merge', '--unique', '--output=' + timestamps_file_name] +
        timestamps_file_names)

    timestamps_num_unique = int(subprocess.check_output(
        ['wc', '-l', timestamps_file_name]).strip().split(' ')[0])

    for x in timestamps_file_names:
        os.remove(x)

    return file_patterns, timestamps_num_unique, timestamps_file_name


# Worker that scans a single chunk.
def scan_multiprocessing_worker(work):
    chunk, args, q = work

    path, scan_start, scan_length = chunk

    patterns = {}

    timestamp_info = TimestampInfo(
        args.out_prefix + "-chunk-" +
        str(chunk).replace('/', '_').replace('-', '_').replace(' ', '') +
        "-timestamps.txt")

    # Optimize to ignore a path check, as the path should equal path_ignored.
    def v(path_ignored, timestamp, entry, entry_size):
        if (not timestamp) or (not entry):
            return

        update_patterns_with_entry(patterns, timestamp, entry, timestamp_info)

    args.path = [path]
    args.scan_start = scan_start
    args.scan_length = scan_length

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
        "timestamps_file_name": timestamp_info.file_name
    }


# Single-threaded scan of all the logs to build file_patterns.
def scan_file_patterns(args, bar=None):
    visitor, file_patterns, timestamp_info = scan_file_patterns_visitor(args)

    # Main driver of visitor callbacks is reused from logmerge.
    logmerge.main_with_args(args, visitor=visitor, bar=bar)

    timestamp_info.flush()
    timestamp_info.close()

    return file_patterns, timestamp_info.num_unique, timestamp_info.file_name


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
        else:
            self.file_name = None

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


def make_pattern_info(pattern, timestamp_first):
    return {
        "pattern": pattern,
        "pattern_base": None,
        "timestamp_first": timestamp_first,
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
    timestamp_first = scan_info["timestamp_first"]
    timestamps_num_unique = scan_info["timestamps_num_unique"]

    if not (file_patterns and pattern_ranks and
            timestamp_first and timestamps_num_unique):
        return

    if args.multiprocessing >= 0:
        plot_multiprocessing_scan_info(args, scan_info)
    else:
        plot_scan_info(args, scan_info)


# Plot of the scan_info using multiprocessing.
def plot_multiprocessing_scan_info(args, scan_info):
    paths, total_size, path_sizes = \
        logmerge.expand_paths(args.path, args.suffix)

    chunks = chunkify_path_sizes(path_sizes,
                                 (args.chunk_size or 0) * 1024 * 1024)

    q = multiprocessing.Manager().Queue()

    pool_processes = args.multiprocessing or multiprocessing.cpu_count()

    pool = multiprocessing.Pool(processes=pool_processes)

    results = pool.map_async(
        plot_multiprocessing_worker,
        [(chunk, args, q) for chunk in chunks])

    pool.close()

    multiprocessing_wait(q, len(chunks), total_size)

    pool.join()

    return plot_multiprocessing_join(results.get())


# Worker that plots a single chunk.
def plot_multiprocessing_worker(work):
    chunk, args, q = work

    path, scan_start, scan_length = chunk

    file_name = os.path.basename(path)

    with open(args.out_prefix + "-scan.json", 'r') as f:
        scan_info = byteify(json.load(f, object_hook=byteify),
                            ignore_dicts=True)

    patterns = scan_info["file_patterns"].get(file_name)

    pattern_ranks = scan_info["pattern_ranks"]

    pattern_ranks_key_prefix = file_name + ": "

    chunk_out_prefix = args.out_prefix + "-chunk-" + \
        path.replace("/", "_").replace("-", "_") + "-" + \
        str(scan_start) + "-" + str(scan_length)

    image_files = bounds = None

    if patterns and pattern_ranks:
        dirs, path_prefix, width_dir, datetime_base, image_files, p = \
            plot_init(args.path, args.suffix, chunk_out_prefix, scan_info)

        rank_dir = dirs.get(os.path.dirname(path[len(path_prefix):]))
        if rank_dir is not None:
            x_base = rank_dir * width_dir

            def v(path_ignored, timestamp, entry, entry_size):
                if (not timestamp) or (not entry):
                    return

                plot_entry(patterns, pattern_ranks,
                           datetime_base, x_base,
                           pattern_ranks_key_prefix,
                           timestamp_prefix_len,
                           timestamp, entry, p)

            args.path = [path]
            args.scan_start = scan_start
            args.scan_length = scan_length

            # Driver for visitor callbacks comes from logmerge.
            logmerge.main_with_args(args, visitor=v, bar=QueueBar(chunk, q))

        p.finish_image()

        bounds = (p.min_x, p.min_y, p.max_x, p.max_y)

    q.put("done", False)

    return {
        "path": path,
        "chunk": chunk,
        "chunk_out_prefix": chunk_out_prefix,
        "image_files": image_files,
        "bounds": bounds
    }


def plot_multiprocessing_join(results):
    pass  # TODO.


# Single-threaded plot of the scan_info.
def plot_scan_info(args, scan_info):
    dirs, path_prefix, width_dir, datetime_base, image_files, p = \
        plot_init(args.path, args.suffix, args.out_prefix, scan_info)

    file_patterns = scan_info["file_patterns"]
    pattern_ranks = scan_info["pattern_ranks"]

    def plot_visitor(path, timestamp, entry, entry_size):
        if (not timestamp) or (not entry):
            return

        rank_dir = dirs.get(os.path.dirname(path))
        if rank_dir is None:
            return

        file_name = os.path.basename(path)

        patterns = file_patterns.get(file_name)
        if not patterns:
            return

        plot_entry(patterns, pattern_ranks,
                   datetime_base, rank_dir * width_dir,
                   file_name + ": ",
                   timestamp_prefix_len,
                   timestamp, entry, p)

    # Driver for visitor callbacks comes from logmerge.
    logmerge.main_with_args(args, visitor=plot_visitor)

    p.finish_image()

    print "len(dirs)", len(dirs)
    print "len(pattern_ranks)", len(pattern_ranks)
    print "timestamp_first", scan_info["timestamp_first"]
    print "timestamps_num_unique", scan_info["timestamps_num_unique"]
    print "p.im_num", p.im_num
    print "p.plot_num", p.plot_num
    print "image_files", image_files

    return image_files


def plot_init(paths_in, suffix, out_prefix, scan_info):
    file_patterns = scan_info["file_patterns"]
    pattern_ranks = scan_info["pattern_ranks"]
    timestamp_first = scan_info["timestamp_first"]
    timestamps_num_unique = scan_info["timestamps_num_unique"]

    # Sort the dir names, with any common prefix already stripped.
    paths, total_size, path_sizes = \
        logmerge.expand_paths(paths_in, suffix)

    dirs, dirs_sorted, path_prefix = sort_dirs(paths)

    # Initialize plotter.
    width_dir = len(pattern_ranks) + 1  # Width of a single dir.

    width = timestamp_gutter_width + \
        width_dir * len(dirs)  # First pixel is encoded seconds.

    height = 1 + timestamps_num_unique
    if height > max_image_height and max_image_height > 0:
        height = max_image_height

    height_text = 15

    datetime_base = parser.parse(timestamp_first, fuzzy=True)

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

            x = timestamp_gutter_width + x_base + (width_dir - 1)

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

    return dirs, path_prefix, width_dir, datetime_base, image_files, p


def plot_entry(patterns, pattern_ranks,
               datetime_base, x_base,
               pattern_ranks_key_prefix,
               timestamp_prefix_len,
               timestamp, entry, p):
    pattern = entry_to_pattern(entry)
    if not pattern:
        return

    pattern_key = str(pattern)

    pattern_info = patterns[pattern_key]

    if pattern_info["pattern_base"]:
        pattern_key = str(pattern_info["pattern_base"])

    rank = pattern_ranks.get(pattern_ranks_key_prefix + pattern_key)
    if rank is None:
        return

    x = x_base + rank

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
                        x-2, p.cur_y+3), fill="#933")


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

        self.plot_point(x, self.cur_y)

        self.cur_timestamp = timestamp

        return cur_timestamp_changed, cur_im_changed

    def plot_point(self, x, y):
        x = timestamp_gutter_width + x

        self.draw.point((x, y), fill=self.white)

        self.min_x = min(self.min_x, x)
        self.min_y = min(self.min_y, y)

        self.max_x = max(self.max_x, x)
        self.max_y = max(self.max_y, y)

        self.plot_num += 1


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


def git_describe_long():
    return subprocess.check_output(
        ['git', 'describe', '--long'],
        cwd=os.path.dirname(os.path.realpath(__file__))).strip()


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
