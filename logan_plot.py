#!/usr/bin/env python
# -*- mode: Python;-*-

import json
import multiprocessing
import os

from dateutil import parser

from PIL import Image, ImageDraw

import logmerge

from logan_scan import entry_to_pattern, re_erro, timestamp_prefix_len

from logan_util import byteify, chunkify_path_sizes, \
    multiprocessing_wait, QueueBar


timestamp_gutter_width = 1  # In pixels.

max_image_height = 0  # 0 means unlimited plot image height.


# Scan the log entries, plotting them based on the given scan info.
def plot(argv, args, scan_info):
    file_patterns = scan_info["file_patterns"]
    pattern_ranks = scan_info["pattern_ranks"]
    timestamp_first = scan_info["timestamp_first"]
    timestamps_num_unique = scan_info["timestamps_num_unique"]

    if not (file_patterns and pattern_ranks and
            timestamp_first and timestamps_num_unique):
        return

    if False and args.multiprocessing >= 0:
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
