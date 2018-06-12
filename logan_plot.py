#!/usr/bin/env python
# -*- mode: Python;-*-

import copy
import bisect
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

    if args.multiprocessing >= 0:
        plot_multiprocessing_scan_info(args, scan_info)
    else:
        plot_scan_info(args, scan_info)

    plot_info = dict(scan_info)  # Copy before modifying.

    plot_info["timestamp_gutter_width"] = timestamp_gutter_width

    # The file_patterns are too big / unused for plot_info so remove.
    del plot_info["file_patterns"]

    return plot_info


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

    return plot_multiprocessing_join(args, scan_info, results.get())


last_scan_info_file_name = None
last_scan_info = None

last_timestamps_file_name = None
last_timestmps = None


# Worker that plots a single chunk.
def plot_multiprocessing_worker(work):
    try:
        return plot_multiprocessing_worker_actual(work)
    except Exception as e:
        print "plot_multiprocessing_worker exception", e


def plot_multiprocessing_worker_profile(work):
    try:
        import cProfile
        import pstats
        import StringIO

        pr = cProfile.Profile()
        pr.enable()

        rv = plot_multiprocessing_worker_actual(work)

        pr.disable()
        s = StringIO.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats("tottime")
        ps.print_stats()
        print s.getvalue()

        return rv
    except Exception as e:
        print "plot_multiprocessing_worker exception", e


def plot_multiprocessing_worker_actual(work):
    chunk, args, q = work

    path, scan_start, scan_length = chunk

    file_name = os.path.basename(path)

    global last_scan_info_file_name
    global last_scan_info

    scan_info_file_name = args.out_prefix + "-scan.json"
    if scan_info_file_name == last_scan_info_file_name and last_scan_info:
        scan_info = last_scan_info
    else:
        with open(scan_info_file_name, 'r') as f:
            scan_info = byteify(json.load(f, object_hook=byteify),
                                ignore_dicts=True)

            last_scan_info_file_name = scan_info_file_name
            last_scan_info = scan_info

    global last_timestamps_file_name
    global last_timestamps

    timestamps_file_name = scan_info["timestamps_file_name"]
    if timestamps_file_name == last_timestamps_file_name and last_timestamps:
        timestamps = last_timestamps
    else:
        with open(timestamps_file_name, 'r') as f:
            timestamps = f.readlines()

            last_timestamps_file_name = timestamps_file_name
            last_timestamps = timestamps

    patterns = scan_info["file_patterns"].get(file_name)

    pattern_ranks = scan_info["pattern_ranks"]

    pattern_ranks_key_prefix = file_name + ": "

    chunk_out_prefix = args.out_prefix + "-chunk-" + \
        path.replace("/", "_").replace("-", "_") + "-" + \
        str(scan_start) + "-" + str(scan_length)

    bar = QueueBar(chunk, q)

    image_infos = []

    dirs, path_prefix, width_dir, datetime_base, image_infos, p = \
        plot_init(args.path, args.suffix, chunk_out_prefix, scan_info,
                  crop_on_finish=True)

    rank_dir = dirs.get(os.path.dirname(path[len(path_prefix):]))

    x_base = rank_dir * width_dir

    class VState:
        def __init__(self):
            self.i = 0  # Total entries seen so far.
            self.n = 0  # Total entry_size's seen so far.

            self.last_timestamp = None
            self.last_y = None

    v_state = VState()

    def v(path_ignored, timestamp, entry, entry_size):
        if (not timestamp) or (not entry):
            return

        rank = entry_pattern_rank(
            patterns, pattern_ranks, pattern_ranks_key_prefix, entry)

        x = x_base + rank

        timestamp = timestamp[:timestamp_prefix_len]

        if timestamp == v_state.last_timestamp:
            y = v_state.last_y
        else:
            # TODO: This is inefficient, as timestamps are ordered,
            # and callback timestamp arg is always increasing.
            y = bisect.bisect_left(timestamps, timestamp) + 1

        p.plot_point(x, y)

        v_state.last_timestamp = timestamp
        v_state.last_y = y

        v_state.i += 1
        v_state.n += entry_size

        if bar and v_state.i % 2000 == 0:
            bar.update(v_state.n)

    # Driver for visitor callbacks comes from logmerge.
    args = copy.copy(args)
    args.path = [path]
    args.scan_start = scan_start
    args.scan_length = scan_length

    logmerge.main_with_args(args, visitor=v, bar=bar)

    p.finish_image()

    q.put("done", False)

    return {
        "path": path,
        "chunk": chunk,
        "chunk_out_prefix": chunk_out_prefix,
        "image_infos": image_infos,
    }


def plot_multiprocessing_join(args, scan_info, results):
    with open(scan_info["timestamps_file_name"], 'r') as f:
        timestamps = f.readlines()

    dirs, path_prefix, width_dir, datetime_base, image_infos, p = \
        plot_init(args.path, args.suffix, args.out_prefix, scan_info)

    for i, timestamp in enumerate(timestamps):
        plot_timestamp(p, datetime_base, timestamp, i + 1)

    results.sort()

    for result in results:
        if not result:
            continue

        for image_info in result["image_infos"]:
            image_file_name, bounds = image_info
            if not image_file_name:
                continue

            min_x, min_y, max_x, max_y = bounds
            if min_x <= max_x and min_y <= max_y:
                chunk_image = Image.open(image_file_name)

                p.im.paste(chunk_image, (min_x, min_y))

                chunk_image.close()

            os.remove(image_file_name)

    p.finish_image()

    print image_infos


# Single-threaded plot of the scan_info.
def plot_scan_info(args, scan_info):
    dirs, path_prefix, width_dir, datetime_base, image_infos, p = \
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

        plot_entry(patterns, pattern_ranks, file_name + ": ",
                   datetime_base, rank_dir * width_dir, timestamp, entry, p)

    # Driver for visitor callbacks comes from logmerge.
    logmerge.main_with_args(args, visitor=plot_visitor)

    p.finish_image()

    print "len(dirs)", len(dirs)
    print "len(pattern_ranks)", len(pattern_ranks)
    print "timestamp_first", scan_info["timestamp_first"]
    print "timestamps_num_unique", scan_info["timestamps_num_unique"]
    print "p.im_num", p.im_num
    print "p.plot_num", p.plot_num
    print "image_infos", image_infos

    return image_infos


def plot_init(paths_in, suffix, out_prefix, scan_info, crop_on_finish=False):
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

    timestamp_first_nearest_minute = \
        timestamp_first[:len("2010-01-01T00:00:")] + "00"

    datetime_base = parser.parse(timestamp_first_nearest_minute, fuzzy=True)

    datetime_2010 = parser.parse("2010-01-01 00:00:00")

    start_minutes_since_2010 = \
        int((datetime_base - datetime_2010).total_seconds() / 60.0)

    image_infos = []

    def on_start_image(p):
        # Encode the start_minutes_since_2010 at line 0's timestamp gutter.
        p.draw.line((0, 0, timestamp_gutter_width - 1, 0),
                    fill=to_rgb(start_minutes_since_2010))
        p.cur_y = 0

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

    def on_finish_image(p):
        image_infos.append((p.im_name, (p.min_x, p.min_y, p.max_x, p.max_y)))

    p = Plotter(out_prefix, width, height,
                on_start_image, on_finish_image,
                crop_on_finish=crop_on_finish)

    p.start_image()

    return dirs, path_prefix, width_dir, datetime_base, image_infos, p


def plot_entry(patterns, pattern_ranks, pattern_ranks_key_prefix,
               datetime_base, x_base, timestamp, entry, p):
    rank = entry_pattern_rank(
        patterns, pattern_ranks, pattern_ranks_key_prefix, entry)

    x = x_base + rank

    timestamp = timestamp[:timestamp_prefix_len]

    timestamp_changed, im_changed = p.plot(timestamp, x)

    if timestamp_changed:
        plot_timestamp(p, datetime_base, timestamp, p.cur_y)

    if (not im_changed) and (re_erro.search(entry[0]) is not None):
        # Mark ERRO with a red triangle.
        p.draw.polygon((x, p.cur_y,
                        x+2, p.cur_y+3,
                        x-2, p.cur_y+3), fill="#933")


def entry_pattern_rank(patterns, pattern_ranks, pattern_ranks_key_prefix,
                       entry):
    pattern = entry_to_pattern(entry)
    if not pattern:
        return

    pattern_key = str(pattern)

    pattern_info = patterns[pattern_key]

    if pattern_info["pattern_base"]:
        pattern_key = str(pattern_info["pattern_base"])

    return pattern_ranks.get(pattern_ranks_key_prefix + pattern_key)


def plot_timestamp(p, datetime_base, timestamp, y):
    datetime_cur = parser.parse(timestamp, fuzzy=True)

    delta_seconds = int((datetime_cur - datetime_base).total_seconds())

    p.draw.line((0, y, timestamp_gutter_width - 1, y),
                fill=to_rgb(delta_seconds))


class Plotter(object):
    def __init__(self, prefix, width, height,
                 on_start_image, on_finish_image, crop_on_finish):
        self.prefix = prefix

        self.width = width
        self.height = height

        self.on_start_image = on_start_image
        self.on_finish_image = on_finish_image

        self.crop_on_finish = crop_on_finish

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
        if self.min_x <= self.max_x and \
           self.min_y <= self.max_y:
            if self.crop_on_finish:
                c = self.im.crop((self.min_x, self.min_y,
                                  self.max_x + 1, self.max_y + 1))
                c.save(self.im_name)
                c.close()
            else:
                self.im.save(self.im_name)

            if self.on_finish_image:
                self.on_finish_image(self)

        self.im.close()
        self.im = None
        self.im_num += 1
        self.im_name = None

        self.draw = None

        self.cur_y = None

    # Plot a point at (x, cur_y), advancing cur_y if the timestamp changed.
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

        self.draw.point((x, y), fill="white")

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
