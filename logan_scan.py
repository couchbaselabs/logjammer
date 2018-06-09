#!/usr/bin/env python
# -*- mode: Python;-*-

import multiprocessing
import os
import re
import subprocess

# See progressbar2 https://github.com/WoLpH/python-progressbar
# Ex: pip install progressbar2
import progressbar

import logmerge


timestamp_gutter_width = 1  # In pixels.


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
