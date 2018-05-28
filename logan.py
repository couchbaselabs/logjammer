#!/usr/bin/env python
# -*- mode: Python;-*-

import argparse
import collections
import os
import re
import sys

from PIL import Image, ImageDraw

import logmerge


def main(argv):
    set_argv_default(argv, "out", "/dev/null")

    # Scan the logs to build the pattern info's.
    file_pos_term_counts, file_patterns = \
        scan_patterns(argv, init_argument_parser())

    # Process the pattern info's to find similar pattern info's.
    mark_similar_pattern_infos(file_patterns)

    print "\n============================================"

    print "len(file_pos_term_counts)", len(file_pos_term_counts)

    for file_name, pos_term_counts in file_pos_term_counts.iteritems():
        print "  ", file_name
        print "    len(pos_term_counts)", len(pos_term_counts)
        print "    sum(pos_term_counts.values)", sum(pos_term_counts.values())
        print "    most common", pos_term_counts.most_common(10)
        print "    least common", pos_term_counts.most_common()[:-10:-1]
        print "    ------------------"

    print "\n============================================"

    print "len(file_patterns)", len(file_patterns)

    num_entries = 0

    num_pattern_infos = 0
    num_pattern_infos_base = 0
    num_pattern_infos_base_none = 0

    # The unique (file_name, pattern_tuple)'s when shared
    # pattern_tuple_base's are also considered.  The value is the
    # total number of entries seen.
    pattern_tuple_uniques = {}

    for file_name, patterns in file_patterns.iteritems():
        num_pattern_infos += len(patterns)

        print "  ", file_name
        print "    len(patterns)", len(patterns)

        pattern_tuples = patterns.keys()
        pattern_tuples.sort()

        for i, pattern_tuple in enumerate(pattern_tuples):
            pattern_info = patterns[pattern_tuple]

            num_entries += pattern_info.total

            if pattern_info.pattern_tuple_base:
                pattern_tuple = pattern_info.pattern_tuple_base

                num_pattern_infos_base += 1
            else:
                num_pattern_infos_base_none += 1

            k = (file_name, pattern_tuple)

            pattern_tuple_uniques[k] = \
                pattern_tuple_uniques.get(k, 0) + \
                pattern_info.total

            print "      ", file_name, i, pattern_tuple, pattern_info.total

    print "\n============================================"

    print "num_entries", num_entries

    print "num_pattern_infos", num_pattern_infos
    print "num_pattern_infos_base", num_pattern_infos_base
    print "num_pattern_infos_base_none", num_pattern_infos_base_none

    print "len(pattern_tuple_uniques)", \
        len(pattern_tuple_uniques)

    print "\n============================================"

    pattern_tuple_ranks = {}

    pattern_tuple_unique_keys = pattern_tuple_uniques.keys()
    pattern_tuple_unique_keys.sort()

    print "pattern_tuple_uniques..."

    for i, k in enumerate(pattern_tuple_unique_keys):
        print "  ", pattern_tuple_uniques[k], "-", k

        pattern_tuple_ranks[k] = i  # TODO - for now.

    print "\n============================================"

    print "len(pattern_tuple_ranks)", len(pattern_tuple_ranks)

    scan_to_plot(argv, init_argument_parser(),
                 file_patterns, pattern_tuple_ranks, num_entries)


# Modify argv with a default for the --name=val argument.
def set_argv_default(argv, name, val):
    prefix = "--" + name + "="

    for arg in argv:
        if arg.startswith(prefix):
            return

    argv.insert(1, prefix + val)


def init_argument_parser():
    return argparse.ArgumentParser(
        description="""%(prog)s provides log analysis
                       (extends logmerge.py feature set)""")


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


# Scan the log files to build up pattern info's.
def scan_patterns(argv, argument_parser):
    # Custom visitor.
    visitor, file_pos_term_counts, file_patterns = \
        scan_patterns_visitor()

    # Main driver of visitor callbacks is reused from logmerge.
    logmerge.main(argv,
                  argument_parser=argument_parser,
                  visitor=visitor)

    return file_pos_term_counts, file_patterns


def scan_patterns_visitor():
    # Keyed by file name, value is collections.Counter.
    file_pos_term_counts = {}

    # Keyed by file name, value is dict of pattern => PatternInfo.
    file_patterns = {}

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

    return v, file_pos_term_counts, file_patterns


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
def scan_to_plot(argv, argument_parser,
                 file_patterns, pattern_tuple_ranks, num_entries):
    timestamp_prefix_len = len("YYYY-MM-DDTHH:MM:SS")

    height = num_entries
    if height > 2000:
        height = 2000

    p = Plotter(len(pattern_tuple_ranks), height)

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

        rank = pattern_tuple_ranks[(file_name, pattern_tuple)]

        p.plot(timestamp[:timestamp_prefix_len], rank)

    logmerge.main(argv,
                  argument_parser=argument_parser,
                  visitor=plot_visitor)

    p.finish_image()

    print "len(pattern_tuple_ranks)", len(pattern_tuple_ranks)
    print "num_entries", num_entries
    print "p.im_num", p.im_num
    print "p.plot_num", p.plot_num


class Plotter(object):
    white = 1

    def __init__(self, width, height):
        self.width = width
        self.height = height

        self.im = None
        self.im_num = 0
        self.draw = None
        self.cur_y = 0
        self.cur_timestamp = None
        self.plot_num = 0

    def start_image(self):
        self.im = Image.new("1", (self.width, self.height))
        self.draw = ImageDraw.Draw(self.im)
        self.cur_y = 0
        self.cur_timestamp = None

    def finish_image(self):
        self.im.save("out-" + "{0:0>3}".format(self.im_num) + ".png")
        self.im.close()
        self.im_num += 1
        self.draw = None
        self.cur_y = None

    def plot(self, timestamp, x):
        if self.cur_timestamp != timestamp:
            self.cur_y += 1

        if self.cur_y > self.height:
            self.finish_image()
            self.start_image()

        self.draw.point((x, self.cur_y), fill=self.white)

        self.cur_timestamp = timestamp

        self.plot_num += 1


if __name__ == '__main__':
    sys.exit(main(sys.argv))
