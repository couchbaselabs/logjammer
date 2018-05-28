#!/usr/bin/env python
# -*- mode: Python;-*-

import argparse
import collections
import os
import re
import sys

import logmerge


def main(argv):
    set_argv_default(argv, "out", "/dev/null")

    # Custom argument parser.
    argument_parser = argparse.ArgumentParser(
        description="""%(prog)s provides log analysis
                       (extends logmerge.py feature set)""")

    # Scan the logs to build up pattern info's.
    file_pos_term_counts, file_patterns = \
        scan_patterns(argv, argument_parser)

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

    pattern_tuple_bases = {}

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
                num_pattern_infos_base += 1

                k = (file_name, pattern_info.pattern_tuple_base)
                pattern_tuple_bases[k] = pattern_tuple_bases.get(k, 0) + 1
            else:
                num_pattern_infos_base_none += 1

            print "      ", file_name, i, pattern_tuple, pattern_info.total

    print "\n============================================"

    print "num_entries", num_entries

    print "num_pattern_infos", num_pattern_infos
    print "num_pattern_infos_base", num_pattern_infos_base
    print "num_pattern_infos_base_none", num_pattern_infos_base_none
    print "len(pattern_tuple_bases)", len(pattern_tuple_bases)

    print "\n============================================"

    pattern_tuple_base_keys = pattern_tuple_bases.keys()
    pattern_tuple_base_keys.sort()

    print "pattern_tuple_bases..."
    for k in pattern_tuple_base_keys:
        print "  ", pattern_tuple_bases[k], "-", k

    print "\n============================================"

    scan_to_plot(argv, num_pattern_infos, num_entries, file_patterns)


# Modify argv with a default for the --name=val argument.
def set_argv_default(argv, name, val):
    prefix = "--" + name + "="

    for arg in argv:
        if arg.startswith(prefix):
            return

    argv.insert(1, prefix + val)


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
    ("idn", r"[a-zA-Z][a-zA-Z\-_]+\d+"),  # A numbered identifier, like "vb_8".
    ("neg", r"-\d[\d\.]*"),               # A negative dotted number.
    ("pos", r"\d[\d\.]*")]                # A positive dotted number.

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

        patterns = file_patterns.get(file_name)
        if patterns is None:
            patterns = {}
            file_patterns[file_name] = patterns

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

                pos_term_counts.update([pos_term])

                pattern.append(pos_term)

            # Next, handle a num-ish section, where re.split()
            # produces as many items as there were capture groups.
            if i < len(sections):
                num_ish_kind = None
                j = 0
                while j < len(pattern_num_ish):
                    if sections[i + 1 + j]:
                        num_ish_kind = j
                    j += 1

                pattern.append("#" + pattern_num_ish[num_ish_kind][0])

                i += 1 + len(pattern_num_ish)

        # Register into patterns dict if it's a brand new pattern.
        if pattern:
            pattern_tuple = tuple(pattern)

            pattern_info = patterns.get(pattern_tuple)
            if not pattern_info:
                pattern_info = PatternInfo(pattern_tuple, timestamp, entry)
                patterns[pattern_tuple] = pattern_info

            # Increment the total count of instances of this pattern.
            pattern_info.total += 1

            # Remember recent instances of this pattern.
            pattern_info.recents.append((timestamp, entry_first_line))

    return v, file_pos_term_counts, file_patterns


class PatternInfo(object):
    # Max number of recently seen entries to remember.
    pattern_info_recent_max = 100

    def __init__(self, pattern_tuple, first_timestamp, first_entry):
        self.pattern_tuple_base = None
        self.pattern_tuple = pattern_tuple
        self.first_timestamp = first_timestamp
        self.first_entry = first_entry
        self.total = 0
        self.recents = collections.deque((), self.pattern_info_recent_max)


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
def scan_to_plot(argv, num_pattern_infos, num_entries,
                 file_patterns):
    from PIL import Image, ImageDraw

    w = num_pattern_infos
    h = num_entries
    if h > 2000:
        h = 2000

    im = Image.new("1", (w, h))

    white = 1

    draw = ImageDraw.Draw(im)
    draw.line((0, 0) + im.size, fill=white)
    draw.line((0, im.size[1], im.size[0], 0), fill=white)
    del draw

    im.save("out.png")


if __name__ == '__main__':
    sys.exit(main(sys.argv))
