#!/usr/bin/env python
# -*- mode: Python;-*-

import argparse
import collections
import os
import re
import sys

import logmerge


# IDEAS:
# terms
# commonly seen sequences
# what about uuid's
# numbers are special (except when they're in a uuid?)
# longest common substring
# backtrack as it's actually not a common substring after all?
# e.g., date changes hours


# Need 32 hex chars for a uid pattern.
pattern_uid = "[a-f0-9]" * 32

# An example rev to initialize pattern_rev.
ex_rev = \
    "g2wAAAABaAJtAAAAIDJkZTgzNjhjZTNlMjQ0Y2Q" + \
    "3ZDE0MWE2OGI0ODE3ZDdjaAJhAW4FANj8ddQOag"

pattern_rev = "[a-zA-Z90-9]" * len(ex_rev)

# Some number-like patterns such as optionally dotted or dashed or
# slashed or colon'ed numbers.  Patterns like YYYY-MM-DD, HH:MM:SS and
# IP addresses would also be matched.
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


def main(argv):
    file_pos_term_counts, file_patterns = process(argv)

    print "len(file_pos_term_counts)", len(file_pos_term_counts)

    for file_name, pos_term_counts in file_pos_term_counts.iteritems():
        print "  ", file_name
        print "    len(pos_term_counts)", len(pos_term_counts)
        print "    sum(pos_term_counts.values)", sum(pos_term_counts.values())
        print "    most common", pos_term_counts.most_common(10)
        print "    least common", pos_term_counts.most_common()[:-10:-1]

    print "len(file_patterns)", len(file_patterns)

    num_pattern_infos = 0
    num_pattern_infos_total = 0
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

            num_pattern_infos_total += pattern_info.total

            if pattern_info.pattern_tuple_base:
                num_pattern_infos_base += 1
                pattern_tuple_bases[pattern_info.pattern_tuple_base] = True
            else:
                num_pattern_infos_base_none += 1

            print "      ", file_name, i, pattern_tuple, pattern_info.total

            if False:
                for recent in list(pattern_info.recents):
                    print "        ", recent

    print "num_pattern_infos", num_pattern_infos
    print "num_pattern_infos_total", num_pattern_infos_total
    print "num_pattern_infos_base", num_pattern_infos_base
    print "num_pattern_infos_base_none", num_pattern_infos_base_none
    print "len(pattern_tuple_bases)", len(pattern_tuple_bases)

    pattern_tuple_bases = pattern_tuple_bases.keys()
    pattern_tuple_bases.sort()

    print "pattern_tuple_bases..."
    for pattern_tuple_base in pattern_tuple_bases:
        print "  ", pattern_tuple_base


def process(argv):
    # Default to /dev/null for the --out argument.
    has_out_argument = False
    for arg in argv:
        if arg.startswith("--out="):
            has_out_argument = True
            break

    if not has_out_argument:
        argv.insert(1, "--out=/dev/null")

    # Custom argument parser.
    argument_parser = argparse.ArgumentParser(
        description="""%(prog)s provides log analysis
                       (extends logmerge.py feature set)""")

    # Custom visitor.
    visitor, file_pos_term_counts, file_patterns = prepare_visitor()

    # Main driver of visitor callbacks is reused from logmerge.
    logmerge.main(argv,
                  argument_parser=argument_parser,
                  visitor=visitor)

    # Find similar patterns that should share the same base.
    for file_name, patterns in file_patterns.iteritems():
        pattern_tuples = patterns.keys()
        pattern_tuples.sort()

        for i, pattern_tuple in enumerate(pattern_tuples):
            pattern_info = patterns[pattern_tuple]

            j = i - 1
            while j >= 0 and j > i - 10:
                prev_pattern_tuple = pattern_tuples[j]
                prev_pattern_info = patterns[prev_pattern_tuple]

                if mark_similar_pattern_infos(pattern_info,
                                              prev_pattern_info):
                    break

                j -= 1

    return file_pos_term_counts, file_patterns


def prepare_visitor():
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

            # Next, handle a num-ish section.
            if i < len(sections):
                num_ish_kind = None
                j = 0
                while j < len(pattern_num_ish):
                    if sections[i + 1 + j]:
                        num_ish_kind = j
                    j += 1

                pattern.append("#" + pattern_num_ish[num_ish_kind][0])

                i += 1 + len(pattern_num_ish)

        if pattern:
            pattern_tuple = tuple(pattern)

            pattern_info = patterns.get(pattern_tuple)
            if not pattern_info:
                pattern_info = PatternInfo(pattern_tuple, timestamp, entry)
                patterns[pattern_tuple] = pattern_info

                print path, pattern

            pattern_info.total += 1
            pattern_info.recents.append((timestamp, entry_first_line))

    return v, file_pos_term_counts, file_patterns


def parse_pos_term(pos_term):
    i = pos_term.find('>')

    return int(pos_term[0:i]), pos_term[i+1:]


default_pattern_info_max_recent = 100

class PatternInfo(object):
    def __init__(self, pattern_tuple, first_timestamp, first_entry):
        self.pattern_tuple_base = None
        self.pattern_tuple = pattern_tuple
        self.first_timestamp = first_timestamp
        self.first_entry = first_entry
        self.total = 0
        self.recents = collections.deque((), default_pattern_info_max_recent)


def mark_similar_pattern_infos(a, b):
    a_tuple = a.pattern_tuple
    b_tuple = b.pattern_tuple

    if len(a_tuple) != len(b_tuple):
        return False

    if b.pattern_tuple_base:
        b_tuple = b.pattern_tuple_base

    for i in range(len(a_tuple)):
        if a_tuple[i] == b_tuple[i]:
            continue

        # Return if the rest of the tuples are not the same.
        if a_tuple[i+1:] != b_tuple[i+1:]:
            return False

        # Pattern infos a & b only differ by a single entry, so
        # initialize their pattern_tuple_base with a '$' at the
        # differing entry.
        if not b.pattern_tuple_base:
            b_list = list(b_tuple)
            b_list[i] = "$"

            b.pattern_tuple_base = tuple(b_list)

        a.pattern_tuple_base = b.pattern_tuple_base

        return True


if __name__ == '__main__':
    sys.exit(main(sys.argv))
