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

    for file_name, patterns in file_patterns.iteritems():
        print "  ", file_name
        print "    len(patterns)", len(patterns)


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

    return file_pos_term_counts, file_patterns


# Need 32 hex chars for a uid pattern.
pattern_uid = "[a-f0-9]" * 32

# An example rev to initialize pattern_rev.
ex_rev = \
    "g2wAAAABaAJtAAAAIDJkZTgzNjhjZTNlMjQ0Y2Q" + \
    "3ZDE0MWE2OGI0ODE3ZDdjaAJhAW4FANj8ddQOag"

pattern_rev = "[a-zA-Z90-9]" * len(ex_rev)

# A number-like pattern that's an optionally dotted or dashed or
# slashed or colon'ed number, or a UID or a rev.  Patterns like
# YYYY-MM-DD, HH:MM:SS and IP addresses would also be matched.
pattern_num_ish = [
    ("hex", r"0x[a-f0-9][a-f0-9]+"),
    ("hex", r"0x[A-F0-9][A-F0-9]+"),
    ("num", r"[\d\-][\d\.\-\:/,]*"),
    ("uid", pattern_uid),
    ("rev", pattern_rev)]

pattern_num_ish_joined = "(" + \
                         "|".join(["(" + p[1] + ")"
                                   for p in pattern_num_ish]) + \
                         ")"

re_num_ish = re.compile(pattern_num_ish_joined)

re_section_split = re.compile(r"[^a-zA-z0-9_\-/]+")


def prepare_visitor():
    # Keyed by file name, value is collections.Counter.
    file_pos_term_counts = {}

    # Keyed by file name, value is dict of patterns.
    file_patterns = {}

    def v(path, timestamp, entry, entry_size):
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
                num_ish = sections[i]

                i += 1 + len(pattern_num_ish)

                pattern.append("*")

        if pattern:
            pattern_tuple = tuple(pattern)
            if not patterns.get(pattern_tuple):
                patterns[tuple(pattern)] = True
                print file_name, pattern

    return v, file_pos_term_counts, file_patterns


def parse_pos_term(pos_term):
    i = pos_term.find('>')

    return int(pos_term[0:i]), pos_term[i+1:]


if __name__ == '__main__':
    sys.exit(main(sys.argv))
