#!/usr/bin/env python
# -*- mode: Python;-*-

import argparse
import collections
import os
import re
import sys

import logmerge

import networkx as nx


def main(argv):
    file_ids, pos_term_counts, g = process(argv)

    print pos_term_counts

    print pos_term_counts.most_common(10)

    print "len(pos_term_counts)", len(pos_term_counts)

    print "sum(pos_term_counts.values())", sum(pos_term_counts.values())

    print "g.number_of_nodes()", g.number_of_nodes()

    print "g.number_of_edges()", g.number_of_edges()

    print "pos_terms with no predecessors"
    for pos_term in g.nodes:
        if len(g.pred[pos_term]) <= 0:
            print "  ", pos_term


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
    visitor, file_ids, pos_term_counts, g = prepare_visitor()

    # Main driver comes from logmerge.
    logmerge.main(argv,
                  argument_parser=argument_parser,
                  visitor=visitor)

    return file_ids, pos_term_counts, g


# Need 32 hex chars for a uuid pattern.
pattern_uuid = "[a-f0-9]" * 32

# An example rev to initialize pattern_rev.
ex_rev = \
    "g2wAAAABaAJtAAAAIDJkZTgzNjhjZTNlMjQ0Y2Q" + \
    "3ZDE0MWE2OGI0ODE3ZDdjaAJhAW4FANj8ddQOag"

pattern_rev = "[a-zA-Z90-9]" * len(ex_rev)

# A number-like pattern that's an optionally dotted or dashed or
# slashed or colon'ed number, or a UUID or a rev.  Patterns like
# YYYY-MM-DD, HH:MM:SS and IP addresses would also be matched.
pattern_num_ish = \
    r"((\-?\d([\d\.\-\:/,]*\d))" + \
    "|(" + pattern_uuid + ")" + \
    "|(" + pattern_rev + "))"

# Number of match groups in the pattern_num_ish.
pattern_num_ish_groups = len(re.findall("\(", pattern_num_ish))

re_num_ish = re.compile(pattern_num_ish)

re_section_split = re.compile(r"[^a-zA-z0-9_\-/]+")


# terms
# commonly seen sequences
# what about uuid's
# numbers are special (except when they're in a uuid?)
# longest common substring
# backtrack as it's actually not a common substring after all?
# e.g., date changes hours


def prepare_visitor():
    file_ids = {}

    pos_term_counts = collections.Counter()

    g = nx.DiGraph()

    def v(path, timestamp, entry, entry_size):
        file_name = os.path.basename(path)

        file_id = file_ids.get(file_name)
        if file_id is None:
            file_id = str(len(file_ids))
            file_ids[file_name] = file_id

        pos_term_prev = None

        pos_terms_or_vals = []

        entry_first_line = entry[0].strip()

        sections = re.split(re_num_ish, entry_first_line)

        i = 0
        while i < len(sections):
            section = sections[i]
            i += 1

            for term in re.split(re_section_split, section):
                if not term:
                    continue

                # A "positioned term" encodes a term with its source
                # file_id and term position.
                pos_term = \
                    file_id + ":" + \
                    str(len(pos_terms_or_vals)) + ">" + \
                    term

                pos_terms_or_vals.append(pos_term)

                pos_term_counts.update([pos_term])

                if pos_term_prev:
                    g.add_edge(pos_term_prev, pos_term)

                pos_term_prev = pos_term

            if i < len(sections):
                num_ish = sections[i]
                i += pattern_num_ish_groups

                pos_terms_or_vals.append(num_ish)

        if g.number_of_nodes() < 1000:  # Emit some early sample lines.
            print pos_terms_or_vals

    return v, file_ids, pos_term_counts, g


if __name__ == '__main__':
    sys.exit(main(sys.argv))
