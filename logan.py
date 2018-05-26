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
    argument_parser = argparse.ArgumentParser(
        description="""%(prog)s provides log analysis
                       (extends logmerge.py feature set)""")

    visitor, file_ids, term_counts, g = prepare_visitor()

    logmerge.main(argv,
                  argument_parser=argument_parser,
                  visitor=visitor)

    print term_counts

    print term_counts.most_common(10)

    print "len(term_counts)", len(term_counts)

    print "sum(term_counts.values())", sum(term_counts.values())

    print "g.number_of_nodes()", g.number_of_nodes()

    print "g.number_of_edges()", g.number_of_edges()


# Need 32 hex chars for a uuid.
pat_uuid = "[a-f0-9]" * 32

# Example rev:
ex_rev = \
    "g2wAAAABaAJtAAAAIDJkZTgzNjhjZTNlMjQ0Y2Q" + \
    "3ZDE0MWE2OGI0ODE3ZDdjaAJhAW4FANj8ddQOag"

pat_rev = "[a-zA-Z90-9]" * len(ex_rev)

# A number-like pattern that's an optionally dotted or dashed or
# slashed or colon'ed number, or a UUID or a rev.  Patterns like
# YYYY-MM-DD, HH:MM:SS and IP addresses would also be matched.
pat_num_ish = \
    r"((\-?\d([\d\.\-\:/,]*\d))" + \
    "|(" + pat_uuid + ")" + \
    "|(" + pat_rev + "))"

pat_num_ish_groups = len(re.findall("\(", pat_num_ish))

re_num_ish = re.compile(pat_num_ish)

re_words_split = re.compile(r"[^a-zA-z0-9_\-/]+")


# terms
# commonly seen sequences
# what about uuid's
# numbers are special (except when they're in a uuid?)
# longest common substring
# backtrack as it's actually not a common substring after all?
# e.g., date changes hours


def prepare_visitor():
    file_ids = {}

    term_counts = collections.Counter()

    g = nx.DiGraph()

    def v(path, timestamp, entry, entry_size):
        file_name = os.path.basename(path)

        file_id = file_ids.get(file_name)
        if file_id is None:
            file_id = len(file_ids)
            file_ids[file_name] = file_id

        file_id_str = str(file_id)

        first_line = entry[0].strip()

        parts = re.split(re_num_ish, first_line)

        a = []

        prev_term = None

        i = 0
        while i < len(parts):
            words = parts[i]
            i += 1

            for term in re.split(re_words_split, words):
                if term:
                    # Prefix file_id and term position onto term.
                    term = file_id_str + ":" + str(len(a)) + ">" + term

                    term_counts.update([term])
                    a.append(term)

                    if prev_term:
                        g.add_edge(prev_term, term)

                    prev_term = term

            if i < len(parts):
                a.append(parts[i])
                i += pat_num_ish_groups

        if g.number_of_nodes() < 1000:  # Emit some early sample lines.
            print a

    return v, file_ids, term_counts, g


if __name__ == '__main__':
    sys.exit(main(sys.argv))
