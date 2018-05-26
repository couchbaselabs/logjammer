#!/usr/bin/env python
# -*- mode: Python;-*-

import argparse
import collections
import re
import sys

import logmerge
import networkx as nx


def main(argv):
    argument_parser = argparse.ArgumentParser(
        description="""%(prog)s provides log analysis
                       (extends logmerge.py feature set)""")

    visitor, term_counts = prepare_visitor()

    logmerge.main(argv,
                  argument_parser=argument_parser,
                  visitor=visitor)

    print term_counts

    print term_counts.most_common(10)

    print "len(term_counts)", len(term_counts)

    print "sum(term_counts.values())", sum(term_counts.values())


# Need 32 hex chars for a uuid.
pat_uuid = "[a-f0-9]" * 32

# Example rev:
ex_rev = "g2wAAAABaAJtAAAAIDJkZTgzNjhjZTNlMjQ0Y2Q3ZDE0MWE2OGI0ODE3ZDdjaAJhAW4FANj8ddQOag"

pat_rev = "[a-zA-Z90-9]" * len(ex_rev)

# A number-like pattern that's optionally dotted or dashed or slashed
# or coloned, or a UUID.  Patterns like YYYY-MM-DD, HH:MM:SS & IP
# addresses would also be matched.
pat_num_ish = r"((\-?\d([\d\.\-\:/,]*\d))|(" + pat_uuid + ")|(" + pat_rev + "))"

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
    G = nx.Graph()

    term_counts = collections.Counter()

    def v(path, timestamp, entry, entry_size):
        textHead = entry[0].strip()

        parts = re.split(re_num_ish, textHead)

        a = []

        i = 0
        while i < len(parts):
            words = parts[i]
            i += 1

            for word in re.split(re_words_split, words):
                if word:
                    term_counts.update([word])
                    a.append(word)

            if i < len(parts):
                a.append("{" + parts[i] + "}")

                i += pat_num_ish_groups

        print "==>", a

    return v, term_counts


if __name__ == '__main__':
    sys.exit(main(sys.argv))
