#!/usr/bin/env python
# -*- mode: Python;-*-

import argparse
import sys

import logmerge


def main(argv):
    argument_parser = argparse.ArgumentParser(
        description="""%(prog)s provides log analysis
                       (extends logmerge.py feature set)""")

    visitor = prepare_visitor()

    logmerge.main(argv,
                  argument_parser=argument_parser,
                  visitor=visitor)


def prepare_visitor():
    def v(path, timestamp, entry, entry_size):
        pass  # TODO: no-op for now.

    return v


if __name__ == '__main__':
    sys.exit(main(sys.argv))
