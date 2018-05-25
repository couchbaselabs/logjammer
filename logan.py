#!/usr/bin/env python
# -*- mode: Python;-*-

import argparse
import sys

import logmerge


def main(argv):
    logmerge.main(argv, ap=argparse.ArgumentParser(
        description='%(prog)s provides log analysis (based on logmerge)'))


if __name__ == '__main__':
    sys.exit(main(sys.argv))
