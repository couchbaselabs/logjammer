#!/usr/bin/env python
# -*- mode: Python;-*-

import json
import os
import signal
import sys

from logan_args import new_argument_parser
from logan_http import http_server
from logan_plot import plot
from logan_scan import scan, byteify


def main(argv):
    args = new_argument_parser().parse_args(argv[1:])

    if (args.steps is None) and args.http:
        args.steps = "http"

    if (args.steps is None):
        args.steps = "scan,save,plot"

    # Since logan invokes logmerge, set any args needed by logmerge.

    args.out = os.devnull
    args.fields = None
    args.max_entries = None
    args.max_lines_per_entry = 0.5  # Only need first lines for logan.
    args.scan_start = None
    args.scan_length = None
    args.single_line = None
    args.timestamp_prefix = None
    args.wrap = None
    args.wrap_indent = None

    main_steps(argv, args)


def main_steps(argv, args, scan_info=None):
    if args.multiprocessing >= 0:
        signal.signal(signal.SIGINT, on_sigint)

    steps = args.steps.split(",")

    if "load" in steps:
        print "\n============================================"
        scan_file = args.out_prefix + "-scan.json"
        print "loading scan info file:", scan_file
        with open(scan_file, 'r') as f:
            scan_info = byteify(json.load(f, object_hook=byteify),
                                ignore_dicts=True)

    if "scan" in steps:
        print "\n============================================"
        print "scanning..."
        scan_info = scan(argv, args)

    if "save" in steps:
        print "\n============================================"
        scan_file = args.out_prefix + "-scan.json"
        print "saving scan info file:", scan_file
        with open(scan_file, 'w') as f:
            json.dump(scan_info, f, separators=(',', ':'))

        print "\nwrote", scan_file

    if "plot" in steps:
        print "\n============================================"
        print "plotting..."
        plot(argv, args, scan_info)

        plot_info = dict(scan_info)  # Copy before modifying.
        del plot_info["file_patterns"]  # Too big / unused for plot_info.

        plot_file = args.out_prefix + ".json"
        with open(plot_file, 'w') as f:
            f.write(json.dumps(plot_info))

        print "\nwrote", plot_file

    if "http" in steps:
        print "\n============================================"
        http_server(argv, args)


def on_sigint(signum, frame):
    print("SIGINT received")
    os._exit(1)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
