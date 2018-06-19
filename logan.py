#!/usr/bin/env python
# -*- mode: Python;-*-

import json
import os
import signal
import sys

from logan_args import new_argument_parser, prep_args
from logan_http import http_server
from logan_plot import plot, init_plot_info
from logan_scan import scan
from logan_util import byteify


def main(argv):
    args = new_argument_parser().parse_args(argv[1:])

    args = prep_args(args)

    if args.multiprocessing >= 0:
        signal.signal(signal.SIGINT, on_sigint)

    main_steps(argv, args)


def main_steps(argv, args, scan_info=None):
    if (args.steps is None) and args.http:
        args.steps = "http"

    if (args.steps is None):
        args.steps = "scan,save,plot"

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
        plot_info = init_plot_info(argv, args, scan_info)

        plot_info_file = args.out_prefix + ".json"
        with open(plot_info_file, 'w') as f:
            f.write(json.dumps(plot_info))

        print "\nwrote", plot_info_file

        print "\nplotting images..."
        plot(argv, args, scan_info)

    if "http" in steps:
        print "\n============================================"
        http_server(argv, args)


def on_sigint(signum, frame):
    print("SIGINT received")
    os._exit(1)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
