#!/usr/bin/env python
# -*- mode: Python;-*-

import argparse
import os

import logmerge


# These are args known by logan but not by logmerge.
arg_names = ["chunk-size", "http", "max-image-height",
             "multiprocessing", "out-prefix", "repo", "steps"]


def new_argument_parser():
    ap = argparse.ArgumentParser(
        description="""%(prog)s provides log analysis
                       (extends logmerge.py feature set)""")

    ap.add_argument('--chunk-size', type=int, default=2,
                    help="""split large log files into smaller chunks
                    (in MB) when multiprocessing; use 0 for no chunking
                    (default: %(default)s MB)""")

    ap.add_argument('--http', type=str,
                    help="""when specified, this option overrides
                    the default processing steps to be 'http'
                    in order to allow the analysis / plot to be
                    interactively browsed;
                    the HTTP is the port number to listen on""")

    ap.add_argument('--max-image-height', type=int, default=1200,
                    help="""max height in pixels of output image plots,
                    where multiple output plot files will be generated if
                    needed; 0 means unconstrained output image height
                    (default: %(default)s)""")

    ap.add_argument('--multiprocessing', type=int, default=0,
                    help="""number of processes to use for multiprocessing;
                    use 0 for default cpu count,
                    use -1 to disable multiprocessing
                    (default: %(default)s)""")

    ap.add_argument('--out-prefix', type=str, default="out-logan",
                    help="""when the processing steps include
                    'load', 'save' or 'plot', the persisted files
                    will have this prefix, like $(out-prefix)-000.png,
                    $(out-prefix).json and $(out-prefix)-scan.json
                    (default: %(default)s)""")

    ap.add_argument('--repo', type=str,
                    help="""optional directory to source code repo""")

    ap.add_argument('--steps', type=str,
                    help="""processing steps are a comma separated list,
                    where valid steps are: load, scan, save, plot, http;
                    (default: scan,save,plot)""")

    ap.add_argument('--verbose', '-v', action='count',
                    help="""provides verbose output, level of verbosity determined
                    by the count of option i.e. -v, -vv or -vvv""")

    # Subset of arguments shared with logmerge.

    logmerge.add_path_arguments(ap)
    logmerge.add_match_arguments(ap)
    logmerge.add_time_range_arguments(ap)

    return ap


def prep_args(args):
    # Provide defaults for args needed by invocations of logmerge.

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

    return args
