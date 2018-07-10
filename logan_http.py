#!/usr/bin/env python
# -*- mode: Python;-*-

import __builtin__
import keyword
import os
import re
import subprocess

import SimpleHTTPServer
import SocketServer
import urlparse

from logan_args import arg_names


def http_server(argv, args):
    clean_argv = []
    for arg in argv:
        found = [s for s in arg_names if arg.startswith("--" + s)]
        if not found:
            clean_argv.append(arg)

    class Handler(SimpleHTTPServer.SimpleHTTPRequestHandler):
        def translate_path(self, path):
            if self.path in ["/logan.html", "/logan-vr.html"]:
                return os.path.dirname(os.path.realpath(__file__)) + self.path

            return SimpleHTTPServer.SimpleHTTPRequestHandler.translate_path(
                self, path)

        def do_GET(self):
            p = urlparse.urlparse(self.path)

            if p.path == '/logan-drill':
                return handle_drill(self, p, clean_argv, args.repo)

            if p.path == '/':
                self.path = '/logan.html'

            return SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

    port_num = int(args.http)

    SocketServer.TCPServer.allow_reuse_address = True

    server = SocketServer.TCPServer(('0.0.0.0', port_num), Handler)

    print "http server started"

    print "\nplease visit...\n"

    extra = ""
    if args.out_prefix != "out-logan":
        extra = "?outPrefix=" + args.out_prefix

    print "  http://localhost:" + str(port_num) + extra + "\n"

    server.serve_forever()


re_term_disallowed = re.compile(r"[^a-zA-Z0-9\-_/]")


def handle_drill(req, p, argv, repo):
    q = urlparse.parse_qs(p.query)

    if not q.get("start"):
        req.send_response(404)
        req.end_headers()
        req.wfile.close()
        return

    start = q.get("start")[0]

    max_entries = "1000"
    if q.get("max_entries"):
        max_entries = q.get("max_entries")[0]

    req.send_response(200)
    req.send_header("Content-type", "text/plain")
    req.end_headers()

    # Have logmerge.py emit to stdout.
    req.wfile.write("q: " + str(q))
    req.wfile.write("\n")

    req.wfile.write("\n=============================================\n")
    cmd = [os.path.dirname(os.path.realpath(__file__)) + "/logmerge.py",
           "--out=--", "--max-entries=" + max_entries, "--start=" + start] + \
        argv[1:]

    req.wfile.write(" ".join(cmd))
    req.wfile.write("\n\n")

    subprocess.call(cmd, stdout=req.wfile)

    req.wfile.write("\n\n=============================================\n")

    if repo:
        if q.get("terms"):
            terms = q.get("terms")[0].split(',')

            req.wfile.write("searching repo for terms: ")
            req.wfile.write(" ".join(terms))
            req.wfile.write("\n\n")

            terms = [re.sub(re_term_disallowed, '', term) for term in terms]
            terms = [term for term in terms if not keyword.iskeyword(term)]
            terms = [term for term in terms if not hasattr(__builtin__, term)]
            terms = [term for term in terms if len(term) >= 4]

            req.wfile.write("searching repo for terms (pre-filtered): ")
            req.wfile.write(" ".join(terms))
            req.wfile.write("\n\n")

            cmd, out = repo_grep_terms(repo, terms)

            req.wfile.write("filtered ")
            req.wfile.write(" ".join(cmd))
            req.wfile.write("\n\n")
            req.wfile.write(out)
            req.wfile.write("\n")
        else:
            req.wfile.write("(no terms for source code grep)\n\n""")
    else:
        req.wfile.write("(please provide --repo=/path/to/source/repo" +
                        " for source code grep)\n\n""")

    req.wfile.close()


def repo_grep_terms(repo, terms):
    if not terms:
        return ["error"], "(not enough terms to repo grep)"

    regexp = "|".join(terms)

    cmd = ["repo", "grep", "-n", "-E", regexp]

    repo = os.path.expanduser(repo)

    print repo
    print cmd

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, cwd=repo)

    rv = []

    best = []
    best_terms_left_len = len(terms)

    curr = None
    curr_terms_left = None
    curr_line_num = None

    for line in p.stdout:
        if len(line) > 1000:
            continue

        # A line looks like "fileName:lineNum:lineContent".
        line_parts = line.split(":")
        if len(line_parts) < 3:
            continue

        line_num = int(line_parts[1])

        # If we're still in the same fileName, on the very next
        # lineNum, and some new terms are now matching in the
        # lineContent, then extend the curr info.
        if (curr and
            curr[0][0] == line_parts[0] and
            curr_line_num + 1 == line_num and
            remove_matching(curr_terms_left,
                            line_parts[2:])):
            curr.append(line_parts)
            curr_line_num = line_num
        else:
            # Else start a new curr info.
            curr = [line_parts]
            curr_terms_left = list(terms)
            remove_matching(curr_terms_left, line_parts[2:])
            curr_line_num = line_num

        # See if we have a new best scoring curr.
        if best_terms_left_len >= len(curr_terms_left):
            best = list(curr)  # Copy.
            best_terms_left_len = len(curr_terms_left)

            rv.append("".join([":".join(x) for x in best]))
            if len(rv) > 10:
                rv = list(rv[-10:])

        if best_terms_left_len <= 0:
            break

    rv.reverse()

    return cmd, "\n".join(rv[:5])


def remove_matching(terms, parts):
    removed = False
    for part in parts:
        for part_term in re.split(re_term_disallowed, part):
            for i, term in enumerate(terms):
                if term == part_term:
                    del terms[i]
                    removed = True
                    break
    return removed
