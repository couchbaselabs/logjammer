#!/usr/bin/env python
# -*- mode: Python;-*-

import os
import subprocess

# See progressbar2 https://github.com/WoLpH/python-progressbar
# Ex: pip install progressbar2
import progressbar


def chunkify_path_sizes(path_sizes, default_chunk_size):
    chunks = []

    for path, size in path_sizes.iteritems():
        chunk_size = default_chunk_size or size

        x = 0
        while size and x < size and chunk_size:
            chunks.append((path, x, chunk_size))
            x += chunk_size

    chunks.sort()

    return chunks


# Allows the parent process to wait until there are enough done worker
# messages, while also keeping a progress bar updated.
def multiprocessing_wait(q, num_chunks, total_size):
    bar = progressbar.ProgressBar(max_value=total_size)

    num_done = 0
    progress = {}

    while num_done < num_chunks:
        bar.update(min(total_size, sum(progress.itervalues())))

        x = q.get()
        if x == "done":
            num_done += 1
        else:
            chunk, amount = x
            progress[chunk] = amount


# QueueBar implements a subset of progress bar methods, forwarding
# update() invocations to a queue.
class QueueBar(object):
    def __init__(self, chunk, q):
        self.chunk = chunk
        self.q = q

    def start(self, max_value=None):
        pass  # Ignore since parent has an aggregate max_value.

    def update(self, amount):
        self.q.put((self.chunk, amount), False)


def git_describe_long():
    return subprocess.check_output(
        ['git', 'describe', '--long'],
        cwd=os.path.dirname(os.path.realpath(__file__))).strip()


# See: https://stackoverflow.com/questions/956867/
#      how-to-get-string-objects-instead-of-unicode-from-json
def byteify(data, ignore_dicts=False):
    if isinstance(data, unicode):
        return data.encode('utf-8')

    if isinstance(data, list):
        return [byteify(item, ignore_dicts=True) for item in data]

    if isinstance(data, dict) and not ignore_dicts:
        return {
            byteify(key, ignore_dicts=True): byteify(value, ignore_dicts=True)
            for key, value in data.iteritems()
        }

    return data
