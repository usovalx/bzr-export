#!/usr/bin/env python2

import datetime
import git
import sys
import time

def main(args):
    if len(args) != 1:
        sys.stderr.write("Need path to the repo\n")
        sys.exit(1)

    repo = git.Repo(args[0], odbt=git.GitCmdObjectDB)
    vmap = set()
    i = 0
    startTime = time.time()
    for c in repo.iter_commits('--all'):
        traverse(c.tree, vmap, c)
        i += 1
        if i % 5000 == 0:
            now = time.time()
            dur = datetime.timedelta(seconds = now - startTime)
            sys.stderr.write("Scanned {} revisions in {} ({} revisions/minute)\n".format(
                i, dur, int(i*60/(now-startTime))))

def traverse(t, vmap, c):
    if t.hexsha in vmap:
        return

    vmap.add(t.hexsha)

    # print out unseen blobs
    for b in t.blobs:
        if b.hexsha not in vmap and b.size > 102400:
            vmap.add(b.hexsha)
            print('%s  %s  %8d  %s' % (b.hexsha, c.hexsha, b.size/1024, b.path))

    # and sub-trees
    for st in t.trees:
        traverse(st, vmap, c)

if __name__ == '__main__':
    main(sys.argv[1:])
