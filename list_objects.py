#!/usr/bin/env python2

import git
import sys

def main(args):
    if len(args) != 1:
        sys.stderr.write("Need path to the repo\n")
        sys.exit(1)

    repo = git.Repo(args[0], odbt=git.GitCmdObjectDB)
    vmap = set()
    i = 0
    for c in repo.iter_commits('--all'):
        traverse(c.tree, vmap)
        i += 1
        if i % 5000 == 0:
            sys.stderr.write("Scanned %d revisions\n" % i)

def traverse(t, vmap):
    if t.hexsha in vmap:
        return

    vmap.add(t.hexsha)

    # print out unseen blobs
    for b in t.blobs:
        if b.hexsha not in vmap and b.size > 102400:
            vmap.add(b.hexsha)
            print('%s  %8d  %s' % (b.hexsha, b.size/1024, b.path))

    # and sub-trees
    for st in t.trees:
        traverse(st, vmap)

if __name__ == '__main__':
    main(sys.argv[1:])
