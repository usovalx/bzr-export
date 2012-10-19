#!/usr/bin/env python2

from bzrlib import branch, bzrdir, revision, repository
from bzrlib import errors as berrors
import sys
import getopt

def log(f, *args):
    sys.stderr.write((str(f) + "\n").format(*args))

def err(f, *args):
    log("ERROR: " + str(f), *args)
    sys.exit(1)


def exportBranch(branch):
    log("Exporting branch {} ({})", branch.nick, branch.base)
    lockobj = branch.lock_read()
    try:
        pass
    finally:
        lockobj.unlock()

    
def startExport(path):
    # try opening it as branch
    try:
        b = branch.Branch.open(path)
        return exportBranch(b)
    except berrors.NotBranchError:
        pass

    # or as shared repo
    try:
        repo = repository.Repository.open(path)
        bs = repo.find_branches()
        for b in bs:
            exportBranch(b)
        return repo
    except berrors.BzrError as e:
        err(e)

def usage():
    m = """Usage: bzr-export.py [-h] [-m <marks file>] <path to branch or shared repo>

Both branch and shared repo can be provided as a source. In case of shared repo,
all branches in it will be exported.

Resulting stream is sent to standard output.

Marks file is loaded before export begins, and will be used to "resume" export.
"""
    print(m)
    sys.exit(1)

def main(argv):
    # parse command-line flags
    try:
        opts, args = getopt.getopt(argv, "hm:")
    except getopt.GetoptError as e:
        err(e)

    if len(args) != 1:
        usage()

    marksFile = None
    for o, v in opts:
        if o == '-h':
            usage()
        elif o == '-m':
            err("Marks not implemented yet")
            marksFile = v
        else:
            usage()
        
    # proceed to export
    startExport(args[0])

if __name__ == '__main__':
    main(sys.argv[1:])

