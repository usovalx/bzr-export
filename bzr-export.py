#!/usr/bin/env python2

from bzrlib import branch, bzrdir, revision, repository
from bzrlib import errors as berrors
import email.utils
import sys
import getopt

class Config(object):
    """Misc stuff here -- various config flags & marks management"""
    
    def __init__(self):
        self.nextMark = 1
        self.marks = {}

    def newMark(self, revid):
        """Add new revision to the set and return its mark"""
        m = ':{}'.format(self.nextMark)
        self.marks[revid] = m
        self.nextMark += 1
        return m

    def getMark(self, revid):
        """Get a mark corresponding to revid. Returns None if revision isn't marked"""
        return self.marks.get(revid, None)

def usage():
    m = """Usage: bzr-export.py [-h] [-m <marks file>] <path to branch or shared repo>

Both branch and shared repo can be provided as a source. In case of shared repo,
all branches in it will be exported.

Resulting stream is sent to standard output.

Marks file is loaded before export begins, and will be used to 'resume' export.
"""
    print(m)
    sys.exit(1)

def main(argv):
    # parse command-line flags
    try:
        opts, args = getopt.getopt(argv, 'hm:')
    except getopt.GetoptError as e:
        err(e)

    if len(args) != 1:
        usage()

    cfg = Config()
    for o, v in opts:
        if o == '-h':
            usage()
        elif o == '-m':
            err('Marks not implemented yet')
            marksFile = v
        else:
            usage()
        
    # proceed to export
    startExport(args[0], cfg)

def startExport(path, cfg):
    # try opening it as branch
    try:
        b = branch.Branch.open(path)
        exportBranch(b, cfg) ## FIXME: return here
        return b
        return
    except berrors.NotBranchError:
        pass

    # or as shared repo
    try:
        repo = repository.Repository.open(path)
        bs = repo.find_branches()
        for b in bs:
            exportBranch(b, cfg)
    except berrors.BzrError as e:
        err(e)

def exportBranch(branch, cfg):
    branchName = branch.nick ## FIXME: subdirs, sanitize
    refName = 'refs/heads/' + branchName
    log('Exporting branch {0} as {1} ({2})', branchName, refName, branch.base)
    lockobj = branch.lock_read()
    try:
        # TODO: check if branch.last_revision() is in the cached set and export it as plain reset?
        #   only valid for the first export???

        # get full history of the branch
        hist = [x[0] for x in branch.iter_merge_sorted_revisions(direction='forward')]
        log('Starting export of {0:d} revisions', len(hist))
        ## FIXME: add some timings
        for revid in hist:
            # skip those which are already exported
            exportCommit(revid, refName, branch, cfg)
    finally:
        lockobj.unlock()

def exportCommit(revid, ref, branch, cfg):
    try:
        rev = branch.repository.get_revision(revid)
    except berrors.NoSuchRevision:
        # ghost revision?
        # that's what bzr fast-export plugin does, but I have no idea what these ghosts are
        log('WARN: encountered ghost revision  {}', revid)
        return

    parents = rev.parent_ids
    if len(parents) == 0:
        parentRev = revision.NULL_REVISION
        emitReset(ref, None)
    else:
        parentRev = parents[0]

    thisMark = cfg.newMark(revid)
    parentsMarks = map(cfg.getMark, parents)
    assert(all(parentsMarks)) # FIXME: check that all parents are present in marks
    emitCommitHeader(ref, thisMark, rev, parentsMarks)
    oldTree, newTree = map(branch.repository.revision_tree, [parentRev, revid])
    exportTreeChanges(oldTree, newTree, cfg)
    sys.stdout.write('\n')

def exportTreeChanges(oldTree, newTree, cfg):
    changes = newTree.iter_changes(oldTree)
    # changes is a list of tuples 
    # (file_id, (path_in_source, path_in_target),
    #    changed_content, versioned, parent, name, kind,
    #    executable)

    # renaming of dirs, deletion of dirs, moving stuff around are tricky to implement correctly
    # however with Git we can just delete old content, and spit out new one
    

def exportSubTree(path, tree, cfg):
    for item in tree.walkdirs(prefix=path):
        if len(item[1]) == 0:
            # empty dir -- emit placeholder to keep it alive
            # FIXME: quote filename
            sys.stdout.write('M 644 inline {}/.keepme\ndata 0\n'.format(item[0][0]))
        else:
            for obj in item[1]:
                # obj is (relpath, basename, kind, lstat?, file_id, versioned_kind)
                if not obj[2]: # I don't think this might ever happen, but....
                    log("WARN: empty kind when walking tree of {}: subdir: {}", tree.get_revision_id(), item[0][0])
                    continue
                elif obj[2] == 'directory':
                    # skip dir entries -- we will step into them later
                    continue
                else:
                    emitFile(obj[0], obj[2], obj[4], tree)
    
def emitReset(ref, mark):
    if mark:
        sys.stdout.write('reset {0:s}\nfrom {1:s}\n\n'.format(ref, mark))
    else:
        sys.stdout.write('reset {0:s}\n\n'.format(ref))

def emitCommitHeader(ref, mark, revobj, parents):
    headF = 'commit {}\nmark {}\ncommitter {} {}\n'
    sys.stdout.write(headF.format(ref, mark, formatName(revobj.committer), formatTimestamp(revobj.timestamp, revobj.timezone)))
    msg = revobj.message.encode('utf8')
    msg += '\n\n revid:%s' % revobj.revision_id
    sys.stdout.write('data %d\n%s\n' % (len(msg), msg))
    if len(parents) != 0:
       fmt = 'from {}\n' + 'merge {}\n'*(len(parents)-1)
       sys.stdout.write(fmt.format(*parents))

def emitFile(path, kind, fileId, tree):
    if kind == 'file':
        data = tree.get_file_text(fileId)
        if tree.is_executable(fileId):
            mode = '755'
        else:
            mode = '644'
    elif kind == 'symlink':
        mode = '120000'
        data = tree.get_symlink_target(fileId)
    else:
        log("WARN: unsupported file kind '{}' in {} path {}", kind, tree.get_revision_id(), path)
        return

    # fixme quote filename
    sys.stdout.write('M {} inline {}\ndata {}\n{}\n'.format(mode, path, len(data), data))
    
def emptyDir(tree, path):
    for x in tree.walkdirs(prefix=path):
        return len(x[1]) == 0
       
def formatTimestamp(timestamp, offset):
    if offset < 0:
        sign = '-'
        offset = -offset
    else:
        sign = '+'
    hours, offset = offset/3600, offset % 3600
    minutes = offset/60
    return '%d %s%02d%02d' % (timestamp, sign, hours, minutes)

def formatName(name):
    name, mail = email.utils.parseaddr(name.encode('utf8'))
    if name == '':
        return '<%s>' % mail
    else:
        return '%s <%s>' % (name, mail)
    
def log(f, *args):
    sys.stderr.write((str(f) + '\n').format(*args))

def err(f, *args):
    log('ERROR: ' + str(f), *args)
    sys.exit(1)

if __name__ == '__main__':
    main(sys.argv[1:])

