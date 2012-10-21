#!/usr/bin/env python2

from bzrlib import branch, bzrdir, revision, repository
from bzrlib import errors as berrors
import datetime
import email.utils
import getopt
import os
import re
import sys
import time

class Config(object):
    """Misc stuff here -- various config flags & marks management"""

    def __init__(self):
        self.debug = False
        self.fname = None
        self.forceAll = False
        self.refName = None

        self.nextMark = 1
        self.marks = {}

    def load(self, fname):
        self.fname = fname
        if os.path.exists(fname):
            f = open(fname, "r")
            maxMark = 1
            for l in f:
                s = l.split()
                if len(s) == 0:
                    continue
                if len(s) != 2:
                    log("ERROR: this doesn't looks like marks string: {}", l.strip())
                    continue
                maxMark = max(maxMark, int(s[0][1:]))
                self.marks[s[1]] = s[0]
            f.close()
            self.nextMark = maxMark
            log("Marks loaded: {} entries", len(self.marks))
        else:
            # try creating empty file, just to make sure we can write there
            f = open(fname, "w")
            f.close()

    def save(self):
        if self.fname is None:
            return
        f = open(self.fname, "w")
        for r, m in self.marks.iteritems():
            f.write('%s %s\n' % (m, r))
        f.close()

    def newMark(self, revid):
        """Add new revision to the set and return its mark"""
        m = ':{}'.format(self.nextMark)
        self.marks[revid] = m
        self.nextMark += 1
        return m

    def getMark(self, revid):
        """Get a mark corresponding to revid. Returns None if revision isn't marked"""
        return self.marks.get(revid, None)

class Stats(object):
    def __init__(self):
        self._skipped = 0
        self._exported = 0
        self._starttime = time.time()

    def __str__(self):
        now = time.time()
        dur = datetime.timedelta(seconds = now - self._starttime)
        return "{} exported {} reused, time spent {} ({} revision/minute)".format(
            self._exported, self._skipped, dur, int(self._exported*60/(now - self._starttime)))

    def skipped(self):
        self._skipped += 1

    def exported(self):
        self._exported += 1

def usage():
    m = """Usage: bzr-export.py [-h] [-f] [-d] [-m <marks file>] <path>

   -h      Show this message.
   -b      Branch name for git.
           Only used when exporting single branch, not a shared repo.
   -f      Force export of all branches, even if they are cached in marks.
   -d      Debugging. Writes some debug info as comments in the resulting stream.
   -m      Load/save marks into this file.

Both branch and shared repo can be provided as a source. In case of shared repo,
all branches in it will be exported. If you are doing initial export you really
want to use -f, so that all branches are exported.

Resulting fast-export stream is sent to standard output.
"""
    print(m)
    sys.exit(1)

def main(argv):
    # parse command-line flags
    try:
        opts, args = getopt.getopt(argv, 'fhdm:b:')
    except getopt.GetoptError as e:
        err(e)

    if len(args) != 1:
        usage()

    cfg = Config()
    for o, v in opts:
        if o == '-h':
            usage()
        elif o == '-m':
            cfg.load(v)
        elif o == '-d':
            cfg.debug = True
        elif o == '-f':
            cfg.forceAll = True
        elif o == '-b':
            cfg.refName = v
        else:
            usage()

    # proceed to export
    startExport(args[0], cfg)

def startExport(path, cfg):
    # try opening it as branch
    try:
        b = branch.Branch.open(path)
        name = cfg.refName or b.user_url.rstrip('/').split('/')[-1]
        exportBranch(b, formatRefName(name), cfg)
        return
    except berrors.NotBranchError:
        pass

    # or as shared repo
    try:
        repo = repository.Repository.open(path)
        prevRefs = set()
        bs = repo.find_branches()
        for b in bs:
            assert(b.user_url.startswith(repo.user_url))
            name = b.user_url[len(repo.user_url):].strip('/')
            ref = formatRefName(name)
            if ref in prevRefs:
                log("ERROR: refname rewriting resulted in colliding refnames.")
                log("ERROR: ref {} for branch {}", ref, b.user_url)
                log("ERROR: skipping this branch")
                continue
            prevRefs.add(ref)
            exportBranch(b, ref, cfg)
    except berrors.BzrError as e:
        err(e)

def exportBranch(branch, ref, cfg):
    log('Exporting branch {0} as {1} ({2})', branch.nick, ref, branch.user_url)
    stats = Stats()
    lockobj = branch.lock_read()
    try:
        if cfg.getMark(branch.last_revision()):
            if cfg.forceAll:
                emitReset(ref, cfg.getMark(branch.last_revision()))
            else:
                log("Nothing to do: no new revisions")
        else:
            # get full history of the branch
            hist = [x[0] for x in branch.iter_merge_sorted_revisions(direction='forward')]
            log('Starting export of {0:d} revisions', len(hist))
            for revid in hist:
                if cfg.getMark(revid):
                    stats.skipped()
                    continue
                exportCommit(revid, ref, branch, cfg)
                stats.exported()
                if stats._exported % 5000 == 0:
                    log("{}", stats)
            cfg.save()
    finally:
        lockobj.unlock()
    log("Finished export: {}", stats)

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

    if cfg.debug:
        sys.stdout.write('# committer: {}\n'.format(rev.committer.encode('utf8')))

    thisMark = cfg.newMark(revid)
    parentsMarks = map(cfg.getMark, parents)
    assert(all(parentsMarks)) # FIXME: check that all parents are present in marks
    emitCommitHeader(ref, thisMark, rev, parentsMarks)
    oldTree, newTree = map(branch.repository.revision_tree, [parentRev, revid])
    exportTreeChanges(oldTree, newTree, cfg)
    sys.stdout.write('\n')

def exportTreeChanges(oldTree, newTree, cfg):
    # In general case exporting changes from bzr is highly nontrivial
    # bzr is tracking each file & directory by internal ids.
    # If you want to preserve history correctly you are forced to do some
    # very messy voodo to correctly order and emit renames/deletes/modifications

    # However I'm exporting stuff to git which is much simpler -- I can simply
    # delete all modified paths and export them afresh. This is a bit slower, but
    # much easier to implement correctly. There is one complication though -- my
    # bzr repo has empty dirs in the history, and I want to preserve them in the
    # export by emitting .keepme files where appropriate. To correctly track them
    # during deletes/renames I need to make sure I correctly issue whole-dir commands.

    # The resulting algorithm is following:
    # * rewrite the list of changes between two trees in terms of simple delete/create
    # commands and split them into 4 groups:
    #    delete/create directories & delete/create(modify) files
    # * simplify the resulting list of operations by taking into account subdirectory
    # operations. E.g. we don't need to separately delete nested files/subdirs if
    # parent directory is to be deleted. Same with creation.
    # * Finally issue resulting list of deletes & then creations.

    delDirs, newDirs, delFiles, newFiles = [], [], [], []
    def addDel(c):
        if c[6][0] == 'directory':
            delDirs.append(c[1][0])
        else:
            delFiles.append((c[1][0], c[6][0], c[0]))
    def addNew(c):
        if c[6][1] == 'directory':
            newDirs.append(c[1][1])
        else:
            newFiles.append((c[1][1], c[6][1], c[0]))
    for c in newTree.iter_changes(oldTree):
        # c is a tuple (file_id, (path_in_source, path_in_target),
        #    changed_content, versioned, parent, name, kind,
        #    executable)
        if cfg.debug:
            sys.stdout.write('# change: {}\n'.format(c))
        if c[1][0] is None:  # stuff added
            assert(c[6][0] is None)
            assert(c[1][1] is not None and c[6][1] is not None)
            addNew(c)
        elif c[1][1] is None: # removed
            assert(c[6][1] is None)
            assert(c[1][0] is not None and c[6][0] is not None)
            addDel(c)
        else: # changed or moved
            # files changed in-place don't have to be deleted, everything else
            # becomes delete + new item
            if c[1][0] == c[1][1] and c[6][0] != 'directory' and c[6][1] != 'directory':
                addNew(c)
            else:
                addDel(c)
                addNew(c)

    # now clean up nested directories and files
    def isIncluded(roots, path):
        for x in roots:
            if path.startswith(x):
                return True
        return False

    def cleanDirs(dirs):
        # sort them, so the roots are first
        dirs = sorted(dirs)
        r = []
        for d in dirs:
            if not isIncluded(r, d):
                r.append(d)
        return r

    def cleanFiles(dirs, files):
        r = []
        for f in files:
            if not isIncluded(dirs, f[0]):
                r.append(f)
        return r

    delDirs = cleanDirs(delDirs)
    newDirs = cleanDirs(newDirs)
    delFiles = cleanFiles(delDirs, delFiles)
    newFiles = cleanFiles(newDirs, newFiles)

    if cfg.debug:
        sys.stdout.write('# delDirs: {}\n# delFiles: {}\n# newDirs: {}\n# newFiles: {}\n'.format(delDirs, delFiles, newDirs, newFiles))

    # and finally -- write out resulting changes
    keepmes = set()
    for d in delDirs:
        if d == '':
            emitDeleteAll()
        else:
            emitDelete(d)
    for f in delFiles:
        # check if we need to emit placeholder to keep dir alive
        base = f[0].rpartition('/')[0]
        if base != '' and base not in keepmes and emptyDir(base, newTree):
            keepmes.add(base)
            emitPlaceholder(base)
        emitDelete(f[0])
    for d in newDirs:
        exportSubTree(d, newTree, cfg)
    for f in newFiles:
        emitFile(f[0], f[1], f[2], newTree)

    return delDirs, newDirs, delFiles, newFiles


def exportSubTree(path, tree, cfg):
    for item in tree.walkdirs(prefix=path):
        if len(item[1]) == 0:
            # empty dir -- write placeholder to keep it alive
            emitPlaceholder(item[0][0])
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
    msg += '\n\nBazaar: revid:%s' % revobj.revision_id
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

def emitPlaceholder(path):
    sys.stdout.write('M 644 inline {}\ndata 0\n'.format(formatPath(path + '/.keepme')))

def emitDelete(path):
    sys.stdout.write('D {}\n'.format(formatPath(path)))

def emitDeleteAll():
    sys.stdout.write('deleteall\n')

def emptyDir(path, tree):
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

emailRe = re.compile(r'[<>@\n]')
def formatName(name):
    if emailRe.search(name):
        name, mail = email.utils.parseaddr(name.encode('utf8'))
        if name == '':
            return '<%s>' % mail
        else:
            return '%s <%s>' % (name, mail)
    else:
        return '%s <>' % name

def formatPath(path):
    assert(path is not None)
    assert(path != '')
    assert(path[0] != '/')
    quote = False
    if '\n' in path:
        quote = True
        path = path.replace('\n', '\\n')
    if path[0] == '"':
        quote = True
        path = path.replace('"', '\\"')
    if quote:
        return '"%s"' % path.encode('utf8')
    else:
        return path.encode('utf8')

# stolen from bzr fast-export
def formatRefName(branchName):
    """Rewrite branchName so that it will be accepted by git-fast-import.
    For the detailed rules see check_ref_format.

    By rewriting the refname we are breaking uniqueness guarantees provided by bzr
    so we have to manually verify that resulting ref names are unique.

    http://www.kernel.org/pub/software/scm/git/docs/git-check-ref-format.html
    """
    refname = "refs/heads/%s" % branchName.encode('utf8')
    refname = re.sub(
        # '/.' in refname or startswith '.'
        r"/\.|^\."
        # '..' in refname
        r"|\.\."
        # ord(c) < 040
        r"|[" + "".join([chr(x) for x in range(040)]) + r"]"
        # c in '\177 ~^:?*['
        r"|[\177 ~^:?*[]"
        # last char in "/."
        r"|[/.]$"
        # endswith '.lock'
        r"|.lock$"
        # "@{" in refname
        r"|@{"
        # "\\" in refname
        r"|\\",
        "_", refname)
    return refname

def log(f, *args):
    sys.stderr.write((str(f) + '\n').format(*args))

def err(f, *args):
    log('ERROR: ' + str(f), *args)
    sys.exit(1)

if __name__ == '__main__':
    main(sys.argv[1:])

