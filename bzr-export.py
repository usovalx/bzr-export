#!/usr/bin/env python2

from bzrlib import branch, bzrdir, revision, repository
from bzrlib import errors as berrors
import datetime
import email.utils
import getopt
import os
import re
import subprocess
import sys
import tempfile
import time

class Config(object):
    """Misc stuff here -- various config flags & marks management"""

    def __init__(self):
        self.debug = False
        self.excludedFiles = set()
        self.edits = dict()
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
    m = """Usage: bzr-export.py [-h] [-f] [-d] [-m <marks file>] [-x <excludes>] <path>

   -h      Show this message.
   -b      Branch name for git.
           Only used when exporting single branch, not a shared repo.
   -f      Force export of all branches, even if they are cached in marks.
   -d      Debugging. Writes some debug info as comments in the resulting stream.
   -m      Load/save marks into this file.
   -x      Read a list of excluded files/directories. File is eval'd and should
           return python array of file names.
   -e      Read a list of editing commands. File is eval's and shoud return python
           dictionary. Keys of the dictionary are file names, and corresponding value
           is command-line which would be run to edit the file. Command-line is
           interpolated to replace single {} with the name of the file to edit.


Both branch and shared repo can be provided as a source. In case of shared repo,
all branches in it will be exported. If you are doing initial export you really
want to use -f, so that all branches are exported.

Excludes specified via -x are matched against both files and directories. Don't try
to force matching against directories by appending '/' -- it won't match at all.
Example: [ "foo", "bar/buz", ]

Editing commands specified via -e are only applied to REGULAR files. Directories or
symlinks aren't editable. Edits should normally be stable. If the editing command
exits with non-zero return value export is aborted.
Example: { "foo": "sed -e 's/foo_string/bar_string/g' -i {}" }

Resulting fast-export stream is sent to standard output.
"""
    print(m)
    sys.exit(1)

def main(argv):
    # parse command-line flags
    try:
        opts, args = getopt.getopt(argv, 'fhdm:b:x:e:')
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
        elif o == '-x':
            f = open(v, 'r')
            x = eval(f.read())
            f.close()
            cfg.excludedFiles = set(x)
        elif o == '-e':
            f = open(v, 'r')
            x = eval(f.read())
            f.close()
            cfg.edits = dict(x)
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
            stats = Stats()
            for revid in hist:
                if cfg.getMark(revid):
                    stats.skipped()
                    continue
                exportCommit(revid, ref, branch, cfg)
                stats.exported()
                if stats._exported % 5000 == 0:
                    log("{}", stats)
            cfg.save()
            log("Finished export: {}", stats)
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

    debug(cfg, 'committer: {}\n', rev.committer.encode('utf8'))

    thisMark = cfg.newMark(revid)
    parentsMarks = map(cfg.getMark, parents)
    assert(all(parentsMarks)) # FIXME: check that all parents are present in marks
    emitCommitHeader(ref, thisMark, rev, parentsMarks)
    oldTree, newTree = map(branch.repository.revision_tree, [parentRev, revid])
    exportTreeChanges(oldTree, newTree, cfg)
    out('\n')

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
        debug(cfg, 'change: {}\n', c)
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
    def isNested(roots, path):
        for x in roots:
            if path.startswith(x + '/'):
                return True
        return False

    def cleanDirs(dirs):
        if '' in dirs:
            return ['']
        # sort them, so that we traverse shallower dirs first
        dirs = sorted(dirs)
        r = []
        for d in dirs:
            if d in cfg.excludedFiles or isNested(cfg.excludedFiles, d):
                continue
            if not isNested(r, d):
                r.append(d)
        return r

    def cleanFiles(dirs, files):
        if '' in dirs:
            return []
        r = []
        for f in files:
            if f[0] in cfg.excludedFiles or isNested(cfg.excludedFiles, f[0]):
                continue
            if not isNested(dirs, f[0]):
                r.append(f)
        return r

    delDirs = cleanDirs(delDirs)
    newDirs = cleanDirs(newDirs)
    delFiles = cleanFiles(delDirs, delFiles)
    newFiles = cleanFiles(newDirs, newFiles)
    debug(cfg, 'delDirs: {}\n# delFiles: {}\n# newDirs: {}\n# newFiles: {}\n', delDirs, delFiles, newDirs, newFiles)

    keepmes = set()
    def emitDeleteAndKeepme(path):
        # check if we need to emit placeholder to keep parent dir alive
        parent = path.rpartition('/')[0]
        if parent != '' and parent not in keepmes and emptyDir(parent, cfg, newTree):
            keepmes.add(parent)
            emitPlaceholder(parent)
        emitDelete(path)

    # and finally -- write out resulting changes
    keepmes = set()
    for d in delDirs:
        if d == '':
            emitDeleteAll()
        else:
            emitDeleteAndKeepme(d)
    for f in delFiles:
        emitDeleteAndKeepme(f[0])
    for d in newDirs:
        exportSubTree(d, newTree, cfg)
    for f in newFiles:
        emitFile(f[0], f[1], f[2], newTree, cfg)

def exportSubTree(path, tree, cfg):
    for item in tree.walkdirs(prefix=path):
        # we need to update it in-place to prevent it from walking into excluded items
        excludes = filter(lambda x: x[0] in cfg.excludedFiles, item[1])
        for x in excludes:
            item[1].remove(x)
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
                    emitFile(obj[0], obj[2], obj[4], tree, cfg)

def emitReset(ref, mark):
    if mark:
        out('reset {0:s}\nfrom {1:s}\n\n', ref, mark)
    else:
        out('reset {0:s}\n\n', ref)

def emitCommitHeader(ref, mark, revobj, parents):
    headF = 'commit {}\nmark {}\ncommitter {} {}\n'
    out(headF, ref, mark, formatName(revobj.committer), formatTimestamp(revobj.timestamp, revobj.timezone))
    msg = revobj.message.encode('utf8')
    msg += '\n\nBazaar: revid:%s' % revobj.revision_id
    out('data {}\n{:s}\n', len(msg), msg)
    if len(parents) != 0:
       fmt = 'from {}\n' + 'merge {}\n'*(len(parents)-1)
       out(fmt, *parents)

def emitFile(path, kind, fileId, tree, cfg):
    if kind == 'file':
        data = tree.get_file_text(fileId)
        if path in cfg.edits:
            data = editFile(path, cfg.edits[path], data)
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
    out('M {} inline {}\ndata {}\n{}\n', mode, formatPath(path), len(data), data)

def emitPlaceholder(path):
    out('M 644 inline {}\ndata 0\n', formatPath(path + '/.keepme'))

def emitDelete(path):
    out('D {}\n', formatPath(path))

def emitDeleteAllout():
    out('deleteall\n')

def emptyDir(path, cfg, tree):
    for x in tree.walkdirs(prefix=path):
        # filter out excluded entries and check if it's empty
        r = filter(lambda t: t[0] not in cfg.excludedFiles, x[1])
        return len(r) == 0

def editFile(path, command, data):
    t = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
    t.file.write(data)
    t.close()
    ret = subprocess.call(command.format(t.name), shell=True, stdout=sys.stderr)
    if ret != 0:
        err('Command {!r} exited with error code {}; leaving temporary file in place', command.format(f.name), ret)
    else:
        # reopen temp file, as editing command might have replaced it
        f = open(t.name, "rb")
        data = f.read()
        f.close()
        os.unlink(t.name)
        return data

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
    if path[0] == '"' or '\n' in path:
        path = path.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
        return '"%s"' % path.encode('utf8')
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

def out(f, *args):
    sys.stdout.write(f.format(*args))

def debug(cfg, f, *args):
    if cfg.debug:
        out("# " + f, *args)

def prof():
    import cProfile
    out = open("/dev/null", "w")
    save = sys.stdout
    try:
        sys.stdout = out
        cProfile.run('main(["/home/usov/build/testrepo/docky/trunk"])', "prof")
    finally:
        sys.stdout = save

if __name__ == '__main__':
    main(sys.argv[1:])

