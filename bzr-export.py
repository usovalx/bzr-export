#!/usr/bin/env python2

from bzrlib import branch, revision, repository, tsort
from bzrlib import errors as berrors
import datetime
import email.utils
import getopt
import hashlib
import os
import re
import subprocess
import sys
import tempfile
import time

def usage():
    m = """Usage: bzr-export.py [flags] <path to repo or branch>

   --all-tags
           Export tags from all Bzr branches. Creates annotated tags with empty message.
           It will try to use the same tag name as in Bzr, but may occasionally need to
           rewrite it to make it git-compatible. If there are conflicting tags in
           different branches they won't be exported. Conflicts with --tags.

   -b <name>
           Branch name for git.
           Only used when exporting single branch, not a shared repo.

   --branches=<file>
           Include/exclude rules for branches are read from the file.
           Only used when exporting whole repository.

   -d      Debugging. Writes some debug info as comments in the resulting stream.

   -e <file>
           Read a list of editing commands. File is eval'd and should return python
           dictionary. Keys of the dictionary are file names, and corresponding value is
           command-line which would be run to edit the file. Command-line is interpolated
           to replace {0} with the name of the file to edit.

   --export=<RE>
           Limit the branches in the repository which are going to be exported. RE is a
           regular expression and it will be matched against full branch name (e.g. branch
           path relative to the repo root). You can specify this option multiple times.
           Only used when exporting whole repository.

   -f      Force export of all branches, even if they are cached in marks.

   -h/--help
           Show this message.

   -i <file>
           List of files to inject into the repository during import. File is eval'd and
           should return python dictionary of injected path -> source file name.

   -m <file>
           Load/save marks into this file.

   --skip=<RE>
           Don't export branches whose name matches given RE. Matching is done similarly
           to --only. You can specify this option multiple times.
           Only used when exporting whole repository.

   --tags=<file>
           Read the list of tags to be exported from the given file. This method allows to
           select/rename tags. Conflicts with --all-tags.

   -x <file>
           Read a list of excluded files/directories. File is eval'd and should return
           python array of file names.


Both branch and shared repo can be provided as a source. In case of shared repo, all
branches in it will be exported. By default only branches which contain new commits will
be written into export stream. Specifying -f overrides this behaviour and exports all
branches (up to --export/--skip/--branches filters).

Simple include/exclude patters for branches can be specifies directly on the command line
using --export/--skip flag. A larger (or more permanent) set of include/exclude rules
branches can be specified via --branches. Patterns read from the files and those specifies
on the command line are considered all together.
Example file:
  {
    'export': [ r"^trunk", r"^branches" ],
    'skip': [ r"secret_branch" ]
  }

File excludes specified via -x are matched against both files and directories. Don't try
to force matching against directories only by appending '/' -- it won't match at all.
Example file:
  [ "foo", "bar/buz", ]

Current implementation of file injection is quite simplistic and would only reliably work
if you are injecting files into the root of the tree. Other cases may work, but aren't
explicitly supported. In particular it won't check whether target directory exists and
won't do any smart handling if it is renamed or moved.
Example file:
  { ".gitignore": "source_file" }

Editing commands specified via -e are only applied to REGULAR files. Directories or
symlinks aren't editable. Edits should be stable. If the editing command exits with
non-zero return value export is aborted.
Example file:
  { "foo/bar": "sed -e 's/foo_string/bar_string/g' -i {0}" }

Tags aren't exported by default. With --all-tags it will try to export all tags from bzr.
In bazaar tags are per branch, and may conflict between different branches. If such
conflicts are found, corresponding tags won't be exported. If you want to export just some
of the tags and/or rename them during the export use --tags. File is eval'd and should
contain a dictionary of branchName -> list of tags. Every entry in the list of tags is
either a tag name, or a tuple of old name + new name.
Example file:
{
  'trunk': [ '1.2.0', ('1.3', 'release_1.3')],
}

If you have more algorithmic way of dealing with tags, you can provide lambda expression
which should return either None or new tag name given bzr branch & tag names.
Example file:
lambda branch, tag: 'tag_' + tag if branch == 'trunk' and re.search('^[0-9]', tag) else None

Resulting fast-export stream is sent to standard output.
"""
    print(m)
    sys.exit(1)

def main(argv):
    # parse command-line flags
    try:
        opts, args = getopt.getopt(argv,
            'b:de:fhi:m:x:',
            [
                'all-tags',
                'branches=',
                'export=',
                'help',
                'skip=',
                'tags='
            ])
    except getopt.GetoptError as e:
        err(e)

    if len(args) != 1:
        usage()

    cfg = Config()
    for o, v in opts:
        if o == '--all-tags':
            if cfg.tagFilter is not None:
                err("--all-tags and --tags don't go well together")
            cfg.tagFilter = lambda branch, tag: tag
        elif o == '-b':
            cfg.refName = v
        elif o == '--branches':
            rules = dict(readCfg(v))
            for rule in rules.get('export', []):
                cfg.exportList.add(rule)
            for rule in rules.get('skip', []):
                cfg.skipList.add(rule)
        elif o == '-d':
            cfg.debug = True
        elif o == '-e':
            cfg.edits = dict(readCfg(v))
        elif o == '--export':
            cfg.exportList.add(v)
        elif o == '-f':
            cfg.forceAll = True
        elif o == '-h' or o == '--help':
            usage()
        elif o == '-i':
            files = dict(readCfg(v))
            for k, v in files.iteritems():
                data = readFile(v)
                hasher = hashlib.sha1()
                hasher.update(data)
                id = 'INJECT-' + hasher.hexdigest()
                files[k] = (id, data)
            cfg.injects = files
        elif o == '-m':
            cfg.load(v)
        elif o == '--skip':
            cfg.skipList.add(v)
        elif o == '--tags':
            if cfg.tagFilter is not None:
                err("--all-tags and --tags don't go well together")
            x = readCfg(v)
            if callable(x):
                cfg.tagFilter = x
                # quick and dirty check that it accepts correct arguments
                cfg.tagFilter('foo', 'bar')
            else:
                x = dict(x)
                # convert list of tag names/mappings to map
                for b, ts in x.iteritems():
                    for i, tn in enumerate(ts):
                        if not isinstance(tn, tuple):
                            x[b][i] = (tn, tn)
                    x[b] = dict(ts)
                cfg.tagFilter = lambda branch, tag: x[branch].get(tag, None) if branch in x else None
        elif o == '-x':
            cfg.excludedFiles = set(readCfg(v))
        else:
            err('Unknown option: {0}')

    # proceed to export
    startExport(args[0], cfg)
    cfg.save()

def startExport(path, cfg):
    # try opening 'path' as shared repo
    try:
        repo = repository.Repository.open(path)
        log("Gathering list of branches in repo")
        allBranches = repo.find_branches()
        toExport = []
        prevRefs = set()
        for b in allBranches:
            assert(b.user_url.startswith(repo.user_url))
            name = b.user_url[len(repo.user_url):].strip('/')
            # if there is just 1 branch located at the very root of the repo,
            # handle its name as if it was branch checkout and not a shared repo
            if len(allBranches) == 1 and name == '':
                name = cfg.refName or b.user_url.rstrip('/').split('/')[-1]
            ref = formatBranchName(name)
            if ref in prevRefs:
                log("ERROR: refname rewriting resulted in colliding refnames.")
                log("ERROR: ref {0} for branch {1}", ref, b.user_url)
                log("ERROR: skipping this branch")
                continue
            prevRefs.add(ref)
            if cfg.exportList.m(name) and not cfg.skipList.m(name):
                toExport.append((ref, b, name))
        log("Selected {0} brances for export (out of {1} in repo)", len(toExport), len(allBranches))
        exportBranches(toExport, repo, cfg)
        return
    except berrors.NoRepositoryPresent:
        pass

    # or as a branch
    try:
        b = branch.Branch.open(path)
        name = cfg.refName or b.user_url.rstrip('/').split('/')[-1]
        exportBranches([(formatBranchName(name), b, name)], b.repository, cfg)
        return
    except berrors.NotBranchError as e:
        err(e)

def exportBranches(branches, repo, cfg):
    log("Collecting heads of all branches")
    branches = [(b.last_revision(), ref, b, name) for ref, b, name in branches]

    branchesToExport = []
    commitsToExport = []

    # if head is already in marks we don't need to do anything else
    knownBranches, branches = split(lambda b: cfg.getMark(b[0]), branches)
    if cfg.forceAll:
        branchesToExport = knownBranches

    # if there are new heads, lets download full list of revisions and try to
    # figure out which revisions we need to export
    if branches:
        log("Getting list of all revisions")
        revisions = set(repo.all_revision_ids())

        # filter out branches with bad heads
        branches, badHeads = split(lambda b: b[0] in revisions, branches)
        for head, ref, b, name in badHeads:
            log("WARN: {0} -- invalid or empty head ({1})", name, head)

    # now gather a list of revisions to export
    if branches:
        log("Collating a set of revisions to be exported")
        b, graph = collateNewHistory(branches, revisions, repo, cfg)
        branchesToExport.extend(b)

        log("Got {0} revisions to export. Sorting them", len(graph))
        commitsToExport = tsort.topo_sort(graph)

    if commitsToExport:
        lock = repo.lock_read()
        try:
            log("Starting export of {0} revisions", len(commitsToExport))
            cfg.stats = Stats()
            for revid in commitsToExport:
                if cfg.getMark(revid):
                    log("WARN: we shouldn't be here. Revid: {0}", revid)
                    cfg.stats.skipRev()
                    continue
                exportCommit(revid, "__bzr_export_tmp_ref", repo, cfg)
                cfg.stats.exportRev()
                if cfg.stats._exportedRevs % 1000 == 0:
                    log("{0}", cfg.stats)
            log("Finished: {0}", cfg.stats)
        finally:
            lock.unlock()

    log("Writing {0} branch references", len(branchesToExport))
    buf = []
    for head, ref, b, name in branchesToExport:
        #log("Exporting branch {0} as {1} ({2})", b.nick, ref, b.user_url)
        mark = cfg.getMark(head)
        assert(mark is not None) # it's good branches only
        emitReset(buf, ref, mark)

    if cfg.tagFilter:
        exportTags(buf, branchesToExport, repo, cfg)

    # write out buffer with all stuff in it
    writeBuffer(buf)

def collateNewHistory(branches, allRevs, repo, cfg):
    branches = chunkify(branches, 1000)
    goodBranches = []
    toExport = dict()
    while branches:
        chunk = branches.pop()
        heads = [x[0] for x in chunk]
        x = newRevs(heads, toExport, allRevs, repo, cfg)
        if x:
            toExport.update(x)
            goodBranches.extend(chunk)
        else:
            if len(chunk) == 1:
                log("WARN: skipping {0} -- invalid history", chunk[0][1])
            else:
                log("WARN: it seems there is a bad branch in there, subdividing")
                branches.extend(chunkify(chunk, len(chunk)/3))
    return goodBranches, toExport

def newRevs(revids, toExport, allRevs, repo, cfg):
    toExport = dict(toExport) # temp copy in case stumble onto missing revision
    while revids:
        revids = filter(lambda r: not(r in toExport or cfg.getMark(r)), set(revids))
        if any(map(lambda r: r not in allRevs, revids)):
            return None
        revs = repo.get_revisions(revids)
        toExport.update([(r.revision_id, r.parent_ids) for r in revs])
        revids = [x for r in revs for x in r.parent_ids]
    return toExport

def exportCommit(revid, ref, repo, cfg):
    # buffer up commit details, so that we can write out file blobs
    # before actual commit goes on the wire
    buf = []

    rev = repo.get_revision(revid)
    parents = remove_duplicates(rev.parent_ids)
    if len(parents) == 0:
        parentRev = revision.NULL_REVISION
        emitReset(buf, ref, None)
    else:
        parentRev = parents[0]

    debug(buf, cfg, 'committer: {0}\n', rev.committer.encode('utf8'))

    thisMark = cfg.newMark(revid)
    parentsMarks = map(cfg.getMark, parents)
    assert(all(parentsMarks)) # FIXME: check that all parents are present in marks
    emitCommitHeader(buf, ref, thisMark, rev, parentsMarks)
    oldTree, newTree = map(repo.revision_tree, [parentRev, revid])
    exportTreeChanges(buf, oldTree, newTree, cfg)
    for path, data in cfg.injects.iteritems():
        emitInjectedFile(buf, path, data, cfg)
    out(buf, '\n')
    writeBuffer(buf)

def exportTreeChanges(buf, oldTree, newTree, cfg):
    # In general case exporting changes from bzr is highly nontrivial bzr is tracking each
    # file & directory by internal ids. If you want to preserve history correctly you are
    # forced to do some very messy voodoo to correctly order and emit
    # renames/deletes/modifications

    # However I'm exporting stuff to git which is much simpler -- I can simply delete all
    # modified paths and export them afresh. This is a bit slower, but much easier to
    # implement correctly. There is one complication though -- my bzr repo has empty dirs
    # in the history, and I want to preserve them in the export by emitting .keepme files
    # where appropriate. To correctly track them during deletes/renames I need to make
    # sure I correctly issue whole-dir commands.

    # The resulting algorithm is following:
    # * rewrite the list of changes between two trees in terms of simple delete/create
    # commands and split them into 4 groups:
    #    delete/create directories & delete/create(modify) files
    # * simplify the resulting list of operations by taking into account subdirectory
    # operations. E.g. we don't need to separately delete nested files/subdirs if parent
    # directory is to be deleted. Same with creation.
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
        debug(buf, cfg, 'change: {0}\n', c)
        if c[1][0] is None:  # stuff added
            assert(c[6][0] is None)
            assert(c[1][1] is not None and c[6][1] is not None)
            addNew(c)
        elif c[1][1] is None: # removed
            assert(c[6][1] is None)
            assert(c[1][0] is not None and c[6][0] is not None)
            addDel(c)
        else: # changed or moved
            # files changed in-place don't have to be deleted, everything else becomes
            # delete + new item
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
    debug(buf, cfg, 'delDirs: {0}\n# delFiles: {1}\n# newDirs: {2}\n# newFiles: {3}\n', delDirs, delFiles, newDirs, newFiles)

    keepmes = set()
    def emitDeleteAndKeepme(buf, path):
        # check if we need to emit placeholder to keep parent dir alive
        parent = path.rpartition('/')[0]
        if parent != '' and parent not in keepmes and emptyDir(parent, cfg, newTree):
            keepmes.add(parent)
            emitPlaceholder(buf, parent)
        emitDelete(buf, path)

    # and finally -- write out resulting changes
    keepmes = set()
    for d in delDirs:
        if d == '':
            emitDeleteAll(buf)
        else:
            emitDeleteAndKeepme(buf, d)
    for f in delFiles:
        emitDeleteAndKeepme(buf, f[0])
    for d in newDirs:
        exportSubTree(buf, d, newTree, cfg)
    for f in newFiles:
        emitFile(buf, f[0], f[1], f[2], newTree, cfg)

def exportSubTree(buf, path, tree, cfg):
    for item in tree.walkdirs(prefix=path):
        # we need to update item[1] in-place to prevent it from walking into excluded subdirs
        excludes = filter(lambda x: x[0] in cfg.excludedFiles, item[1])
        for x in excludes:
            item[1].remove(x)
        if len(item[1]) == 0:
            # empty dir -- write placeholder to keep it alive
            emitPlaceholder(buf, item[0][0])
        else:
            for obj in item[1]:
                # obj is (relpath, basename, kind, lstat?, file_id, versioned_kind)
                if not obj[2]: # I don't think this might ever happen, but....
                    log("WARN: empty kind when walking tree of {0}: subdir: {1}", tree.get_revision_id(), item[0][0])
                    continue
                elif obj[2] == 'directory':
                    # skip dir entries -- we will step into them later
                    continue
                else:
                    emitFile(buf, obj[0], obj[2], obj[4], tree, cfg)

def exportTags(buf, branches, repo, cfg):
    branches = filter(lambda b: b[2].supports_tags(), branches)

    # build a list of tags we want to export
    tagList = []
    for _, _, b, branchName in branches:
        for tagName, revid in b.tags.get_tag_dict().iteritems():
            newName = cfg.tagFilter(branchName, tagName)
            if newName:
                tagList.append((sanitizeRefName(newName.encode('utf8')), revid, tagName, branchName))

    # drop tags pointing to non-exported revisions
    tagList, skips = split(lambda t: cfg.getMark(t[1]), tagList)
    for _, revId, tagName, branchName in skips:
        log('WARN: skipping tag {0} from {1}: unknown revision {2}', tagName, branchName, revId)

    # generate mark ids for tags
    tagList = map(lambda t: list(t) + ['TAG-{0}-{1}'.format(t[0], t[1])], tagList)

    # drop tags which we have exported before
    tagList = filter(lambda t: cfg.getMark(t[4]) is None, tagList)

    # now tricky part -- in bzr tags are per-branch
    # we need to filter out tags which are different between branches
    tags = dict()
    bads = set()
    for t in tagList:
        tagRef, revId, tagName, branchName, tagId = t
        if tagRef in bads:
            # we already know its bad
            continue
        if tagRef not in tags:
            tags[tagRef] = t
        elif tags[tagRef][1] != revId:
            log('WARN: skipping tag {0}: not consistent between {1} and {2}', tagRef, branchName, tags[tagRef][3])
            del tags[tagRef]
            bads.add(tagRef)

    # and finally export them
    log("Exporting {0} tags", len(tags))
    for tagRef, revId, tagName, branchRef, tagId in tags.itervalues():
        m = cfg.getMark(revId)
        cfg.newMark(tagId) # to prevent it being re-exported on the next run
        emitTag(buf, tagRef, m, repo.get_revision(revId))

def emitReset(buf, ref, mark):
    if mark is not None:
        out(buf, 'reset {0}\nfrom {1}\n\n', ref, mark)
    else:
        out(buf, 'reset {0}\n\n', ref)

def emitTag(buf, ref, mark, revobj):
    out(buf, 'tag {0}\nfrom {1}\n', ref, mark)
    out(buf, 'tagger bzr-export <> {0}\ndata 0\n\n', formatTimestamp(revobj.timestamp, revobj.timezone))

def emitCommitHeader(buf, ref, mark, revobj, parents):
    committer = revobj.committer
    authors = revobj.get_apparent_authors()
    if len(authors) > 1:
        log("WARN: commit {0} has {1} authors. Dropping all but first one", revobj.revision_id, len(authors))
    assert(len(authors) > 0)
    author = authors[0]
    out(buf, 'commit {0}\nmark {1}\n', ref, mark)
    if author != committer and len(author) != 0:
        out(buf, 'author {0} {1}\n', formatName(author), formatTimestamp(revobj.timestamp, revobj.timezone))
    out(buf, 'committer {0} {1}\n', formatName(committer), formatTimestamp(revobj.timestamp, revobj.timezone))
    msg = revobj.message.encode('utf8')
    msg += '\n\nBazaar: revid:%s' % revobj.revision_id
    out(buf, 'data {0}\n{1:s}\n', len(msg), msg)
    if len(parents) != 0:
        fmt = map(lambda i: 'merge {'+str(i)+'}\n', range(1, len(parents)))
        fmt = "".join(['from {0}\n'] + fmt)
        out(buf, fmt, *parents)

def emitFile(buf, path, kind, fileId, tree, cfg):
    if kind == 'file':
        mode = '644'
        if tree.is_executable(fileId):
            mode = '755'
        sha = tree.get_file_sha1(fileId)
        assert(sha is not None and sha != '')
        # Caching files in presence of editing is a bit tricky. We need to make sure that
        # we don't use edited content where original file should go nor mix up edited
        # content from different edit commands.
        # What we do -- to cached edited file we store it under a key which includes both
        # the hash of the original file and the hash of the editing command we use.
        # This relies on editing being stable.
        editCmd = cfg.edits.get(path, None)
        if editCmd:
            hasher = hashlib.sha1()
            hasher.update(editCmd)
            sha = 'EDITED-' + sha + '-' + hasher.hexdigest()
        mark = cfg.getMark(sha)
        if mark is None:
            mark = cfg.newMark(sha)
            data = tree.get_file_text(fileId)
            if editCmd:
                data = editFile(editCmd, data)
            writeOut('blob\nmark {0}\ndata {1}\n{2}\n'.format(mark, len(data), data))
            cfg.stats.exportFile(len(data))
        else:
            cfg.stats.skipFile()
        out(buf, 'M {0} {1} {2}\n', mode, mark, formatPath(path))
    elif kind == 'symlink':
        mode = '120000'
        data = tree.get_symlink_target(fileId)
        out(buf, 'M {0} inline {1}\ndata {2}\n{3}\n', mode, formatPath(path), len(data), data)
    else:
        log("WARN: unsupported file kind '{0}' in {1} path {2}", kind, tree.get_revision_id(), formatPath(path))
        return

def emitInjectedFile(buf, path, data, cfg):
    mark = cfg.getMark(data[0])
    if mark is None:
        mark = cfg.newMark(data[0])
        writeOut('blob\nmark {0}\ndata {1}\n{2}\n'.format(mark, len(data[1]), data[1]))
    out(buf, 'M {0} {1} {2}\n', '644', mark, formatPath(path))

def emitPlaceholder(buf, path):
    out(buf, 'M 644 inline {0}\ndata 0\n', formatPath(path + '/.keepme'))

def emitDelete(buf, path):
    out(buf, 'D {0}\n', formatPath(path))

def emitDeleteAll(buf):
    out(buf, 'deleteall\n')

def emptyDir(path, cfg, tree):
    for x in tree.walkdirs(prefix=path):
        # filter out excluded entries and check if it's empty
        r = filter(lambda t: t[0] not in cfg.excludedFiles, x[1])
        return len(r) == 0

def editFile(command, data):
    t = tempfile.NamedTemporaryFile(mode='wb', delete=False)
    t.file.write(data)
    t.close()
    ret = subprocess.call(command.format(t.name), shell=True, stdout=sys.stderr)
    if ret != 0:
        err('Command {0!r} exited with error code {1}; leaving temporary file in place', command.format(t.name), ret)
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

emailRe = re.compile(r'[<>@]')
def formatName(name):
    if emailRe.search(name):
        name, mail = email.utils.parseaddr(name.encode('utf8'))
        return '%s <%s>' % (name, mail)
    else:
        return '%s <>' % name.encode('utf8')

def formatPath(path):
    assert(path is not None)
    assert(path != '')
    assert(path[0] != '/')
    if path[0] == '"' or '\n' in path:
        path = path.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
        return '"%s"' % path.encode('utf8')
    return path.encode('utf8')

# stolen from bzr fast-export
def sanitizeRefName(refName):
    """Rewrite branchName so that it will be accepted by git-fast-import. For the detailed
    rules see check_ref_format.

    By rewriting the refname we are breaking uniqueness guarantees provided by bzr so we
    have to manually verify that resulting ref names are unique.

    http://www.kernel.org/pub/software/scm/git/docs/git-check-ref-format.html
    """
    refName = re.sub(
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
        "_", refName)
    return refName

def formatBranchName(name):
    if len(name) > 0:
        return 'refs/heads/' + sanitizeRefName(name.encode('utf8'))
    else:
        # bzr shared repos are pretty stupid in this respect
        # you CAN create a branch (especially if it's bare) at the root of the shared repo
        return 'refs/heads/__repo_root'

def writeBuffer(buf):
    writeOut(''.join(buf))

def writeOut(data):
    sys.stdout.write(data)

def log(f, *args):
    ts = time.strftime('%H:%M:%S ')
    sys.stderr.write((ts + str(f) + '\n').format(*args))

def err(f, *args):
    log('ERROR: ' + str(f), *args)
    sys.exit(1)

def out(buf, f, *args):
    buf.append(f.format(*args))

def debug(buf, cfg, f, *args):
    if cfg.debug:
        out(buf, "# " + f, *args)

def chunkify(l, size):
    size = max(size, 1)
    r = []
    while len(l) != 0:
        r.append(l[:size])
        l = l[size:]
    return r

def split(fcn, l):
    a, b = [], []
    for x in l:
        if fcn(x):
            a.append(x)
        else:
            b.append(x)
    return a, b

def remove_duplicates(l):
    res = l[:1]
    for x in l[1:]:
        if x != res[-1]:
            res.append(x)
    return res

def readFile(fname):
    with open(fname) as f:
        return f.read()

def readCfg(fname):
    return eval(readFile(fname))

def prof():
    import cProfile
    f = open("/dev/null", "w")
    save = sys.stdout
    try:
        sys.stdout = f
        cProfile.run('main(["/home/usov/build/testrepo/docky/trunk"])', "prof")
    finally:
        sys.stdout = save

class Config(object):
    """Misc stuff here -- various config flags & marks management"""

    def __init__(self):
        self.debug = False
        self.edits = dict()
        self.excludedFiles = set()
        self.forceAll = False
        self.fname = None
        self.injects = dict()
        self.refName = None
        self.stats = None
        self.tagFilter = None

        self.exportList = Matcher(True)
        self.skipList = Matcher(False)

        self.nextMark = 1
        self.marks = dict()

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
                    log("ERROR: this doesn't looks like marks string: {0}", l.strip())
                    continue
                maxMark = max(maxMark, int(s[0][1:]))
                self.marks[s[1]] = s[0]
            f.close()
            self.nextMark = maxMark + 1
            log("Marks loaded: {0} entries", len(self.marks))
        else:
            # try creating empty file, just to make sure we can write there
            f = open(fname, "w")
            f.close()

    def save(self):
        if self.fname is None:
            return
        f = open(self.fname, "w")
        # save them sorted
        marks = list(self.marks.iteritems())
        marks.sort(key=lambda m: int(m[1][1:]))
        for r, m in marks:
            f.write('%s %s\n' % (m, r))
        f.close()

    def newMark(self, revid):
        """Add new revision to the set and return its mark"""
        m = ':{0}'.format(self.nextMark)
        self.marks[revid] = m
        self.nextMark += 1
        return m

    def getMark(self, revid):
        """Get a mark corresponding to revid. Returns None if revision isn't marked"""
        return self.marks.get(revid, None)

class Matcher(object):
    # helper for managing include/exclude rules
    def __init__(self, default):
        self.patterns = []
        self.default = default

    def add(self, regexp):
        self.patterns.append(re.compile(regexp))

    def m(self, s):
        if self.patterns:
            return any((p.search(s) for p in self.patterns))
        else:
            return self.default

class Stats(object):
    def __init__(self):
        # stats for this export
        self._skippedRevs = 0
        self._exportedRevs = 0
        self._skippedFiles = 0
        self._exportedFiles = 0
        self._bytes = 0
        self._starttime = time.time()

    def __str__(self):
        dur = time.time() - self._starttime
        s = "{0}(+{1}) revs, {2}(+{3}) files, {4} Mb in {5}; {6} revs/min {7} Mb/min"
        s = s.format(self._exportedRevs, self._skippedRevs,
                     self._exportedFiles, self._skippedFiles,
                     self._bytes/1024/1024,
                     datetime.timedelta(seconds = dur),
                     int(self._exportedRevs/(dur/60)),
                     int(self._bytes/1024/1024/(dur/60)))
        return s

    def skipRev(self):
        self._skippedRevs += 1

    def exportRev(self):
        self._exportedRevs += 1

    def skipFile(self):
        self._skippedFiles += 1

    def exportFile(self, size):
        self._exportedFiles += 1
        self._bytes += size

if __name__ == '__main__':
    main(sys.argv[1:])

