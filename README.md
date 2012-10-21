bzr-export
==========

**bzr-export** is a small tool to export history from Bazaar repository into fast-import format.

Q&A
---
* Why not use bzr-fastimport?  
  It's buggy. Try exporting [this repo](https://code.launchpad.net/~a-s-usov/+junk/badCommit1) or
  [this one](https://code.launchpad.net/~a-s-usov/+junk/badCommit2) with bzr-fastexport and see for yourself.

* Why write a new tool instead of fixing bzr-fastimport?  
  First bzr-fastimport isn't actively maintained anymore. Some of my patches have been stuck 
  in launchpad for months.  
  Then there is a question of complexity -- one of the goals of bzr-fastexport is to be able to round-trip 
  history back into bzr. And because Bazaar uses internal ids to track history of each file & directory it's
  extremely difficult to export non-trivial commits so that they produce correct history when round-tripped.  
  I don't need any of this madness because I'm exporting into Git.

* I need feature X.  
  Patches are welcome.
