# file_keeper

Utilities for keeping an archive of unique files.

## Assumptions / methods

 - a file has not changed if mtime, inode, and size are all the same
 - eventual full hashing, but truncated hashes in the interim
   - no, premature optimization
 - paths are always canonicalized, but...
 - use partition UUID / path relative to mount point?

## Operations

 - report stats
   - number / size of files
   - hash existence / freshness
 - report duplicates
 - scan for changes
   - by time/size/inode
   - by hash
 - scan for new files
 - accept changes to old files
 - generate hashes
   - missing hashes
   - confirm old, update datestamp
 - find duplicate directories
   - hash of hashes?
 - report deleted
   - confirm copies
 - forget deleted (copies, all)

