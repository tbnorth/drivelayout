# file_keeper

Utilities for keeping an archive of unique files.

## Assumptions / methods

 - a file has not changed if mtime, inode, and size are all the same
 - eventual full hashing, but truncated hashes in the interim
   - no, premature optimization
 - paths are always canonicalized, but...
 - use partition UUID / path relative to mount point?
