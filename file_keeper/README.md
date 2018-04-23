# file_keeper

Utilities for keeping an archive of unique files.

## Assumptions / methods

 - a file has not changed if mtime, inode, and size are all the same
 - eventual full hashing, but truncated hashes in the interim
   - no, premature optimization

PRAGMA foreign_keys = ON;

create table hash (    -- hashes
    hash PRIMARY KEY,
    hash_text text,    -- text of hash
    -- length integer     -- amount of data read for hash
);
create index idx_hash_text_len on hash (hash_text); -- , length);
create table file (    -- files
    file PRIMARY KEY,
    path text,         -- path to file
    inode integer,     -- inode of file
    dev integer,       -- device of file
    size integer       -- size of file
);
create index idx_hash_path on file(path);
create index idx_hash_size on file(size);
create index idx_hash_dev on file(dev);
create table file_hash (    -- hashes for a file, M2M
    file integer,
    hash integer,
    date date,      -- date on which the file had that hash
    FOREIGN KEY(file) REFERENCES file(file),
    FOREIGN KEY(hash) REFERENCES hash(hash)
);
create index idx_file_hash_file on file_hash (file);
create index idx_file_hash_hash on file_hash (hash);
create index idx_file_hash_date on file_hash (date);
