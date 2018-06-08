# file_keeper

Utilities for keeping an archive of unique files.

## Assumptions / methods

 - a file has not changed if mtime, inode, and size are all the same
 - eventual full hashing, but truncated hashes in the interim
   - no, premature optimization
 - paths are always canonicalized, but...
 - use partition UUID / path relative to mount point?

PRAGMA foreign_keys = ON;

create table hash (    -- hashes
    hash INTEGER PRIMARY KEY,
    hash_text text     -- text of hash
    -- length integer     -- amount of data read for hash
);
create index idx_hash_text on hash (hash_text); -- , length);
create table uuid (    -- uuids
    uuid INTEGER PRIMARY KEY,
    uuid_text text     -- text of uuid
);
create index idx_uuid_text on uuid (uuid_text);
create table file (    -- files
    file INTEGER PRIMARY KEY,
    uuid integer,      -- uuid of drive (partition, but not partuuid)
    path text,         -- path to file
    inode integer,     -- inode of file
    size integer,      -- size of file,
    mtime integer,  -- modification time of file
    FOREIGN KEY(uuid) REFERENCES uuid(uuid)
);
create index idx_file_path on file(path);
create index idx_file_size on file(size);
create index idx_file_dev on file(dev);
create table file_hash (    -- hashes for a file, M2M
    file integer,
    hash integer,
    date date,      -- date on which the file had that hash
    size integer,  -- size of file when hashed
    mtime integer,  -- modification time of file when hashed
    FOREIGN KEY(file) REFERENCES file(file),
    FOREIGN KEY(hash) REFERENCES hash(hash)
);
create index idx_file_hash_file on file_hash (file);
create index idx_file_hash_hash on file_hash (hash);
create index idx_file_hash_date on file_hash (date);
