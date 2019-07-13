PRAGMA foreign_keys = ON;

create table hash (    -- hashes
    hash INTEGER PRIMARY KEY,
    hash_text text     -- text of hash
    -- length integer     -- amount of data read for hash
);
create index idx_hash_text on hash (hash_text); -- , length);
create table uuid (    -- uuids
    uuid INTEGER PRIMARY KEY,
    uuid_text text,    -- text of uuid
    -- following from lsblk etc. help to describe drive to user
    part_size text,    -- partition size, text
    drive_size text,   -- drive size, text
    label text,        -- label
    model text,        -- drive model
    serial text        -- drive serial
);
create index idx_uuid_text on uuid (uuid_text);
create table file (    -- files
    file INTEGER PRIMARY KEY,
    uuid integer,      -- uuid of drive (partition, but not partuuid)
    path text,         -- path to file
    st_ino integer,    -- inode of file
    st_size integer,   -- size of file
    st_mtime integer,  -- modification time of file
    hash text,         -- file's hash
    hash_date integer, -- date on which the file had that hash
    FOREIGN KEY(uuid) REFERENCES uuid(uuid)
);
create index idx_file_path on file(path);
create index idx_file_size on file(st_size);
create table file_hash (    -- hashes for a file, M2M
    file_hash INTEGER PRIMARY KEY,
    file integer,
    hash integer,
    hash_date integer, -- date on which the file had that size / hash
    st_size integer,   -- size of file, userful even without hash
    FOREIGN KEY(file) REFERENCES file(file),
    FOREIGN KEY(hash) REFERENCES hash(hash)
);
create index idx_file_hash_file on file_hash (file);
create index idx_file_hash_hash on file_hash (hash);
create index idx_file_hash_date on file_hash (hash_date);
