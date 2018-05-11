"""
dupedb.py - scan drives for duplicates files

Terry Brown, Terry_N_Brown@yahoo.com, Wed Aug 20 12:32:14 2014
"""

import os
import argparse
import sqlite3

from drivelayout import dsz, stat_devs

"""-- SCHEMA

create table device (
    device INTEGER PRIMARY KEY,
    uuid text,
    mntpnt text,
    label text,
    size text
);
create unique index device_device_idx on device(device);

create table file (
    file INTEGER PRIMARY KEY,
    device SIGNED BIG INT,
    path text,
    bytes SIGNED BIG INT,
    hash text
);
create unique index file_file_idx on file(file);
create index file_bytes_idx on file(bytes);
create index file_hash_idx on file(hash);

"""
def log_file(con, cur, file_path, stat, device):
    """
    log_file - add a file to the db

    :Parameters:
    - `con`: db connection
    - `cur`: db cursor
    - `file_path`: path to file
    - `stat`: os.stat()
    - `device`: device table ROWID
    """

    query = """insert into file (device, path, bytes)
        values (:device, :path, :bytes)"""
    parameters = {'device': device, 'path': file_path,
        'bytes': stat.st_size}
    cur.execute(query, parameters)
    con.commit()


def main():
    opt = make_parser().parse_args()

    devs, mntpnts = stat_devs()

    con = sqlite3.connect("dupedb.sqlite3")
    cur = con.cursor()

    scan_path = os.path.realpath(os.path.abspath(opt.path))
    print("Scanning %s" % scan_path)

    path_dev = os.stat(scan_path).st_dev

    for partition, mntpnt in mntpnts.items():
        if os.stat(mntpnt).st_dev == path_dev:
            break
    else:
        print("Can't find mountpoint for path")
        exit(1)

    print("Mounted on %s %s" % (partition, mntpnt))

    for dev, partitions in devs.items():
        if partition in partitions:
            uuid = partitions[partition]['UUID']
            label = partitions[partition].get('LABEL')
            break
    else:
        print("Can't find UUID for partition")
        exit(1)
    print("label:%s uuid:%s" % (label, uuid))

    query = "select device from device where uuid=:uuid"
    parameters = {'uuid': uuid, 'mntpnt': mntpnt, 'label': label}
    cur.execute(query, parameters)
    res = cur.fetchall()
    if len(res) > 1:
        raise Exception("More than one entry for UUID "+uuid)
    if not res:
        cur.execute("""insert into device (uuid, mntpnt, label)
            values (:uuid, :mntpnt, :label)""", parameters)
        con.commit()
        cur.execute(query, parameters)
        res = cur.fetchall()
    device = res[0][0]

    scanned = 0
    large = 0
    bytes = 0
    for path, dirs, files in os.walk(scan_path):
        if scanned % 1000:
            print("files:%s large:%s scanned:%s" %
                (scanned, large, dsz(bytes)))
        for filename in files:
            file_path = os.path.join(path, filename)
            stat = os.stat(file_path)
            bytes += stat.st_size
            scanned += 1
            if stat.st_size >= opt.min_size:
                large += 1
                log_file(con, cur, file_path, stat, device)

def make_parser():

     parser = argparse.ArgumentParser(
         description="""general description""",
         formatter_class=argparse.ArgumentDefaultsHelpFormatter
     )

     parser.add_argument("--min-size", type=int, default=10*1024*1024,
         help="minimum size (bytes) to scan"
     )
     parser.add_argument('path', type=str,
         help="path to scan"
     )

     return parser
if __name__ == '__main__':
    main()

