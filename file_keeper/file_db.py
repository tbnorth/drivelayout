"""
file_db.py - manage file db

Terry Brown, terrynbrown@gmail.com, Sun Apr 22 16:36:40 2018
"""

import argparse
import json
import os
import sqlite3
import sys
import time

from collections import defaultdict
from hashlib import sha1
from subprocess import Popen, PIPE

from addict import Dict

# field names to os.stat() attributes
FLD2STAT = (('size', 'st_size'), ('mtime', 'st_mtime'), ('inode', 'st_ino'))

BLKSIZE = 10000000  # amount to read when hashing files

if sys.version_info < (3, 6):
    # need dict insertion order
    print("file_db.py requires Python >= 3.6")
    exit(10)
def get_options(args=None):
    """
    get_options - use argparse to parse args, and return a
    argparse.Namespace, possibly with some changes / expansions /
    validatations.

    Client code should call this method with args as per sys.argv[1:],
    rather than calling make_parser() directly.

    :param [str] args: arguments to parse
    :return: options with modifications / validations
    :rtype: argparse.Namespace
    """
    opt = make_parser().parse_args(args)

    # modifications / validations go here

    return opt

def make_parser():

    parser = argparse.ArgumentParser(
        description="""general description""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # settings
    parser.add_argument("--db-file", default='file_keeper.db',
        help="Path to DB file"
    )
    parser.add_argument("--path",
        help="Path to process"
    )
    parser.add_argument("--min-size", type=int, default=1000000,
        help="Minimum file size to process", metavar='BYTES'
    )
    parser.add_argument("--max-hash-age", type=int, default=30,
        help="Re-hash files with hashes older than DAYS", metavar='DAYS'
    )

    #actions

    parser.add_argument("--update-hashes", action='store_true',
        help="Update hashes for files"
    )


    return parser

def can_path(path):
    """Return canonical path"""
    return os.path.abspath(os.path.realpath(os.path.expanduser(path)))

def do_query(opt, q, vals=None):
    opt.cur.execute(q, vals or [])
    res = opt.cur.fetchall()
    return [
        Dict(zip([i[0] for i in opt.cur.description], i))
        for i in res
    ]

def do_one(opt, q, vals=None):
    ans = do_query(opt, q, vals=vals)
    if len(ans) != 1:
        raise Exception("'%s' did not produce a single record response" % q)
    return ans[0]
def get_devs():
    """get_devs - get output from `lsblk`

    Returns:
        Dict: lsblk output
    """
    cmd = Popen(['lsblk', '--json', '--output-all'], stdout=PIPE)
    out, err = cmd.communicate()
    return Dict(json.loads(out))
def get_pk(opt, table, ident, return_obj=False):
    q = "select {table} from {table} where {vals}".format(
        table=table, vals=' and '.join('%s=?' % k for k in ident))
    if return_obj:
        q = q.replace(table, '*', 1)  # replace first <table>
    opt.cur.execute(q, list(ident.values()))
    res = opt.cur.fetchall()
    if len(res) > 1:
        raise Exception("More than on result for %s %s" % (table, ident))
    if res:
        if return_obj:
            return Dict(zip([i[0] for i in opt.cur.description], res[0]))
        else:
            return res[0][0]
    else:
        return None

def get_rec(opt, table, ident):
    return get_pk(opt, table, ident, return_obj=True)
def get_or_make_pk(opt, table, ident, defaults=None, return_obj=False):
    res = get_pk(opt, table, ident, return_obj=return_obj)
    if res:
        return res, False
    else:
        defaults = defaults.copy() if defaults else dict()
        defaults.update(ident)
        opt.cur.execute('insert into {table} ({fields}) values ({values})'.format(
            table=table, fields=','.join(defaults), values=','.join('?'*len(defaults))),
            list(defaults.values()))
        return get_pk(opt, table, defaults, return_obj=return_obj), True

def get_or_make_rec(opt, table, ident, defaults=None):
    return get_or_make_pk(opt, table, ident, defaults=defaults, return_obj=True)
def hash_path(path):
    """hash_path - hash a file path

    Args:
        path (str): path to file
    Returns:
        str: hex hash for file
    """
    ans = sha1()
    with open(path, 'rb') as data:
        while True:
            block = data.read(BLKSIZE)
            ans.update(block)
            if len(block) != BLKSIZE:
                break

    return ans.hexdigest()
def proc_file(opt, dev, filepath):
    if not os.path.isfile(filepath):
        return
    stat = os.stat(filepath)
    opt.n['stated'] += 1
    file_rec, new = get_or_make_rec(opt, 'file',
        ident=dict(
            uuid=opt.uuid,
            path=os.path.relpath(filepath, start=opt.mntpnt)
        ), defaults=dict(
            inode=stat.st_ino,
            size=stat.st_size,
            mtime=stat.st_mtime,
        )
    )

    if not new:
        changes = [
            k for k,v in FLD2STAT
            if getattr(file_rec, k) != getattr(stat, v)
        ]
        if changes:
            opt.n['changed_stat'] += 1
            print("%s changed (%s)" % (filepath, ', '.join(changes)))
            for k,v in FLD2STAT:
                setattr(file_rec, k, getattr(stat, v))
            save_rec(opt, file_rec)
        else:
            opt.n['unchanged_stat'] += 1
    else:
        opt.n['new'] += 1

    file_hash, new = get_or_make_rec(opt, 'file_hash',
        ident=dict(file=file_rec.file),
        defaults=dict(
            size=stat.st_size,
            date=opt.run_time
        )
    )
def proc_dev(opt, uuid):
    dev = opt.mntpnts[uuid]
    dev.setdefault('label', '???')
    print("{name} ({label}, {uuid}) on {mountpoint}".format(**dev))
    opt.base = os.path.relpath(opt.path, start=dev.mountpoint)
    opt.mntpnt = dev.mountpoint
    assert os.path.join(dev.mountpoint, opt.base) == opt.path
    print(opt.base)
    opt.uuid, new = get_or_make_pk(opt, 'uuid', {'uuid_text': dev.uuid})
    c = 0
    for path, dirs, files in os.walk(opt.path):
        for filename in files:
            proc_file(opt, dev, os.path.join(path, filename))
            c += 1
    print(c)

def main():

    opt = get_options()
    if opt.path:
        canonical = can_path(opt.path)
        if opt.path != canonical:
            print("%s -> %s" % (opt.path, canonical))
            opt.path = canonical
        opt.stat = os.stat(opt.path)
    opt.run_time = int(time.time())
    opt.con, opt.cur = get_or_make_db(opt)
    opt.n = defaultdict(lambda: 0)
    opt.n['run_time'] = time.time()
    opt.dev = get_devs()
    opt.mntpnts = {}
    def mntpnts(nodes, d):
        for node in nodes:
            if node.get('uuid'):
                d[node['uuid']] = Dict()
                d[node['uuid']].update(node)
            mntpnts(node.get('children', []), d)
    mntpnts(opt.dev["blockdevices"], opt.mntpnts)

    for action in ['update_hashes']:
        if getattr(opt, action):
            globals()[action](opt)
            return

    majmin = '%s:%s' % (os.major(opt.stat.st_dev), os.minor(opt.stat.st_dev))
    for uuid in opt.mntpnts:
        if opt.mntpnts[uuid]['maj:min'] == majmin:
            proc_dev(opt, uuid)
            break
    else:
        raise Exception("No device for path %s" % opt.path)

    show_stats(opt)
def get_or_make_db(opt):
    exists = os.path.exists(opt.db_file)
    con = sqlite3.connect(opt.db_file)
    if not exists:
        for cmd in open('file_db.sql').read().split(';\n'):
            con.execute(cmd)
    cur = con.cursor()
    return con, cur
def save_rec(opt, rec):
    """save_rec - save a modified record

    Args:
        opt (argparse namespace): options
        rec (Dict): record
    """
    table = list(rec.keys())[0]
    pk = rec[table]
    vals = [(k, v) for k, v in rec.items() if k != table]
    q = 'update {table} set {values} where {table} = {pk}'.format(
        table=table, pk=pk,
        values=','.join('%s=?' % i[0] for i in vals)
    )
    try:
        opt.cur.execute(q, [i[1] for i in vals])
    except:
        print(q)
        print([i[1] for i in vals])
        raise
def show_stats(opt):
    opt.n['run_time'] = time.time() - opt.n['run_time']
    opt.n['stat_per_sec'] = int(opt.n['stated'] / opt.n['run_time'])
    width = max(len(i) for i in opt.n)  # max. key length
    for k, v in opt.n.items():
        print("%s: %s" % ((' '*width+k)[-width:], v))

    opt.con.commit()

def update_hashes(opt):
    """update_hashes - update hashes, oldest first

    Args:
        opt (argparse namespace): options
    """

    count = do_one(opt, """
select count(*) as count
  from file join file_hash using (file) join uuid using (uuid)
       left join hash using (hash)
 where ?-date > ? or hash is null
""", [time.time(), 24*60*60*opt.max_hash_age]).count

    print("%s hashes to update" % count)

    at_once = 3
    # do this in blocks of `at_once` working backwards because
    # relying on updating of hashes to remove them from the todo
    # list (working blockwise *forward* through the list) won't
    # work when files are deleted / on unmounted drives.
    offset = count // at_once

    while offset >= 0:
        todo = do_query(opt, """
select * from file join file_hash using (file) join uuid using (uuid)
       left join hash using (hash)
 where ?-date > ? or hash is null
 order by hash is null desc, date
 limit ? offset ?
""", [time.time(), 24*60*60*opt.max_hash_age, at_once, offset*at_once])
        offset -= 1
        for rec in todo:
            try:
                hash_text = hash_path(os.path.join(opt.mntpnts[rec.uuid_text].mountpoint, rec.path))
                hash_pk, new = get_or_make_pk(opt, 'hash', {'hash_text': hash_text})
                file_hash, new = get_or_make_rec(opt, 'file_hash', {'file_hash': rec.file_hash})
                assert not new
                file_hash.hash = hash_pk
                file_hash.date = opt.run_time
                save_rec(opt, file_hash)
            except FileNotFoundError:
                print(rec.path, 'not found')
                opt.n['offline/deleted'] += 1
                pass
        print(len(todo))
        opt.con.commit()
if __name__ == '__main__':
    main()
