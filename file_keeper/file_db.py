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

from collections import defaultdict, namedtuple
from hashlib import sha1
from subprocess import Popen, PIPE

from addict import Dict
from drivelayout import stat_devs  # FIXME: replace with lsblk wrapper

if sys.version_info[0] < 3:
    FileNotFoundError = IOError


class FileKeeperError(Exception):
    pass


# field names matching os.stat() attributes
STATFLDS = 'st_size', 'st_mtime', 'st_ino'

BLKSIZE = 10000000  # amount to read when hashing files

if sys.version_info < (3, 6):
    # need dict insertion order
    print("file_db.py requires Python >= 3.6")
    exit(10)


def sqlite_types(dbfile):
    """Return dict of namedtuples for a sqlite3 DB"""
    con = sqlite3.connect(dbfile)
    cur = con.cursor()
    q = "select name from sqlite_master where type='table'"
    tables = [i[0] for i in cur.execute(q)]
    ans = {}
    for table in tables:
        cur.execute("select * from %s limit 0" % table)
        ans[table] = namedtuple(table, [i[0] for i in cur.description])
    return ans


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
    if opt.path:
        canonical = can_path(opt.path)
        if opt.path != canonical:
            print("%s -> %s" % (opt.path, canonical))
            opt.path = canonical
        opt.stat = os.stat(opt.path)

    opt.run_time = int(time.time())

    return opt


def make_parser():

    parser = argparse.ArgumentParser(
        description="""general description""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # settings
    parser.add_argument(
        "--db-file", default='file_keeper.db', help="Path to DB file"
    )
    parser.add_argument("--path", help="Path to process")
    parser.add_argument(
        "--min-size",
        type=int,
        default=1000000,
        help="Minimum file size to process",
        metavar='BYTES',
    )
    parser.add_argument(
        "--max-hash-age",
        type=int,
        default=30,
        help="Re-hash files with hashes older than DAYS",
        metavar='DAYS',
    )

    parser.add_argument(
        "--dry-run", action='store_true', help="Make new changes to DB"
    )

    # actions

    parser.add_argument(
        "--update-hashes", action='store_true', help="Update hashes for files"
    )
    parser.add_argument("--list-files", action='store_true', help="List files")
    parser.add_argument(
        "--list-dupes", action='store_true', help="List duplicates"
    )

    return parser


def get_files(opt):
    """Iterate files in DB

    Args:
        opt (argparse Namespace): options
    """
    q = ["select * from file order by st_size desc"]
    for res in opt.cur.execute(' '.join(q)):
        yield res


def list_files(opt):
    """List files in DB

    Args:
        opt (argparse Namespace): options
    """
    # X type_ = sqlite_types(opt.db_file)
    for path in do_query(opt, "select * from file order by st_size desc"):
        # X path = type_['file']._make(path)
        print(path)


def dupe_check(todo):
    return


def list_dupes(opt):

    size = None
    todo = []
    for path in do_query(opt, "select * from file order by st_size desc"):
        if size != path.st_size:
            if todo:
                dupe_check(todo)
            size = path.st_size
            todo = [path]
        else:
            todo.append(path)


def can_path(path):
    """Return canonical path"""
    return os.path.abspath(os.path.realpath(os.path.expanduser(path)))


def do_query(opt, q, vals=None):
    try:
        opt.cur.execute(q, vals or [])
    except Exception:
        print(q)
        raise
    res = opt.cur.fetchall()
    # this can consume a lot of RAM, but avoids blocking DB calls
    # i.e. making other queries while still consuming this result
    return [Dict(zip([i[0] for i in opt.cur.description], i)) for i in res]


def stat_devs_list():
    """Make sorted list of mounted partitions from stat_devs() output"""
    devs, mntpnts = stat_devs()
    ans = []
    for dev in sorted(devs):
        for part in sorted(devs[dev]):
            d = Dict((k.lower(), v) for k, v in devs[dev][part].items())
            d.dev = dev
            d.part = part
            if part in mntpnts:
                d.mntpnt = mntpnts[part]
                d.stat = os.stat(d.mntpnt)
                ans.append(d)
    return ans


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
        table=table, vals=' and '.join('%s=?' % k for k in ident)
    )
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
        opt.cur.execute(
            'insert into {table} ({fields}) values ({values})'.format(
                table=table,
                fields=','.join(defaults),
                values=','.join('?' * len(defaults)),
            ),
            list(defaults.values()),
        )
        return get_pk(opt, table, defaults, return_obj=return_obj), True


def get_or_make_rec(opt, table, ident, defaults=None):
    return get_or_make_pk(
        opt, table, ident, defaults=defaults, return_obj=True
    )


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
    file_rec, new = get_or_make_rec(
        opt,
        'file',
        ident=dict(
            uuid=opt.uuid, path=os.path.relpath(filepath, start=opt.mntpnt)
        ),
        defaults=dict(
            st_ino=stat.st_ino, st_size=stat.st_size, st_mtime=stat.st_mtime
        ),
    )

    if not new:
        changes = [
            k for k in STATFLDS if getattr(file_rec, k) != getattr(stat, k)
        ]
        if changes:
            opt.n['changed_stat'] += 1
            print("%s changed (%s)" % (filepath, ', '.join(changes)))
            for k in STATFLDS:
                setattr(file_rec, k, getattr(stat, k))
            save_rec(opt, file_rec)
        else:
            opt.n['unchanged_stat'] += 1
    else:
        opt.n['new'] += 1

    file_hash, new = get_or_make_rec(
        opt,
        'file_hash',
        ident=dict(file=file_rec.file),
        defaults=dict(st_size=stat.st_size, hash_date=opt.run_time),
    )


def proc_dev(opt, dev):
    dev.setdefault('label', '???')
    print("{part} ({label}, {uuid}) on {mntpnt}".format(**dev))
    opt.base = os.path.relpath(opt.path, start=dev.mntpnt)
    opt.mntpnt = dev.mntpnt
    assert os.path.join(dev.mntpnt, opt.base) == opt.path
    print(opt.base)
    opt.uuid, new = get_or_make_pk(opt, 'uuid', {'uuid_text': dev.uuid})
    c = 0
    for path, dirs, files in os.walk(opt.path):
        for filename in files:
            proc_file(opt, dev, os.path.join(path, filename))
            c += 1


def main():

    opt = get_options()
    opt.con, opt.cur = get_or_make_db(opt)
    opt.n = defaultdict(lambda: 0)
    opt.n['run_time'] = time.time()
    opt.dev = get_devs()
    opt.mntpnts = {}

    def mntpnts(nodes, d):
        for node in nodes:
            if node.get('uuid'):
                d[node['uuid']] = node.get('mountpoint')
            mntpnts(node.get('children', []), d)

    mntpnts(opt.dev["blockdevices"], opt.mntpnts)

    for action in ['list_dupes', 'list_files', 'update_hashes']:
        if getattr(opt, action):
            globals()[action](opt)
            return

    for dev in stat_devs_list():
        if opt.stat.st_dev == dev.stat.st_dev:
            proc_dev(opt, dev)
            break
    else:
        raise Exception("No device for path")

    show_stats(opt)


def get_or_make_db(opt):
    exists = os.path.exists(opt.db_file)
    if not exists and opt.dry_run:
        raise FileKeeperError(
            "--dry-run: can't create database, no file '%s'" % opt.db_file
        )
    if opt.dry_run:
        con = sqlite3.connect("file:%s?mode=ro" % opt.db_file)
    else:
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
        table=table, pk=pk, values=','.join('%s=?' % i[0] for i in vals)
    )
    try:
        # if not opt.dry_run:
        opt.cur.execute(q, [i[1] for i in vals])
    except Exception:
        print(q)
        print([i[1] for i in vals])
        raise


def show_stats(opt):
    opt.n['run_time'] = time.time() - opt.n['run_time']
    opt.n['stat_per_sec'] = int(opt.n['stated'] / opt.n['run_time'])
    width = max(len(i) for i in opt.n)  # max. key length
    for k, v in opt.n.items():
        print("%s: %s" % ((' ' * width + k)[-width:], v))

    opt.con.commit()


def update_hashes(opt):
    """update_hashes - update hashes, oldest first

    Args:
        opt (argparse namespace): options
    """

    def get_todo():
        """Get unhashed files in blocks of 1000"""
        return do_query(
            opt,
            """
select *
  from file join uuid using (uuid)
 where ?-hash_date > ? or hash is null
 order by hash is null desc, hash_date
 limit 1000
""",
            [time.time(), 24 * 60 * 60 * opt.max_hash_age],
        )

    todo = get_todo()

    while todo:
        rec = todo.pop(0)
        try:
            rec.hash = hash_path(
                os.path.join(opt.mntpnts[rec.uuid_text], rec.path)
            )
            del rec['uuid_text']
            save_rec(opt, rec)
        except FileNotFoundError:
            pass
        if not todo:
            opt.con.commit()
            todo = get_todo()
    opt.con.commit()


if __name__ == '__main__':
    main()
