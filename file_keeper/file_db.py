"""
file_db.py - manage file db

Terry Brown, terrynbrown@gmail.com, Sun Apr 22 16:36:40 2018
"""

import argparse
import os
import sqlite3
import time

from addict import Dict
from drivelayout import stat_devs # dsz

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
         formatter_class=argparse.ArgumentDefaultsHelpFormatter
     )

     parser.add_argument("--foo", action='store_true',
         help="xxx"
     )
     parser.add_argument("--db-file", default='file_keeper.db',
         help="Path to DB file"
     )
     parser.add_argument("--path",
         help="Path to process"
     )
     parser.add_argument("--min-size", type=int, default=1000000,
         help="Minimum file size to process"
     )

     return parser

def can_path(path):
    """Return canonical path"""
    return os.path.abspath(os.path.realpath(os.path.expanduser(path)))

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

def get_pk(opt, table, ident, return_obj=False):
    q = "select {table} from {table} where {vals}".format(
        table=table, vals=' and '.join('%s=?' % k for k in ident))
    if return_obj:
        q = q.replace('{table}', '*', 1)  # replace first {table}
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
def proc_file(opt, dev, filepath):
    if not os.path.isfile(filepath):
        return
    stat = os.stat(filepath)
    file_pk, new = get_or_make_pk(opt, 'file',
        ident=dict(
            uuid=opt.uuid,
            path=os.path.relpath(filepath, start=opt.base)
        ), defaults=dict(
            inode=stat.st_ino,
            size=stat.st_size,
            mtime=stat.st_mtime,
        )
    )

    file_rec, new = get_or_make_rec(opt, 'file_hash',
        ident=dict(file=file_pk),
        defaults=dict(
            size=stat.st_size,
            date=opt.run_time
        )
    )
def proc_dev(opt, dev):
    print("{part} ({label}, {uuid}) on {mntpnt}".format(**dev))
    opt.base = os.path.relpath(opt.path, start=dev.mntpnt)
    assert os.path.join(dev.mntpnt, opt.base) == opt.path
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
    opt.con = sqlite3.connect(opt.db_file)
    opt.cur = opt.con.cursor()
    for dev in stat_devs_list():
        if opt.stat.st_dev == dev.stat.st_dev:
            proc_dev(opt, dev)
            break
    else:
        raise Exception("No device for path")

    opt.con.commit()

if __name__ == '__main__':
    main()
