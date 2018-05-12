"""
file_db.py - manage file db

Terry Brown, terrynbrown@gmail.com, Sun Apr 22 16:36:40 2018
"""

import argparse
import os
import sqlite3

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
    if opt.path != can_path(opt.path):
        print("%s -> %s" % (opt.path, can_path(opt.path)))
        opt.path = can_path(opt.path)
    opt.stat = os.stat(opt.path)

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

def get(opt, table, keyvals):
    q = "select {table} from {table} where {vals}".format(
        table=table, vals=' and '.join('%s=?' % k for k in keyvals))
    opt.cur.execute(q, list(keyvals.values()))
    res = opt.cur.fetchall()
    if res:
        return res[0][0]
    else:
        return None

def get_or_make(opt, table, keyvals):
    res = get(opt, table, keyvals)
    if res:
        return res
    else:
        opt.cur.execute('insert into {table} ({fields}) values ({values})'.format(
            table=table, fields=','.join(keyvals), values=','.join('?'*len(keyvals))),
            list(keyvals.values()))
        return get(opt, table, keyvals)

def proc_file(opt, dev, filepath):
    if not os.path.isfile(filepath):
        return
    stat = os.stat(filepath)

def proc_dev(opt, dev):
    print("{part} ({label}, {uuid}) on {mntpnt}".format(**dev))
    base = os.path.relpath(opt.path, start=dev.mntpnt)
    assert os.path.join(dev.mntpnt, base) == opt.path
    print(base)
    opt.uuid = get_or_make(opt, 'uuid', {'uuid_text': dev.uuid})
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


if __name__ == '__main__':
    main()
