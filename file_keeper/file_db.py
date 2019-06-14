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

from humanread import hr


class FileKeeperError(Exception):
    pass


# field names matching os.stat() attributes
STATFLDS = 'st_size', 'st_mtime', 'st_ino'

BLKSIZE = 100000000  # amount to read when hashing files

if sys.version_info < (3, 6):
    # need dict insertion order
    print("file_db.py requires Python >= 3.6")
    FileNotFoundError = IOError  # for linters
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
        "--dupes-only",
        action='store_true',
        help="Update hashes for possible dupes only",
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
    parser.add_argument(
        "--accept-current",
        action='store_true',
        help="Accept current files as correct",
    )

    return parser


def list_files(opt):
    """List files in DB

    Args:
        opt (argparse Namespace): options
    """
    for path in do_query(opt, "select * from file order by st_size desc"):
        print(path)


def dupe_check(opt, todo):
    lists = defaultdict(list)
    for rec in todo:
        if rec.hash:
            lists[rec.hash].append(rec)
        else:
            lists['NOHASH'].append(rec)
    for hash_text, list_ in lists.items():
        if len(list_) > 1:
            print("\n%s %s" % (hash_text, hr(list_[0].st_size)))
            inos = defaultdict(list)
            for rec in list_:
                inos[(rec.uuid, rec.st_ino)].append(rec)
            for ino in inos.values():
                link = len(ino) > 1
                if link:
                    print("  %s" % ((ino[0].uuid, ino[0].st_ino),))
                for rec in ino:
                    print("  %s%s" % ('  ' if link else '', rec.path))


def list_dupes(opt):

    size = None
    todo = []
    for path in do_query(opt, "select * from file order by st_size desc"):
        if size != path.st_size:
            if todo:
                dupe_check(opt, todo)
            size = path.st_size
            todo = [path]
        else:
            todo.append(path)


def can_path(path):
    """Return canonical path"""
    return os.path.abspath(os.path.realpath(os.path.expanduser(path)))


def do_query(opt, q, vals=None):
    select = q.lower().strip().startswith('select')
    if opt.dry_run and not select:
        return
    try:
        opt.cur.execute(q, vals or [])
    except Exception:
        print(q)
        print(vals)
        raise
    if not select:
        return None
    res = opt.cur.fetchall()
    flds = [i[0] for i in opt.cur.description]
    if False:
        print(q)
        print(vals)
        print(res)
        print([Dict(zip(flds, i)) for i in res])
    # this can consume a lot of RAM, but avoids blocking DB calls
    # i.e. making other queries while still consuming this result
    return [Dict(zip(flds, i)) for i in res]


def do_one(opt, q, vals=None):
    """Run a query expected to create a single record response"""
    ans = do_query(opt, q, vals=vals)
    if ans is None or len(ans) != 1:
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


def get_pk(opt, table, ident, return_obj=False, multi=False):

    q = "select {table} from {table} where {vals}".format(
        table=table, vals=' and '.join('%s=?' % k for k in ident)
    )
    if return_obj:
        q = q.replace(table, '*', 1)  # replace first <table>
    res = do_query(opt, q, list(ident.values()))

    if len(res) > 1 and not multi:
        raise Exception("More than on result for %s %s" % (table, ident))
    if res:
        if multi:
            if return_obj:
                return res
            else:
                return [i[table] for i in res]
        else:
            if return_obj:
                return res[0]
            else:
                return res[0][table]
    else:
        return None


def get_rec(opt, table, ident, multi=False):
    return get_pk(opt, table, ident, return_obj=True, multi=multi)


def get_or_make_pk(opt, table, ident, defaults=None, return_obj=False):
    res = get_pk(opt, table, ident, return_obj=return_obj)
    if res:
        return res, False
    else:
        defaults = defaults.copy() if defaults else dict()
        defaults.update(ident)
        do_query(
            opt,
            'insert into {table} ({fields}) values ({values})'.format(
                table=table,
                fields=','.join(defaults),
                values=','.join('?' * len(defaults)),
            ),
            list(defaults.values()),
        )
        return get_pk(opt, table, defaults, return_obj=return_obj), True


def get_pks(opt, table, ident, return_obj=False):
    return get_pk(opt, table, ident, return_obj=return_obj, multi=True)


def get_or_make_rec(opt, table, ident, defaults=None):
    return get_or_make_pk(
        opt, table, ident, defaults=defaults, return_obj=True
    )


def get_recs(opt, table, ident):
    return get_rec(opt, table, ident, multi=True)


def hash_path(path, callback=None):
    """hash_path - hash a file path

    Args:
        path (str): path to file
    Returns:
        str: hex hash for file
    """
    ans = sha1()
    count = 0
    with open(path, 'rb') as data:
        while True:
            block = data.read(BLKSIZE)
            ans.update(block)
            if len(block) != BLKSIZE:
                break
            count += 1
            if callback:
                callback(count * BLKSIZE)

    return ans.hexdigest()


def proc_file(opt, dev, filepath):
    if os.path.islink(filepath):
        opt.n['sym. links (ignored)'] += 1
        return
    if not os.path.exists(filepath):
        print(filepath, 'not found')
        opt.n['offline/deleted'] += 1
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
        # old/new pairs for size / mtime / inode
        stats = [(k, getattr(file_rec, k), getattr(stat, k)) for k in STATFLDS]
        # name/old/new tuples for changed values, e.g. 'size',234,345
        changes = [i for i in stats if i[1] != i[2]]
        if changes:
            opt.n['changed_stat'] += 1
            print(
                "%s changed (%s)"
                % (filepath, ', '.join('%s:%s→%s' % i for i in changes))
            )
            if opt.accept_current:
                for k in STATFLDS:
                    setattr(file_rec, k, getattr(stat, k))
                save_rec(opt, file_rec)
        else:
            opt.n['unchanged_stat'] += 1
    else:
        opt.n['new'] += 1


# ## def proc_dev(opt, dev):
# ##     dev.setdefault('label', '???')
# ##     print("{part} ({label}, {uuid}) on {mntpnt}".format(**dev))
# ##     opt.base = os.path.relpath(opt.path, start=dev.mntpnt)
# ##     opt.mntpnt = dev.mntpnt
# ##     assert os.path.join(dev.mntpnt, opt.base) == opt.path


def proc_dev(opt, uuid):
    dev = opt.mntpnts[uuid]
    dev.setdefault('label', '???')
    print("{name} ({label}, {uuid}) on {mountpoint}".format(**dev))
    opt.base = os.path.relpath(opt.path, start=dev.mountpoint)
    opt.mntpnt = dev.mountpoint
    assert os.path.join(dev.mountpoint, opt.base).rstrip('./') == opt.path, (
        os.path.join(dev.mountpoint, opt.base),
        opt.path,
        opt.mntpnt,
    )

    print(opt.base)
    opt.uuid, new = get_or_make_pk(opt, 'uuid', {'uuid_text': dev.uuid})
    assert opt.uuid, opt.uuid
    c = 0
    for path, dirs, files in os.walk(opt.path):
        for filename in files:
            proc_file(opt, dev, os.path.join(path, filename))
            c += 1


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

    for action in ['list_dupes', 'list_files', 'update_hashes']:
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

    opt.con.commit()

    show_stats(opt)


def get_or_make_db(opt):
    exists = os.path.exists(opt.db_file)
    if not exists and opt.dry_run:
        raise FileKeeperError(
            "--dry-run: can't create database, no file '%s'" % opt.db_file
        )
    if opt.dry_run:
        con = sqlite3.connect("file:%s?mode=ro" % opt.db_file, uri=True)
    else:
        con = sqlite3.connect(opt.db_file)
    if not exists:
        for cmd in open('file_db.sql').read().split(';\n'):
            con.execute(cmd)  # can't use do_query here, that's OK
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
    do_query(opt, q, [i[1] for i in vals])


def show_stats(opt):
    opt.n['run_time'] = time.time() - opt.n['run_time']
    opt.n['stat_per_sec'] = int(opt.n['stated'] / opt.n['run_time'])
    width = max(len(i) for i in opt.n)  # max. key length
    for k, v in opt.n.items():
        print("%s: %s" % ((' ' * width + k)[-width:], v))


def update_hashes(opt):
    """update_hashes - update hashes, oldest first

    Maybe hash files where more than one of st_size exists.

    Args:
        opt (argparse namespace): options
    """
    q = """
create temp view if not exists up_hash as select *
  from file join uuid using (uuid)
"""
    if opt.dupes_only:
        q += """
       join (select st_size as class,
                    count(*) as ccount from file group by st_size) as x
            on (st_size = class)"""
    q += "\nwhere (%s-hash_date > %s or hash is null)" % (
        # float()/int() here are redundant, but eliminate SQL injection
        float(time.time()), 24 * 60 * 60 * int(opt.max_hash_age))
    if opt.dupes_only:
        q += " and x.ccount > 1"

    do_query(opt, q)

    q_summary = "select count(*) as count, sum(st_size) as total from up_hash"
    count = do_one(opt, q_summary)
    count, total = count.count, count.total

    print("%s hashes to update" % count)

    at_once = 300
    # do this in blocks of `at_once` working backwards because
    # relying on updating of hashes to remove them from the todo
    # list (working blockwise *forward* through the list) won't
    # work when files are deleted / on unmounted drives.
    offset = count // at_once

    done = 0  # count of files done
    read = 0  # total bytes read
    safe = 0  # bytes read since last commit
    start = time.time()
    prog = 0  # time of last progress message
    while offset >= 0:
        todo = do_query(opt, "select * from up_hash limit ? offset ?",
            [
                at_once,
                offset * at_once,
            ],
        )
        offset -= 1
        for rec in todo:
            now = time.time()
            if now - prog > 5:  # every 5 seconds
                print(
                    "{}/{} ({}/{}, {:.2f}%, "
                    "{:.1f}/{:.1f} min., {}/s)".format(
                        done,
                        count,
                        hr(read),
                        hr(total),
                        read / total * 100,
                        (now - start) / 60,
                        (now - start) / 60 * (total / read) if read else -1,
                        hr(int(read / (now - start))),
                    )
                )
                prog = now
            try:
                if rec.st_size > 1000000000:

                    def cb(done, total=rec.st_size):
                        print(
                            "(%s file, %.1f%%)\r"
                            % (hr(rec.st_size), done / total * 100),
                            end='',
                        )

                    cb(0)
                else:
                    cb = None

                hash_text = hash_path(
                    os.path.join(
                        opt.mntpnts[rec.uuid_text].mountpoint, rec.path
                    ),
                    callback=cb,
                )
                if cb:
                    cb(rec.st_size)  # show 100%
                    print()
                rec.hash = hash_text
                del rec['uuid_text']
                del rec['class']
                del rec['ccount']
                save_rec(opt, rec)
                done += 1
                read += rec.st_size
                safe += rec.st_size
                if safe > 1000000000:  # commit every GB read
                    opt.con.commit()
                    safe = 0
            except FileNotFoundError:
                print(rec.path, 'not found')
                opt.n['offline/deleted'] += 1
                pass
        opt.con.commit()


if __name__ == '__main__':
    main()
