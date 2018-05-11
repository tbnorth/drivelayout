"""
file_db.py - manage file db

Terry Brown, terrynbrown@gmail.com, Sun Apr 22 16:36:40 2018
"""

import argparse
import json
import os
import logging
from collections import namedtuple
from subprocess import Popen, PIPE

from drivelayout import dsz, stat_devs

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

     parser.add_argument("--foo", action='store_true',
         help=""
     )
     parser.add_argument('', type=str, nargs='+',
         help=""
     )

     return parser

def stat_devs_list():
    """Make sorted list of mounted partitions from stat_devs() output"""
    devs, mntpnts = stat_devs()
    ans = []
    for dev in sorted(devs):
        for part in sorted(devs[dev]):
            d = devs[dev][part]
            d['DEV'] = dev
            d['PART'] = part
            if part in mntpnts:
                d['MNTPNT'] = mntpnts[part]
                ans.append(d)
    return ans

def main():
    print(json.dumps(stat_devs_list(), sort_keys=True, indent=4))

if __name__ == '__main__':
    main()
