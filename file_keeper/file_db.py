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
Dev = namedtuple("Dev", "dev_num dev_path mount uuid")

def get_devs():
    """get Devs"""
    proc = Popen(['df'], stdout=PIPE)
    devs, _ = proc.communicate()
    devs = [i.split() for i in devs.split('\n') if i.startswith('/')]
    devs = [i for i in devs if i[-1].startswith('/')]
    dev2uuid = {}
    for uuid in os.listdir('/dev/disk/by-uuid/'):
        dev2uuid['/dev/'+os.readlink('/dev/disk/by-uuid/'+uuid).split('/')[-1]] = uuid
    Devs = []
    for dev in devs:
        try:
            stat = os.stat(dev[-1])
        except OSError:
            stat = None
            logging.warning("Couldn't stat '%s'" % dev[-1])
        if stat is not None:
            Devs.append(Dev(
                dev_num = stat.st_dev,
                dev_path = dev[0],
                mount = [-1],
                uuid = dev2uuid[dev[0]]
            ))
    return Devs

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
         help="<|help|>"
     )
     parser.add_argument('<|positional(s)|>', type=str, nargs='+',
         help="<|help|>"
     )

     return parser

def main():
    print(json.dumps(stat_devs()[1]))

if __name__ == '__main__':
    main()
