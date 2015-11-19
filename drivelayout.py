"""Drive layout analysis
"""

# drivelayout.py $Id$
# Author: Terry Brown
# Created: Sun May 25 2008

import glob
import os
import subprocess
import time
import shlex
import sys
import optparse

TMPMP = "/mnt/drive-test-temp"  # temporary mount point for testing

TMPMP_N = [0]
def desc_devs(opt, devs, mntpnt):
    """
    desc_devs - dump description of devices

    :Parameters:
    - `opt`: argparse options, see makeParser()
    - `devs`: {dev1: {part1: {LABEL:, SIZE:, TYPE:, UUID:},
                      part2:...},
               dev2: {part1: {LABEL:, SIZE:, TYPE:, UUID:},
                      part2:...}}
    - `mntpnt`: parition to mount point mapping
    """

    if not opt.summary:
        desc_devs_detail(opt, devs, mntpnt)
    if not opt.details:
        desc_devs_summary(opt, devs, mntpnt)

def desc_devs_detail(opt, devs, mntpnt):
    for dev in sorted(devs.keys()):
        # get size info for whole device
        sz = sizeof(dev)
        parts = sorted(devs[dev].keys())
        if len(parts) == 1:
            # LVM lv names ending in digits, see dev = key.strip('0123456789') above
            devname = parts[0]
        else:
            devname = dev
        if sz:
            print devname, dsz(sz)
        else:
            print devname, 'NO INFO.'
        d = '\033[32m'
        l = '\033[37m'
        f = '\033[31m'
        if not opt.color and not os.isatty(sys.stdout.fileno()):
            d = l = f = ''
        for part in parts:
            if 'SEC_TYPE' in devs[dev][part]:
                del devs[dev][part]['SEC_TYPE']
            print '   ',os.path.basename(part),
            print ' '.join([d+k+':'+l+str(devs[dev][part][k])
                            for k in sorted(devs[dev][part].keys())])
            if (opt.ls #X and devLines
                and part not in mntpnt
                and devs[dev][part].get('TYPE') != 'swap'):
                try:
                    MP = TMPMP
                    if opt.mount_all:
                        MP = TMPMP+str(TMPMP_N[0])
                        TMPMP_N[0] += 1
                    runCmd('mkdir -p '+MP)
                    runCmd('umount '+MP)
                    time.sleep(1)
                    runCmd('mount %s %s' % (part, MP))
                    time.sleep(1)
                    mntpnt[part] = MP
                except:
                    pass
            if part in mntpnt and is_mounted(mntpnt[part]): #X and devLines:
                stat = os.statvfs(mntpnt[part])

                print d+'         ON:'+l,mntpnt[part],
                print d+' FREE:'+l+'%s %d%%' % (dsz(stat.f_bsize*stat.f_bavail),
                                          int(stat.f_bavail*100/stat.f_blocks)),
                print d+' RESV:'+l+dsz(stat.f_bsize*(stat.f_bfree-stat.f_bavail))
                files = [os.path.basename(i)[:10]
                         for i in glob.glob(os.path.join(mntpnt[part],'*'))]
                if files:
                    files.sort()
                    print f+'        ',' '.join(files)[:70]+l
                release = os.path.join(mntpnt[part], 'etc/lsb-release')
                if os.path.isfile(release):
                    for line in open(release):
                        if 'DISTRIB_DESCRIPTION' in line:
                            print '        ', line.strip()


    runCmd('umount '+TMPMP)

def desc_devs_summary(opt, devs, mntpnt):

    # LVM summary
    try:
        print("\nLVM info (check VFree):")
        runCmd('vgs')  # to show unallocated LVM space
    except OSError:
        print("none found")  # not installed?

    print('')

    # device summary
    for dev in sorted(devs.keys()):
        parts = sorted(devs[dev].keys())
        sz = sizeof(dev)
        if not sz:
            sz = devs[dev][parts[0]].get('SIZE')
        else:
            sz = dsz(sz)
        labels = []
        for part in parts:
            labels.append(devs[dev][part].get('TYPE'))
            label = devs[dev][part].get('LABEL')
            if label:
                labels[-1] += ":"+label
        labels = ', '.join(labels)
        if sz:
            print dev.replace('/dev/', ''), sz, labels
        else:
            print dev.replace('/dev/', ''), '(size?)', labels
def dsz(x):
    """translate x bytes to '4513 Gb' type strings"""
    divisor = 1024

    u = 0
    assert ' ' not in str(x)
    x = int(x)
    while x >= 4*divisor:
        x /= divisor
        u += 1
    return "%d %s" % (int(x), ['b','kb','Mb','Gb','Tb','Pb'][u])
def is_mounted(mp):
    """is_mounted - Return True if mp is mounted and not just a mount point

    :Parameters:
    - `mp`: path to mount point
    """

    if not mp.startswith(TMPMP):
        return True  # weak but sufficient

    # FIXME broken with --mount-all, or always broken?
    return os.stat(TMPMP).st_dev != os.stat(os.path.dirname(TMPMP)).st_dev
def main():

    parser = makeParser()
    opt, arg = parser.parse_args()
    if opt.mount_all:
        opt.ls = True
    if opt.summary:
        opt.ls = False

    devs, mntpnt = stat_devs()
    desc_devs(opt, devs, mntpnt)

def makeParser():
    """return the OptionParser for this app."""

    parser = optparse.OptionParser()
    parser.add_option("--ls",
                  action="store_true", default=False,
                  help="list one line of files from unmounted devices")
    parser.add_option("--details",
                  action="store_true", default=False,
                  help="don't include summary information at end")
    parser.add_option("--summary",
                  action="store_true", default=False,
                  help="show only summary information")
    parser.add_option("--mount-all",
                  action="store_true", default=False,
                  help="leave all unmounted drives mounted for inspection")
    parser.add_option("--color",
                  action="store_true", default=False,
                  help="use color output even when redirected")
    return parser
def runCmd(s):
    """run command in string s using subprocess.Popen()
    """

    proc = subprocess.Popen(s.split(), stderr=subprocess.PIPE)
    proc.wait()

def sizeof(thing):
    """determine the size of a device or partition according to `fdisk -s`

    FIXME - hardwired 1024 byte blocks
    """

    proc = subprocess.Popen(('fdisk', '-s', thing), stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    proc.wait()
    txt = proc.stdout.read().strip()
    if not txt.strip():
        sz = None
    else:
        sz = int(txt)*1024

    return sz
def stat_devs():
    """returns devs, mntpnt - the parameters
    for desc_devs(), see desc_devs() for docs."""

    # collect list of mount points for mounted volumes
    proc = subprocess.Popen('mount', stdout=subprocess.PIPE)
    proc.wait()
    mntLines = [i.strip() for i in proc.stdout.readlines()]
    mntpnt = {}
    for line in mntLines:
        line = line.split()
        mntpnt[line[0]] = line[2]

    # get info about all partitions
    proc = subprocess.Popen('blkid', stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    proc.wait()
    devLines = [i.strip() for i in proc.stdout.readlines()]
    devs = {}
    for line in devLines:
        key, info = line.split(': ')
        dev = key.strip('0123456789')
        if dev not in devs: devs[dev] = {}
        parts = devs[dev]
        parts[key] = {}
        for item in shlex.split(info):  # LABEL="A space!"
            ikey, val = item.split('=')
            parts[key][ikey] = val.strip('"')

        # get size info for partition
        sz = sizeof(key)
        if sz:
            parts[key]['SIZE'] = dsz(sz)
        else:
            parts[key]['SIZE'] = 'N/A'

    return devs, mntpnt


if __name__ == '__main__':
    main()
