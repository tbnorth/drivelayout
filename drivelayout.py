"""Drive layout analysis
"""

# drivelayout.py $Id$
# Author: Terry Brown
# Created: Sun May 25 2008

import glob
import os
import subprocess
import time
import sys
import optparse

TMPMP = "/mnt/drive-test-temp"  # temporary mount point for testing
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

    if mp != TMPMP:
        return True  # weak but sufficient

    return os.stat(TMPMP).st_dev != os.stat(os.path.dirname(TMPMP)).st_dev
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
def runCmd(s):
    """run command in string s using subprocess.Popen()
    """

    proc = subprocess.Popen(s.split(), stderr=subprocess.PIPE)
    proc.wait()

def makeParser():
    """return the OptionParser for this app."""

    parser = optparse.OptionParser()
    parser.add_option("--ls",
                  action="store_true", default=False,
                  help="list one line of files from unmounted devices")
    return parser
def main():

    parser = makeParser()
    opt, arg = parser.parse_args()

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
        for item in info.split():
            ikey, val = item.split('=')
            parts[key][ikey] = val.strip('"')

        # get size info for partition
        sz = sizeof(key)
        if sz:
            parts[key]['SIZE'] = dsz(sz)
        else:
            parts[key]['SIZE'] = 'N/A'

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
        if not os.isatty(sys.stdout.fileno()):
            d = l = f = ''
        for part in parts:
            if 'SEC_TYPE' in devs[dev][part]:
                del devs[dev][part]['SEC_TYPE']
            print '   ',os.path.basename(part),
            print ' '.join([d+k+':'+l+str(devs[dev][part][k]) 
                            for k in sorted(devs[dev][part].keys())])
            if (opt.ls and devLines
                and part not in mntpnt
                and devs[dev][part].get('TYPE') != 'swap'):
                try:
                    runCmd('mkdir -p '+TMPMP)
                    runCmd('umount '+TMPMP)
                    time.sleep(1)
                    runCmd('mount %s %s' % (part, TMPMP))
                    time.sleep(1)
                    mntpnt[part] = TMPMP
                except:
                    pass
            if devLines and part in mntpnt and is_mounted(mntpnt[part]):
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
    try:
        print("\nLVM info (check VFree):")
        runCmd('vgs')  # to show unallocated LVM space 
    except OSError:
        print("none found")  # not installed?
if __name__ == '__main__':
    main()
