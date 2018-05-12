"""Drive layout analysis
"""

# drivelayout.py $Id$
# Author: Terry Brown
# Created: Sun May 25 2008

from __future__ import print_function

import glob
import os
import subprocess
import time
import shlex
import socket
import sys
import time
import optparse

from xml.etree import ElementTree as ET

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
    d = '\033[32m'
    l = '\033[39m'
    f = '\033[31m'
    bh = '\033[44m'
    bl = '\033[40m'
    # bh = '\033[33m'
    # bl = l

    # bh = '\033[48;2;40;40;40m'
    if not opt.color and not os.isatty(sys.stdout.fileno()):
        d = l = f = bh = bl = ''
    else:
        print(l)

    for dev in sorted(devs.keys()):
        # get size info for whole device
        sz = sizeof(dev)
        parts = sorted(devs[dev].keys())
        devs[dev]['_:SIZE'] = dsz(sz) if sz else '???'
        if len(parts) == 1:
            # LVM lv names ending in digits, see dev = key.strip('0123456789') above
            devname = parts[0]
        else:
            devname = dev
        print("%s%s %s%s" % (bh, devname, dsz(sz) if sz else 'NO INFO.', bl))
        for part in parts:
            if 'SEC_TYPE' in devs[dev][part]:
                del devs[dev][part]['SEC_TYPE']
            print(part, end= ' ') # '   ',os.path.basename(part),
            print(' '.join([d+k+':'+l+str(devs[dev][part][k])
                            for k in sorted(devs[dev][part].keys())]))
            if (opt.ls #X and devLines
                and part not in mntpnt
                and devs[dev][part].get('TYPE') not in ('swap', 'LVM2_member', None)):
                try:
                    MP = TMPMP
                    if opt.mount_all:
                        if devs[dev][part].get('LABEL'):
                            MP = "/mnt/%s" % devs[dev][part]['LABEL']
                        else:
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

                devs[dev][part]['ON'] = mntpnt[part]
                print(d+'          ON:'+l,mntpnt[part], end= ' ')
                devs[dev][part]['FREE'] = '%s %d%%' % (dsz(stat.f_bsize*stat.f_bavail),
                                          int(stat.f_bavail*100/stat.f_blocks))
                print(d+' FREE:'+l+devs[dev][part]['FREE'], end= ' ')
                devs[dev][part]['RESV'] = dsz(stat.f_bsize*(stat.f_bfree-stat.f_bavail))
                print(d+' RESV:'+l+devs[dev][part]['RESV'])
                files = [os.path.basename(i)[:10]
                         for i in glob.glob(os.path.join(mntpnt[part],'*'))]
                if files:
                    files.sort()
                    devs[dev][part]['FILES'] = files
                    print(f+'         ',' '.join(files)[:70]+l)
                release = os.path.join(mntpnt[part], 'etc/lsb-release')
                if os.path.isfile(release):
                    for line in open(release):
                        if 'DISTRIB_DESCRIPTION' in line:
                            print('         ', line.strip())
                            devs[dev][part]['DISTRIB_DESCRIPTION'] = line.strip().split('=', 1)[-1]

    runCmd('umount '+TMPMP)

def desc_devs_summary(opt, devs, mntpnt):

    summary = []  # pass on to save_opml

    # LVM summary
    try:
        head = "LVM info (check VFree):"
        print("\n"+head)
        sys.stdout.flush()
        out, err = runCmd('vgs', return_data=True)  # to show unallocated LVM space
        print(out)
        summary = [head, out]
    except OSError:
        print("none found")  # not installed?
        summary = [head, "none found"]

    # device summary
    for dev in sorted(devs.keys()):
        parts = sorted(i for i in devs[dev] if not i.startswith('_:'))
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
        labels = ', '.join([i or '???' for i in labels])
        if sz:
            text = "%s %s %s" % (dev.replace('/dev/', ''), sz, labels)
        else:
            text = "%s %s %s" % (dev.replace('/dev/', ''), '(size?)', labels)

        print(text)
        summary.append(text)

    devs["_:SUMMARY"] = '\n'.join(summary)

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

    if opt.opml:
        save_opml(opt, devs, mntpnt)

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
    parser.add_option("--opml", type=str,
                  help="save output in OPML format")
    return parser

def runCmd(s, return_data=False):
    """run command in string s using subprocess.Popen()
    """

    if return_data:
        proc = subprocess.Popen(s.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return proc.communicate()
    else:
        proc = subprocess.Popen(s.split(), stderr=subprocess.PIPE)
        proc.wait()

def save_opml(opt, devs, mntpnt):
    """save_opml - save drives in OPML format for outliner import

    :param optparse Namespace opt: options
    :param dict devs: see desc_devs(()
    :param dict mntpnt: parition to mount point mapping

    """

    LEO = "leo:com:leo-opml-version-1"
    BODY = "{%s}body" % LEO

    def kv(d, k):
        if k in d:
            return "%s: %s " % (k, d[k])
        else:
            return ''

    ET.register_namespace('leo', LEO)
    opml = ET.Element("opml", version="2.0")
    head = ET.SubElement(opml, "head")
    title = "Drives on %s %s" % (socket.gethostname(), time.asctime())
    ET.SubElement(head, "title").text = title
    body = ET.SubElement(opml, "body")
    top = ET.SubElement(body, "outline")
    top.set("text", title)
    ET.SubElement(top, BODY).text = devs['_:SUMMARY']
    for dev_name in sorted([i for i in devs if not i.startswith("_:")]):
        dev = ET.SubElement(top, "outline")
        dev.set("text", "%s %s" % (dev_name, devs[dev_name]['_:SIZE']))
        for part_name in sorted(i for i in devs[dev_name] if not i.startswith('_:')):
            part = ET.SubElement(dev, "outline")
            part_data = devs[dev_name][part_name]
            part.set("text", ' '.join(i for i in [part_name,
                part_data.get('LABEL'), part_data.get('ON'), part_data.get('SIZE'),
                part_data.get('TYPE')] if i and i != TMPMP))
            text = "%s\n%s\n%s\n\n%s\n" % (
                kv(part_data, 'LABEL')+kv(part_data, 'SIZE')+kv(part_data, 'TYPE')+kv(part_data, 'UUID'),
                kv(part_data, 'ON')+kv(part_data, 'FREE')+kv(part_data, 'RESV'),
                kv(part_data, 'DISTRIB_DESCRIPTION'),
                ' '.join(part_data.get('FILES', [])),
            )
            ET.SubElement(part, BODY).text = text.replace('\n\n\n', '\n\n')
            for attr_name in sorted(part_data):
                attr = ET.SubElement(part, 'outline')
                text="%s: %s" % (attr_name, part_data[attr_name])
                if attr_name == 'FILES':
                    text="%s: %s" % (attr_name, ' '.join(part_data[attr_name][:10]))
                attr.set('text', text)

    ET.ElementTree(opml).write(opt.opml)

"""
    sde1 SIZE:44 Gb TYPE:ext4 UUID:0737f555-dccf-4196-8dc3-6c15c041a303
         ON: /  FREE:8 Gb 20%  RESV:2304 Mb
         LIQUID_ASS bin boot cdrom dev etc home initrd.img initrd.img lib lib32
         DISTRIB_DESCRIPTION="Ubuntu 14.04.3 LTS"
"""

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
    mntLines = [i.decode('utf-8').strip() for i in proc.stdout.readlines()]
    mntpnt = {}
    for line in mntLines:
        line, _ = line.rsplit(' type ', 1)
        line = line.split(None, 2)
        if line[0] not in mntpnt:
            mntpnt[line[0]] = line[2]

    # get info about all partitions
    proc = subprocess.Popen('blkid', stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    proc.wait()
    devLines = [i.decode('utf-8').strip() for i in proc.stdout.readlines()]
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

