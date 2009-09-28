"""
"""

# drivelayout.py $Id$
# Author: Terry Brown
# Created: Sun May 25 2008

import glob
import os
import subprocess
import time
import sys

def dsz(x):
    u = 0
    while x >= 1024:
        x /= 1024
        u += 1
    return "%d %s" % (int(x), ['b','k','Mb','Gb','Tb','Pb'][u])

def runCmd(s):
    proc = subprocess.Popen(s.split(), stderr=subprocess.PIPE)
    proc.wait()

def main():
    proc = subprocess.Popen('mount', stdout=subprocess.PIPE)
    proc.wait()
    mntLines = [i.strip() for i in proc.stdout.readlines()]
    mntpnt = {}
    for line in mntLines:
        line = line.split()
        mntpnt[line[0]] = line[2]

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
        proc = subprocess.Popen(('fdisk', '-s', key), stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        proc.wait()
        txt = proc.stdout.read().strip()
        if not txt.strip():
            sz = 0
        else:
            sz = int(txt) / 1024
        if sz > 1023:
            sz = str(sz / 1024) + 'G'
        else:
            sz = str(sz) + 'M'
        parts[key]['SIZE'] = sz
    for dev in sorted(devs.keys()):
        proc = subprocess.Popen(('fdisk', '-l', dev), stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        proc.wait()
        devLines = [i.strip() for i in proc.stdout.readlines()]
        if devLines:
            print dev, devLines[1].split()[2], devLines[1].split()[3] 
        else:
            print dev, 'NO INFO.'
        d = '\033[32m'
        l = '\033[37m'
        f = '\033[31m'
        if not os.isatty(sys.stdout.fileno()):
            d = l = f = ''
        for part in sorted(devs[dev].keys()):
            if 'SEC_TYPE' in devs[dev][part]:
                del devs[dev][part]['SEC_TYPE']
            print '   ',os.path.basename(part),
            print ' '.join([d+k+':'+l+str(devs[dev][part][k]) 
                            for k in sorted(devs[dev][part].keys())])
            if devLines and part not in mntpnt and devs[dev][part].get('TYPE') != 'swap':
                try:
                    runCmd('mkdir -p /mnt/drive-test-temp')
                    runCmd('umount /mnt/drive-test-temp')
                    time.sleep(1)
                    runCmd('mount %s /mnt/drive-test-temp' % part)
                    time.sleep(1)
                    mntpnt[part] = '/mnt/drive-test-temp'
                except:
                    pass
            if devLines and part in mntpnt:
                stat = os.statvfs(mntpnt[part])
                
                print d+'     ON:'+l,mntpnt[part],
                print d+' FREE:'+l+'%s %d%%' % (dsz(stat.f_bsize*stat.f_bavail),
                                          int(stat.f_bavail*100/stat.f_blocks)),
                print d+' RESV:'+l+dsz(stat.f_bsize*(stat.f_bfree-stat.f_bavail))
                files = [os.path.basename(i)[:10] for i in glob.glob(os.path.join(mntpnt[part],'*'))]
                files.sort()
                print f+'        ',' '.join(files)[:70]+l

    runCmd('umount /mnt/drive-test-temp')

if __name__ == '__main__':
    main()
