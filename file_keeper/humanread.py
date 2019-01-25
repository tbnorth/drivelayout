"""Return human readable form of x
"""

# humanread.py $Id$
# Author: Terry Brown
# Created: Tue Jul 24 2007


def hr(x, base=1024.0):
    """Return human readable form of x"""
    ans = float(x)
    if ans == 1.0:
        return '1 byte'
    sizes = 'bytes', 'kb', 'Mb', 'Gb', 'Tb', 'Pb', 'Eb', 'Zb', 'Yb', 'bytes'
    # kilo Mega Giga Tera Peta Exa Zetta Yotta
    for i, s in enumerate(sizes):
        if ans < base:
            break
        ans = ans / base
    ans = ('%.2f' % ans).rstrip('.0')
    if i > 0 and s == 'bytes':  # ran out of prefixes
        ans = str(x)
    return '%s %s' % (ans, s)


def test_hr(base=1024):
    for i in range(1, 30):
        print(hr('1' * i, base))


if __name__ == '__main__':
    test_hr()
