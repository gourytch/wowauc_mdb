#! /usr/bin/env python2.7
# -*- coding: utf8 -*-
#
__author__ = 'gour'

import subprocess
import os
import os.path
import glob
import re
import datetime


def zip(srcdir, dstdir, grouping):
    """
    :param srcdir: str
    :param dstdir: str
    :param grouping: str
    :return: None
    """
    print("srcdir   : {}".format(srcdir))
    print("dstdir   : {}".format(dstdir))
    print("grouping : {}".format(grouping))
    print("---vvv---vvv---vvv---vvv---vvv---vvv---vvv---vvv---vvv---vvv---")
    # сперва соберём все имена и раскидаем по группам
    groups = {}
    splitter = re.compile(r'^(\d{4})(\d{2})(\d{2})_\d{6}_.._[^/]+\.json$');
    for fullname in sorted(glob.glob(srcdir + '/' + '*.json')):
        fname = os.path.basename(fullname)
        r = splitter.match(fname)
        if not r:
            print("filename {} not matched".format(fname))
            continue
        part_year = r.group(1)
        part_month = r.group(2)
        part_day = r.group(3)
        part_week = 'w' + str(1 + ((int(part_day) - 1) / 7))
        #print("{} y{} m{} d{} {}".format(fname, part_year, part_month, part_day, part_week))
        if grouping == 'daily':
            group = part_year + part_month + part_day
        elif grouping == 'weekly':
            group = part_year + part_month + part_week
        elif grouping == 'monthly':
            group = part_year + part_month
        else:
            print("bad grouping: {}".format(grouping))
            sys.exit(1)
        if group not in groups:
            groups[group] = []
        groups[group].append(fname)
    # теперь группы набраны, упаковываем их
    groupkeys = sorted(groups.keys())
    total = len(groupkeys)
    if total < 2:
        print("nothing to process")
        return
    last_group = groupkeys[-1]
    groupkeys = groupkeys[:-1]
    total = total - 1
    cur = 0
    for groupname in groupkeys: # do not process last group
        cur = cur + 1
        groupfiles = groups[groupname]
        dstname = dstdir + "/" + groupname + '.tar.xz'
        tmpname = dstname + '.tmp'
        if os.path.exists(dstname):
            print("destination file exists: {}".format(dstname))
            continue
        if os.path.exists(tmpname):
            print("destination file exists: {}".format(tmpname))
            continue
        cmd = ["tar", "cf", tmpname, "--xz", "-C", srcdir]
        cmd.extend(groupfiles)
        print("[{}/{}] group {} with {} elements".format(cur, total, groupname, len(groupfiles)))
        #print("call: {}".format(' '.join(cmd)))
        t1 = datetime.datetime.now()
        try:
            retval = subprocess.call(cmd)
        except:
            os.remove(tmpname)
            print("\n! Exception caught. removed: {}".format(tmpname))
            sys.exit(1)
        t2 = datetime.datetime.now()
        s = (t2 - t1).total_seconds()
        h, s = divmod(s, 3600)
        m, s = divmod(s, 60)
        print("retval: %d, elapsed time: %02d:%02d:%02d" % (retval, h, m, s))
        if retval == 0:
            print("archive created: {}".format(dstname))
            os.rename(tmpname, dstname)
            for f in groupfiles:
                os.remove(srcdir + '/' + f)
                #print("   source file removed: {}".format(f))
            print("source files removed")
        else:
            os.remove(tmpname)
            print("execution terminated.")
            sys.exit(1)
    print("---^^^---^^^---^^^---^^^---^^^---^^^---^^^---^^^---^^^---^^^---")
    print("the last group remains unzipped: {}".format(last_group))
    print("done.")
    return


if __name__ == '__main__':
    import sys
    if len(sys.argv) not in (3, 4):
        print("use {0} srcdir dstdir [daily|weekly|monthly]".format(sys.argv[0]))
        sys.exit(1)
    srcdir = sys.argv[1]
    dstdir = sys.argv[2]
    grouping = sys.argv[3] if 3 < len(sys.argv) else 'daily'
    if not os.path.exists(srcdir) \
    or not os.path.isdir(srcdir):
        print("srcdir {} not found or not a directory".format(srcdir))
        sys.exit(1)
    srcdir = os.path.realpath(srcdir)
    if not os.path.exists(dstdir) \
    or not os.path.isdir(dstdir):
        print("dstdir {} not found or not a directory".format(dstdir))
        sys.exit(1)
    dstdir = os.path.realpath(dstdir)
    if grouping is not None \
    and grouping not in ('daily', 'weekly', 'monthly'):
        print("grouping {} not recognized".format(grouping))
        sys.exit(1)
    zip(srcdir, dstdir, grouping)
