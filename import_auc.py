#! /usr/bin/env python
# -*- coding: utf-8 -*-
#

from wowauc.Parser import Parser
#from wowauc.Pusher_MongoDB import Pusher_MongoDB as Pusher
from wowauc.Pusher_CachedMongoDB import Pusher_CachedMongoDB as Pusher
from sys import argv
from glob import glob
from argparse import ArgumentParser
from sys import argv
import datetime

erase=False
debug=False
url='mongodb://localhost:27017/'
dirs = []
for arg in argv[1:]:
    if arg == '--new':
        erase = True
    elif arg == '--debug':
        debug = True
    else:
        dirs.append(arg)

pusher = Pusher(debug=debug)
pusher.connect('mongodb://localhost:27017/', 'wowauc')

if erase:
    pusher.recreate()

pusher.fix()

parser = Parser(pusher, debug=debug)

ts_start = datetime.datetime.now()
count = 0
for d in dirs:
    files = sorted(glob(d + '/*.json'))
    count = count + len(files)
    for f in files:
        parser.parse_file(f)
pusher.save_opened()
ts_end = datetime.datetime.now()
print '* %d files processed at %s\n' % (count, str(ts_end - ts_start))
