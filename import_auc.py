#! /usr/bin/env python
# -*- coding: utf-8 -*-
#

from wowauc.Parser import Parser
from wowauc.Pusher_MongoDB import Pusher_MongoDB as Pusher
from sys import argv
from glob import glob
from argparse import ArgumentParser
from sys import argv

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

for d in dirs:
    for f in sorted(glob(d + '/*.json')):
        parser.parse_file(f)
