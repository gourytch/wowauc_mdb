#! /usr/bin/env python
# -*- coding: utf-8 -*-
#

from wowauc.Parser import Parser
from wowauc.Pusher_MongoDB import Pusher_MongoDB as Pusher
from glob import glob

pusher = Pusher(debug=True)
pusher.connect('mongodb://localhost:27017/', 'wowauc')
# pusher.erase()
pusher.create_indexes()
pusher.fix()

parser = Parser(pusher, debug=True)

for f in sorted(glob('fetched/*.json')):
    parser.parse_file(f)
