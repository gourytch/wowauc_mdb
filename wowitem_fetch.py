#! /usr/bin/env python
# -*- coding: utf-8 -*-
#

import pymongo
import datetime
import sets

import pycurl
import cStringIO
import time
import os
import sys
import re
import json

exec file("wowauc.conf", "rt").read()

if "item_region" not in dir():
    item_region = "eu"

if "item_locales" not in dir():
    item_locales = "en_US ru_RU"

locales = items_locales.split()

if "apikey" not in dir():
    apikey = None

if "db_uri" not in dir():
    db_uri = 'mongodb://localhost:27017/'

if "db_name" not in dir():
    db_name = 'wowauc'

if "calls_per_second" not in dir():
    calls_per_second = 7

if "calls_per_minute" not in dir():
    calls_per_minute = 1000

QUIET   = '-q' in sys.argv

db = None # value will initialized in init_d()

class Mongo_Base:

    def __init__(self, uri, dbname):
        self.__debug = not QUIET
        self.connect(uri, dbname)
        return

    def connect(self, uri, dbname):
        """
        соединиться с базой данных
        """
        if self.__debug:
            print "create client for uri %s and base %s" % (uri, dbname)
        self.__uri    = uri
        self.__dbname = dbname
        self.__client = pymongo.MongoClient(uri)
        self.__db     = self.__client[dbname]
        self.__db.set_profiling_level(pymongo.OFF)

        self.__existing_items   = sets.Set()
        self.__cached_items     = sets.Set()
        self.__existing_pets    = sets.Set()
        self.__cached_pets      = sets.Set()


        if self.__debug:
            print "...created"
        return


    def is_connected(self):
        """
        возвращаем True если соединение с базой данных поизведено
        """
        return self.__client is not None


    def disconnect(self):
        """
        отсоединиться от базы данных
        """
        if not self.is_connected():
            return
        assert not self.is_started(), "push session not finished"
        if self.__debug:
            print "diconnect from database"
        self.__client = None
        if self.__debug:
            print "... diconnected"
        return


    def recreate(self):
        print "*** [[[ RECREATE TABLES ]]] ***"
        self.__db['items'].remove({})
        self.__db['items'].create_index([
            ('id', pymongo.ASCENDING),
            ])
        print "*** [[[ TABLES RECREATED ]]] ***"
        return


    def calc_existing_items(self):
        assert self.is_connected(), "not connected"
        self.__existing_items.clear()
        for name in ('opened', 'closed', 'expired'):
            for auc in self.__db[name].find({}):
                item_id = auc['item']
                self.__existing_items.add(item_id)
                if 'petSpeciesId' in auc:
                    self.__existing_pets.add(auc['petSpeciesId'])
        return


    def calc_cached_items(self):
        assert self.is_connected(), "not connected"
        self.__cached_items.clear()
        for item in self.__db['items'].find({}):
            item_id = item['id']
            self.__cached_items.add(item_id)
        return


    def calc_uncached_items(self):
        assert self.is_connected(), "not connected"
        self.__uncached_items = self.__existing_items.difference(self.__cached_items)
        self.__uncached_pets =  self.__existing_pets.difference(self.__cached_pets)
        return


    def calc_ids_for_fetch(self):
        self.calc_existing_items();
        self.calc_cached_items();
        self.calc_uncached_items()
        return sorted(list(self.__uncached_items))


    def put(self, loc, text):
        """
        items[]: { item_id,
                   localised => {
                        locale => {% item description %}
                        ...
                }
        """
        R = json.loads(text)
        found = False
        for V in self.__db['items'].find({'id': R['id']}):
            found = True
            if 'localized' not in V:
                V['localized'] = {}
            if loc in V['localized']:
                if'error' not in R:
                    V['localized'][loc] = R
                else:
                    pass
            else:
                V['localized'][loc] = R
            self.__db['items'].save(V)
        if not found:
            V = {
                'id': R['id'],
                'localized': {loc: R}
            }
            self.__db['items'].insert(V)
        return


def dumphdr(hdr):
    if QUIET: return
    print "%% got headers: %s" % repr(hdr)
    return


def fetch_items(db, ids):
    c = pycurl.Curl()

    c.setopt(c.CONNECTTIMEOUT, 15)
    c.setopt(c.TIMEOUT, 15)
    if "http_proxy" in dir():
        PXY = http_proxy
    else:
        PXY = os.getenv("http_proxy")
    if PXY:
        c.setopt(c.PROXY, PXY)
    c.setopt(c.ENCODING, 'gzip')
    c.setopt(c.FOLLOWLOCATION, True)
    c.setopt(c.HEADERFUNCTION, dumphdr)
    c.setopt(c.HTTPHEADER, ['Pragma: no-cache', 'Cache-Control: no-cache'])

    sec_limit = calls_per_second
    sec_start = time.time()

    min_limit = calls_per_minute
    min_start = time.time()

    print "* %d items for fetch" % len(ids)
    for id in ids:
        cur_time = time.time()

        if (1.0 < cur_time - min_start) or (min_limit <= 0):
            if cur_time - min_start < 1.0:
                dt = min_start + 60.1 - cur_time
                print " = wait till start of next minute (%.2s)" % dt
                time.sleep(dt)
            # reset second limit
            cur_time = time.time()
            min_limit = calls_per_minute
            min_start = cur_time
        else:
            min_limit = min_limit - 1

        if (1.0 < cur_time - sec_start) or (sec_limit <= 0):
            if cur_time - sec_start < 1.0:
                dt = sec_start + 1.1 - cur_time
                print " = wait till start of next second (%.2s)" % dt
                time.sleep(dt)
            # reset second limit
            cur_time = time.time()
            sec_limit = calls_per_second
            sec_start = cur_time
        else:
            sec_limit = sec_limit - 1

        if type(id) in (int, long):
            iid = id
        else:
            iid = int(id)

        showed = False
        for loc in locales:
            buf = cStringIO.StringIO()
            c.setopt(c.WRITEFUNCTION, buf.write)
            if apikey:
                url = "https://{0}.api.battle.net/wow/item/{1}?locale={2}&apikey={3}"\
                .format (item_region, iid, loc, apikey)
            else:
                url = "http://{0}.battle.net/api/wow/item/{1}?locale={2}"\
                .format (item_region, iid, loc)
            c.setopt(c.URL, url)
            if not QUIET:
                if not showed:
                    print "* get item %d" % iid
                    showed = True
                print "* retrieve url %s" % url
            c.perform()

            retcode = c.getinfo(pycurl.HTTP_CODE)

            s = buf.getvalue()
            buf.close()

            if retcode == 200:
                if not QUIET:
                    print "got data"
                    print s
                    print "--------------------"
                db.put(loc, s)
            else:
                print ""
                print "%06d-%s retcode %d" % (iid, loc, retcode)
                print "url: %s" % url
                print ""
                db.put(loc, '{"id":%d, "error": "%d"}' % (iid, retcode))
        # end for loc in locales
    # end for id in ids

    if not QUIET:
        print "* done"
    return


if __name__ == '__main__':
    db = Mongo_Base(db_uri, db_name)
    if '--new' in sys.argv[1:]:
        db.recreate()
    ids = db.calc_ids_for_fetch()
    fetch_items (db, ids)
    db.disconnect()
    print "done"
