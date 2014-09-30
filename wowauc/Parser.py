#! /usr/bin/env python
# -*- coding: utf-8 -*-
#

import re, datetime, sys, os, os.path, glob

class Parser(object):

    def __init__(self, pusher, region='eu', locale='en_US', debug=False):
        self.__pusher = pusher
        self.__region = region
        self.__locale = locale
        self.__debug  = debug
        return


    def thrifty_push(self, ts, fobj):
        """
        более бережливый до памяти вариант:
        парсим файл построчно, eval-им что нужно
        заливаем в базу
        """

        rx_realm        = re.compile(r'^"realm":(\{.*\}),$')
        rx_ah_start     = re.compile(r'^"([^"]+)":\{"auctions":\[$')
        rx_lot          = re.compile(r'^\s*(\{"auc":[^}]+\})(|\]\},?|,)$')
        rx_ah_end       = re.compile(r'\}\]\},?$')
        rx_end          = re.compile(r'^\}$')

        realm = None
        slug  = None
        house = None

        while True:
            s = fobj.readline()
            if not len(s):
                break # закончился файл - заканчиваем работу
            s = s.strip()

            if not s: # пустая строчка
                continue

            if s == '{': # открытие данных
                continue

            # "realm":{"name":"Fordragon","slug":"fordragon"},
            r = rx_realm.search(s)
            if r:
                v = eval(r.group(1))
                realm = v['name']
                slug  = v['slug']
                if self.__pusher.need(self.__region, realm, ts):
                    self.__pusher.touch_realm(
                        self.__region, realm, slug, self.__locale)
                    self.__pusher.start(self.__region, realm, ts)
                elif self.__debug:
                    print "skip %s @ %s" % (realm, ts)
                continue # переходим к следующей строчке


            #"alliance":{"auctions":[
            r = rx_ah_start.search(s)
            if r:
                house = r.group(1)
                if self.__pusher.is_started():
                    self.__pusher.set_AH(house)
                elif self.__debug:
                    print "skip %s/%s @ %s" % (realm, house, ts)
                continue # следующие строчки - данные аукционного дома


            #{"auc":1649217884,"item":25043,"owner": ... "timeLeft":"VERY_LONG"},
            r = rx_lot.search(s)
            if r:
                if self.__pusher.is_started():
                    v = eval(r.group(1))
                    self.__pusher.push(v)
                if r.group(2) != ',': # последний лот AH
                    house = None
                continue

            r = rx_end.search(s)
            if r:
                if self.__pusher.is_started():
                    self.__pusher.finish()
                break;

            print "? %s" % s
        return


    def parse_file(self, fname):
        rx = re.search(r'.*/(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})(?:_(.+)_(.+)|)\.json$', fname)
        assert rx, "fname not match: %s" % fname
        ts = datetime.datetime(
            int(rx.group(1)), int(rx.group(2)), int(rx.group(3)),
           +int(rx.group(4)), int(rx.group(5)), int(rx.group(6)))
        if rx.group(7):
            self.__region = rx.group(7)
        realm = rx.group(8) or 'Fordragon'
        ts_start = datetime.datetime.now()
        fobj = open(fname, 'rt')
        print '* process file %s' % fname
        self.thrifty_push(ts, fobj)
        fobj.close()
        ts_end = datetime.datetime.now()
        print '* file %s processed at %s\n' % (fname, str(ts_end - ts_start))
