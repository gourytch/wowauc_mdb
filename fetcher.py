#! /usr/bin/env python
# -*- coding: utf-8 -*-
#

import pycurl
import cStringIO
import time
import os.path
import sys
from wowauc.Parser import Parser

exec file("wowauc.conf", "rt").read()

try:
    APIKEY = apikey
    assert len(APIKEY)
except:
    print "%% API KEY NOT FOUND! get it from https://dev.battle.net/"
    sys.exit(1)

try:
    LOCALE = locale
except:
    print "%% locale is not set. assume <en_US>"
    LOCALE = 'en_US'

try:
    FETCH_ONLY = fetch_only
except:
    print "%% fetch_only is not set. assume False"
    FETCH_ONLY = False

if not FETCH_ONLY:
    from wowauc.Pusher_CachedMongoDB import Pusher_CachedMongoDB as Pusher
    
    
CURDIR = os.path.dirname(os.path.abspath(__file__)) + "/"

TMPDIR  = CURDIR + dir_fetching + "/"
SAVEDIR = CURDIR + dir_fetched + "/"

QUIET   = '-q' in sys.argv
DEBUG = not QUIET

def dumphdr(hdr):
    if QUIET: return
    print "%% got headers: %s" % repr(hdr)
    return


class Writer(object):

    def __init__(self, fname):
        object.__init__(self)
        self._fname = fname
        if DEBUG:
            print "+ open %s" % self._fname
        self._file = open(self._fname, "wt")
        self._count = 0
        self._gzipped = False
        self._data = ''
        return

    def header(self, s):
        if DEBUG:
            print "+ got header: {%s}" % repr(s)
        k = [x.strip() for x in s.split(':')]
        if len(k) < 2:
            return
        if k[0] == 'Content-Encoding':
            self._gzipped = (k[1] == 'gzip')
            if DEBUG:
                print "+ encoding: %s" % k[1]
        return

    def write(self, data):
        n = len(data)
        self._count += n
#        if not QUIET:
#            s = "+ %d bytes received " % self._count
            # sys.stdout.write("%s%s" % (s, '\b' * len(s)))
#            sys.stdout.write("%s  \r" % s)
        self._file.write(data)
        self._data += data
        return

    def close(self):
        if DEBUG:
            print "\n+ close stream. %d octets wrote" % self._count
        self._file.close()
        return

    def getData(self):
        return self._data

def fetch(region, realm):
    if DEBUG:
        print "* fetch for region=%s, realm=%s" % (region, realm)
    buf = cStringIO.StringIO()
    c = pycurl.Curl()

    c.setopt(c.WRITEFUNCTION, buf.write)
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

    url = "https://{0}.api.battle.net/wow/auction/data/{1}?locale={2}&apikey={3}" \
        .format(region, realm, LOCALE, APIKEY)
    if DEBUG:
        print "* retrieve auction url {0}".format(url)
    c.setopt(c.URL, url)
    c.perform()

    s = buf.getvalue()
    buf.close()

    if DEBUG:
        print "got data"
        print s
        print "--------------------"

    M   = eval(s)
    if M.get('status') == 'nok':
        if DEBUG:
            print "* wowapi does not known about %s:%s" % (region, realm)
        return
    url = M['files'][0]['url']
    t_s = M['files'][0]['lastModified'] / 1000.0
    t_gm = time.gmtime(t_s)
    t_lo = time.localtime(t_s)

    name  = "%s_%s_%s.json" \
        % (time.strftime(r"%Y%m%d_%H%M%S", t_gm), region, realm)
    tname = TMPDIR + name
    sname = SAVEDIR + name
    ts_gm = time.strftime(r"%Y-%m-%d %H:%M:%S", t_gm)
    ts_lo = time.strftime(r"%Y-%m-%d %H:%M:%S", t_lo)

    if DEBUG:
        d_h = int(time.time() - t_s)
        d_s = d_h % 60
        d_h /= 60
        d_m = d_h % 60
        d_h /= 60
        print "* timestamp: %s" % ts_gm
        print "* tocaltime: %s" % ts_lo
        print "*       age: %d:%02d:%02d" % (d_h, d_m, d_s)

    if os.path.exists(tname):
        if DEBUG:
            print "* temporary name {0} already exists. skip".format(tname)
    elif os.path.exists(sname):
        if DEBUG:
            print "* name {0} already exists. skip".format(sname)
    else:
        try:
            if DEBUG:
                print "* save to %s" % tname
                print "* retrieve auction data"
            c.setopt(c.URL, url)
            c.setopt(c.CONNECTTIMEOUT, 15)
            c.setopt(c.TIMEOUT, 300)
            c.setopt(c.ENCODING, 'gzip')
            c.setopt(c.FOLLOWLOCATION, True)
            f = Writer(tname)
            c.setopt(c.WRITEFUNCTION, f.write)
            c.setopt(c.HEADERFUNCTION, f.header)
            c.perform()
            retcode = c.getinfo(pycurl.HTTP_CODE)
            c.close()
            f.close()
            if DEBUG:
                print "* retcode=%d" % retcode
            if retcode == 200:
                if DEBUG:
                    if FETCH_ONLY:
                        parser = Parser(None, region=region, debug=DEBUG)
                        data, reason = parser.load_and_check(file(tname).read())
                        good = data is not None
                    else:
                        print "* good retcode. import data"
                        ### IMPORT ###
                        pusher = Pusher(debug=DEBUG)
                        pusher.connect('mongodb://localhost:27017/', 'wowauc')
                        pusher.fix()
                        parser = Parser(pusher, region=region, debug=DEBUG)
                        good, reason = parser.parse_file(tname, greedy=True)
                        pusher.save_opened()
                        ### END IMPORT ###
                if good:
                    if DEBUG:
                        print "* good data. rename results to %s" % sname
                    os.rename(tname, sname)
                    if DEBUG:
                        print "* ...moved"
                else:
                    if DEBUG:
                        print "* bad/broken data. {0}".format(reason)
            else: # retcode != 200
                if DEBUG:
                    print "* something wrong. retcode: {0}".format(retcode)
                    print "* retrieved data:"
                    print file(tname).read()
        finally:
            if os.path.exists(tname):
                bad_sname = sname + ".bad"
                if DEBUG:
                    print "* move temporary file {0} to bad name".format(tname)
                if os.path.exists(bad_sname):
                    os.remove(bad_sname)
                os.rename(tname, bad_sname)

    if not QUIET:
        print "* done"

if __name__ == '__main__':
    for item in watchlist.split():
        (region, realm) = item.split(':')
        fetch(region, realm)
