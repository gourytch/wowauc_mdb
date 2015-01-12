#! /usr/bin/env python
# -*- coding: utf-8 -*-
#

##############################################################################
##
## дополнительные поля
##   snapshot :
##      region, realm, house -- идентификатор AH
##      time -- таймштамп серверного времени
##
##   opened :
##      region, realm, house -- идентификатор AH
##      success
##      time {
##          opened
##          lastseen
##          raised
##          deadline {
##              min
##              max
##          }
##
##   closed / expired :
##      region, realm, house -- идентификатор AH
##      time {
##          opened
##          lastseen
##          raised
##          deadline {
##              min
##              max
##          }
##

import datetime
import dateutil.parser
import re
import gc
import pdb
import random
import sets
import json
import sys
import os
import os.path
import glob
import codecs
import cPickle as pickle
import collections

#import pprint
#pp = pprint.PrettyPrinter(indent = 4)

intervals = {
    'SHORT'     : {
                    'min' : datetime.timedelta(0,0),
                    'max' : datetime.timedelta(0, 60 * 30)
                  },
    'MEDIUM'    : {
                    'min' : datetime.timedelta(0, 60 * 30),
                    'max' : datetime.timedelta(0, 2 * 3600)
                  },
    'LONG'      : {
                    'min' : datetime.timedelta(0,  2 * 3600),
                    'max' : datetime.timedelta(0, 12 * 3600)
                  },
    'VERY_LONG' : {
                    'min' : datetime.timedelta(0, 12 * 3600),
                    'max' : datetime.timedelta(2, 0)
                  }
            }


FIELDS = {
    'auc_modifiers': ['type', 'value'],
    'auc_bonusLists': ['bonusListId'],
    'auc': ['auc', 'item', 'owner', 'ownerRealm',
        'bid', 'buyout', 'quantity',
        'timeLeft',
        'rand', 'seed', 'context',
        'bonusLists', 'modifiers',
        'petSpeciesId', 'petBreedId', 'petLevel', 'petQualityId'],
    'meta' : ['region', 'realm', 'id',
            'ts_opened', 'ts_deadline', 'ts_lastseen',
            'ts_raised', 'ts_closed',
            'is_success', 'is_buyout',
            'bid_first', 'bid_last'],
}


random.seed()

def random_timedelta(dt):
    """ вычисление случайного интервала, не превышающего заданный """
    assert isinstance(dt, datetime.timedelta), "type of dt should be datetime.timedelta, not %s" % type(dt)
    return datetime.timedelta(0, int(dt.total_seconds() * random.random()))


def random_datetime(t1, t2):
    """ вычисление случайного  времени из диапазона """
    assert isinstance(t1, datetime.datetime), "type of t1 should be datetime.datetime, not %s (%s)" % (type(t1), repr(t1))
    assert isinstance(t2, datetime.datetime), "type of t2 should be datetime.datetime, not %s (%s)" % (type(t1), repr(t2))
    t1, t2 = min(t1, t2), max(t1, t2)
    return t1 + random_timedelta(t2 - t1)


def guess_expiration(t, timeLeft):
    """ временнЫе рамки для именованного диапазона """
    assert isinstance(t, datetime.datetime), "type of t should be datetime.datetime, not %s (%s)" % (type(t), repr(t))
    p = intervals[timeLeft]
    return {'min': t + p['min'], 'max': t + p['max']}


def mapfmt(name, M):
    """ красиво форматируем табличку ключ = значение """
    spc = '\n' + (' ' * (len(name) + 3))
    maxlen1 = max((len(unicode(x)) for x in M.keys()))
    maxlen2 = max((len(unicode(x)) for x in M.values()))
    return name + ' : ' + (
        spc.join(("%-*s = %*s" % ( maxlen1, unicode(x),
                                    maxlen2, unicode(M[x]))
                for x in sorted(M.keys()))))


def wrap(M):
    """свернём то, что надо, перед отправкой в JSON"""
    for k in M:
        if isinstance(M[k], dict) \
        or isinstance(M[k], collections.OrderedDict):
            M[k] = wrap(M[k]) # рекурсивно сворачиваем
        elif isinstance(M[k], list) \
        or isinstance(M[k], tuple):
            V = []
            for v in M[k]:
                V.append(wrap(v))
            M[k] = V
        elif isinstance(M[k], datetime.datetime):
            M[k] = M[k].isoformat()
        elif isinstance(M[k], unicode):
            M[k] = M[k].encode('utf8')
    return M


def unwrap(M):
    """развернём то, что надо, после получения из JSON"""
    rx_ts = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}\d{2}$')
    for k in M:
        if isinstance(M[k], dict) \
        or isinstance(M[k], collections.OrderedDict):
            M[k] = unwrap(M[k]) # рекурсивно разворачиваем
        elif isinstance(M[k], list) \
        or isinstance(M[k], tuple):
            V = []
            for v in M[k]:
                V.append(unwrap(v))
            M[k] = V
        elif isinstance(M[k], str) \
        or isinstance(M[k], unicode):
            if rx_ts.match(M[k]):
                M[k] = dateutil.parser.parse(M[k])
            elif isinstance(M[k], str):
                M[k] = M[k].decode('utf8')
    return M


def mkOrderedDict(src, path):
    global FIELDS
    flist = FIELDS[path]

    def pwn(obj, path, name):
        if isinstance(obj, dict) \
        or collections.OrderedDict():
            return mkOrderedDict(obj, path + "_" + name)
        elif isinstance(obj, list) \
        or isinstance(obj, tuple):
            V = []
            for v in obj:
                V.append(pwn(v, path, name))
            return V
        return obj

    dst = collections.OrderedDict()
    for f in flist:
        if f in src:
            dst[f] = pwn(src[f], path, f)
    for f in src:
        if f not in dst:
            print "WARN: not in flist by path '%s': '%s'" % (path, f)
            dst[f] = pwn(src[f], path, f)
    return dst


def aucOrderedDict(auc):
    return mkOrderedDict(wrap(auc), 'auc')


def metaOrderedDict(meta):
    return mkOrderedDict(wrap(meta), 'meta')


def dtstr(sec):
    s = sec
    h = s // 3600; s = s % 3600
    m = s // 60; s = s % 60
    return "%d:%02d:%02d" % (h, m, s)


def printmap(name, M):
    print mapfmt(name, M)
    return


def memory_usage_resource():
    import resource
    rusage_denom = 1024.
    if sys.platform == 'darwin':
        # ... it seems that in OSX the output is different units ...
        rusage_denom = rusage_denom * rusage_denom
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / rusage_denom
    return mem


def print_memory_usage(title=""):
    print ("Memory used %s: %.2f MiB" %  (title, memory_usage_resource()))


class SnapshotProcessor (object):
    """
    класс для анализа дампов аукциона из WoW API
    и построения базы завершенных аукционов
    """

    def __init__ (self, basedir=".", region='eu', realm='Fordragon', safe=True, debug=True):
        self.__safe     = safe
        self.__debug    = debug

        self.__basedir  = basedir
        if not os.path.exists(self.__basedir):
            self.debug("create bas directory '{}'".format(self.__basedir))
            os.makedirs(self.__basedir, 0755)
        assert(os.path.isdir(self.__basedir))

#        self.__name_state     = basedir + "/" + region + "-" + realm + "-session_state.json"
        self.__name_state     = basedir + "/" + region + "-" + realm + "-session_state.pickle"
        self.__name_finished  = basedir + "/" + region + "-" + realm + "-closed-finished.txt"
        self.__name_expired   = basedir + "/" + region + "-" + realm + "-closed-expired.txt"
        self.__name_meta      = basedir + "/" + region + "-" + realm + "-closed-meta.txt"

        self.__region   = region
        self.__realm    = realm
        self.__time     = None
        self.__prevtime = None

        self.__opened   = {} # dict {auc -> opened auction}
        self.__unsaved  = False

        self.__openset  = sets.Set() # auc's in opened
        self.__seenset  = sets.Set() # auc's in snapshot
        self.__started  = False

        self.__fields = {} # "/" - root fields, "/subfield" - subfields

        return


    def debug(self, text):
        if self.__debug:
            print(text)
        return


    def load(self):
        """
        чтение сохраненного состояния (открытые ранее аукционы и т.д.)
        """
        assert not self.is_started()

        self.__opened = {}
        self.__openset.clear()
        if os.path.isfile(self.__name_state):
            self.debug("load saved state")
            saved = pickle.load(file(self.__name_state, "r"))
#           saved = unwrap(json.load(codecs.open(self.__name_state, "r", "utf-8")))
            assert self.__region == saved['region']
            assert self.__realm == saved['realm']
            self.__time = saved['time']
            self.__opened = saved['opened']
            self.__openset.update(self.__opened)
            self.debug("last processed timestamp : {}".format(self.__time.isoformat()))
            self.debug("opened positions         : {}".format(len(self.__opened)))
        else:
            self.debug("no saved state found")
        self.__unsaved = False
        return


    def save(self, force=False):
        assert self.__region is not None
        assert self.__realm is not None
        assert not self.is_started()
        if not self.__unsaved and not force:
            self.debug("no unsaved changes. no save performed")
            return
        saved = {
            'region': self.__region,
            'realm': self.__realm,
            'time': self.__time,
            'opened': self.__opened
        }
        try:
            tmpname = self.__name_state + ".tmp"
            bakname = self.__name_state + ".bak"
            pickle.dump(saved, file(tmpname, 'w'))
            if os.path.exists(bakname):
                os.unlink(bakname)
#        json.dump(wrap(saved),
#            codecs.open(self.__name_state, 'w', "utf-8"),
#            ensure_ascii=False,
#            encoding='utf8',
#            indent=3)
            if os.path.exists(self.__name_state):
                os.rename(self.__name_state, bakname)
            os.rename(tmpname, self.__name_state)
            self.debug("state saved")
            self.__unsaved = False
        except Exception as e:
            print("go exception: %s" % e)
        return


    def is_started(self):
        """
        возвращаем True если активен сеанс добавления данных
        """
        return self.__started


    def start(self, wowts):
        """
        начинаем сессию добавления данных для всех аукционов реалма
        """
        assert isinstance(wowts, datetime.datetime)
        assert not self.is_started(), "push session already started"

        self.__prevtime = self.__time
        self.__time     = wowts
        self.__ts_start = datetime.datetime.now()

        self.debug("start parsing session")
        if self.__prevtime is not None:
            self.debug("previous time = %s" % self.__prevtime.isoformat())
        self.debug("snapshot time = %s" % self.__time.isoformat())
        if self.__prevtime:
            self.debug("time delta    = %s" % (self.__time - self.__prevtime))

        self.__seenset.clear()
        self.__num_opened   = 0
        self.__num_raised   = 0
        self.__num_adjusted = 0
        self.__num_closed   = 0
        self.__num_expired  = 0

        self.__bulk_closed  = []
        self.__bulk_expired = []

        self.__started = True
        return


    def push(self, lot):
        """
        запихнём данные по лоту в базу
        объект лота - словарь
        """
        assert self.is_started(), "push session not started"
        assert isinstance(lot, dict), \
            "lot is not a dict instance but %s" % type(lot)

        id = lot['auc']
        if id in self.__opened:
            self.__lot_update(lot)
        else:
            self.__lot_open(lot)
        self.__seenset.add(id)
        return


    def finish(self):
        """
        завершение push-сеанса и коммит данных
        возвращает словарь с краткой статистикой по завершенной сессии
        (количество открытых/закрытых/выкупленных позиций)
        """
        assert self.is_started(), "push session is not started"
        self.debug("update collections")

        clset = self.__openset - self.__seenset

        if self.__debug:
            printmap("set sizes" , {
                "openset" : len(self.__openset),
                "seenset" : len(self.__seenset),
                "clset"   : len(clset)})

        for id in clset:
            self.__lot_close(id)

        self.__save_closed()
        self.__save_expired()

        self.__ts_done = datetime.datetime.now()

        self.debug("... session finished")
        self.__started = False

        if self.__safe:
            self.save()
        return


    def __lot_open(self, lot):
        """
        запись открытия лота по записи lot
        """
        id = lot['auc']
        t = self.__time

        t_dead1 = guess_expiration(self.__time, lot['timeLeft'])['min']
        if self.__prevtime is None:
            # мы не знаем, когда был открыт этот аукцион даже примерно
            # значит выставим минимальное время
            t_deadline = t_dead1
        else:
            # в прошлый раз этого лота не было
            t_deadline = guess_expiration(self.__prevtime,
                                          lot['timeLeft'])['max']
            if t_deadline < t_dead1:
                # но слишком уж много времени прошло с того раза :)
                t_deadline = t_dead1

#        bundle = collections.OrderedDict([
#            ('meta', collections.OrderedDict([
#                ('region'           , self.__region),
#                ('realm'            , self.__realm),
#                ('id'               , id),
#                ('ts_opened'        , t),
#                ('ts_deadline'      , t_deadline),
#                ('ts_lastseen'      , t),
#                ('ts_raised'        , None),
#                ('ts_closed'        , None),
#                ('is_success'       , None),  # true=куплен false=просрочен
#                ('is_buyout'        , None),  # true=buyout false=by bid
#                ('bid_first'        , lot['bid']),
#                ('bid_last'         , lot['bid']),
#            ])),
#            ('data', lot),
#        ])


        bundle = {
            'meta' : {
                'region'            : self.__region,
                'realm'             : self.__realm,
                'id'                : id,
                'ts_opened'         : t,
                'ts_deadline'       : t_deadline,
                'ts_lastseen'       : t,
                'ts_raised'         : None,
                'ts_closed'         : None,
                'is_success'        : None,  # true=куплен false=просрочен
                'is_buyout'         : None,  # true=buyout false=by bid
                'bid_first'         : lot['bid'],
                'bid_last'          : lot['bid'],
            },
            'data': lot,
        }

        self.__opened[id] = bundle
        self.__unsaved = True
        self.__openset.add(id)
        self.__num_opened = self.__num_opened + 1
        return


    def __lot_update(self, lot):
        id = lot['auc']
        opn = self.__opened[id]
        opn['meta']['ts_lastseen'] = self.__time
        self.__unsaved = True
        changed = False
        if opn['data']['bid'] != lot['bid']:
            changed = True
            opn['meta']['ts_raised'] = self.__time
            opn['meta']['bid_last'] = lot['bid']
            self.__num_raised = self.__num_raised + 1
        if opn['data']['timeLeft'] != lot['timeLeft']:
            changed = True
            # сменился диапазон времени
            # предполагаем, что это произошло где-то
            # между предыдущим и текущим обновлением
            # но не позже старого deadline
            t_hit = random_datetime(opn['meta']['ts_lastseen'], self.__time)
            if opn['meta']['ts_deadline'] < t_hit:
                t_hit = opn['meta']['ts_deadline']
            # это гарантированно изменение интервала,
            # так что можно заложиться
            # на максимально возможное значение
            t = guess_expiration(t_hit, lot['timeLeft'])['max']
            opn['meta']['ts_deadline'] = t
            self.__num_adjusted = self.__num_adjusted + 1
        if changed:
            # replace/update lot record
            opn['data'] = lot
        return


    def __lot_close(self, id):
        opn = self.__opened[id]
        opn['meta']['ts_closed'] = self.__time
        is_raised = opn['meta']['ts_raised'] is not None
        is_expired = opn['meta']['ts_deadline'] < opn['meta']['ts_closed']
        if is_raised or not is_expired:
            opn['meta']['is_success'] = True
            opn['meta']['is_buyout'] = not is_expired
            self.__bulk_closed.append(opn)
            self.__num_closed = self.__num_closed + 1
        else:
            opn['meta']['is_success'] = False
            self.__bulk_expired.append(opn)
            self.__num_expired = self.__num_expired + 1
        del self.__opened[id]
        self.__unsaved = True
        self.__openset.remove(id)
        return


    def __save_closed(self):
        if not len(self.__bulk_closed):
            self.debug("no closed auction in this session")
            return
        self.debug("save {} finished auction".format(len(self.__bulk_closed)))

        Fauc = codecs.open(self.__name_finished, 'at', "utf-8")
        Fmeta = codecs.open(self.__name_meta, 'at', "utf-8")
        for R in self.__bulk_closed:
            Fauc.write(json.dumps(aucOrderedDict(R['data']), ensure_ascii=False, encoding='utf8') + "\n")
            Fmeta.write(json.dumps(metaOrderedDict(R['meta']), ensure_ascii=False, encoding='utf8') + "\n")
        Fauc.close()
        Fmeta.close()
        return


    def __save_expired(self):
        if not len(self.__bulk_closed):
            self.debug("no expired auction in this session")
            return
        self.debug("save {} expired auction".format(len(self.__bulk_expired)))

        Fauc = codecs.open(self.__name_expired, 'at', "utf-8")
        Fmeta = codecs.open(self.__name_meta, 'at', "utf-8")
        for R in self.__bulk_expired:
            Fauc.write(json.dumps(aucOrderedDict(R['data']), ensure_ascii=False, encoding='utf8') + "\n")
            Fmeta.write(json.dumps(metaOrderedDict(R['meta']), ensure_ascii=False, encoding='utf8') + "\n")
#            F.write(json.dumps(wrap(R), ensure_ascii=False, encoding='utf8') + "\n")
        Fauc.close()
        Fmeta.close()
        return


    def collect_fields(self, R, path = "/"):
        if isinstance(R, dict):
            prev_key = None
            if path not in self.__fields:
                self.__fields[path] = []
            for k in R:
                if k not in self.__fields[path]:
                    if prev_key is None:
                        print "(debug) add new field %s at top of %s" % (
                            k, path)
                        self.__fields[path].insert(0, k)
                    else:
                        print "(debug) add new field %s after %s in %s" % (
                            k, prev_key, path)
                        prev_ix = self.__fields[path].index(prev_key)
                        self.__fields[path].insert(prev_ix + 1, k)
                prev_key = k
                v = R[k]
                if isinstance(v, collections.OrderedDict) \
                or isinstance(v, dict) \
                or isinstance(v, list) \
                or isinstance(v, tuple):
                    subpath = path
                    if subpath[-1] != "/":
                        subpath += "/"
                    subpath += k
                    self.collect_fields(R[k], subpath)
        elif isinstance(R, list) or isinstance(R, tuple):
            for v in R:
                if isinstance(v, collections.OrderedDict) \
                or isinstance(v, dict) \
                or isinstance(v, list) \
                or isinstance(v, tuple):
                    self.collect_fields(v, path)
        else:
            print "ignore type %s" % type(R)
        return


    def dump_fields(self):
        print "fields = {"
        for (k, v) in self.__fields.items():
            print "  %s: [%s]," % (
                k.replace('/', '_'),
                ', '.join(["'%s'" % x for x in v]))
        print "}"
        return


    def process(self, fname):
        self.debug("process {}".format(fname))
        rx = re.search(r'.*/(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})(?:_(.+)_(.+)|)\.json$', fname)
        assert rx, "fname not match: %s" % fname
        ts = datetime.datetime(
            int(rx.group(1)), int(rx.group(2)), int(rx.group(3)),
           +int(rx.group(4)), int(rx.group(5)), int(rx.group(6)))
        region = rx.group(7)
        realm = rx.group(8)
        assert region == self.__region
        assert realm == self.__realm
        if self.__time is not None and ts <= self.__time:
            self.debug("snapshot time < last processed ( {} < {}). file skipped.".format(ts.isoformat(), self.__time.isoformat()))
            return True

        ts_start = datetime.datetime.now()
        self.debug("read json from file {}".format(fname))
#        try:
#        R = json.load(codecs.open(fname, 'r', 'utf-8'),
#            object_pairs_hook=collections.OrderedDict)
        R = json.load(codecs.open(fname, 'r', 'utf-8'))
        assert isinstance(R, dict), \
            "type is not a dict instance but '{0}'".format(type(R))
        assert "realm" in R, "have no 'realm' key"
        assert "name" in R['realm'], "have no realm.name"
        assert "slug" in R['realm'], "have no realm.slug"
        assert "auctions" in R, "have no 'auctions' key"
        assert "auctions" in R['auctions'], "have no auctions.auctions"
        assert type(R['auctions']['auctions']) in (list, tuple), \
                "{0} have bad auctions type {1}" \
                .format(house, type(R['auctions']['auctions']))
        assert self.__realm == R['realm']['name']

        gc.collect()
        print_memory_usage("before json")
        self.debug("json snapshot loaded. parse it")
        self.start(ts)
        for auc in R['auctions']['auctions']:
#            self.collect_fields(auc, '/')
            self.push(auc)
        self.finish()
        ts_end = datetime.datetime.now()
        self.debug('* file %s processed at %s\n' % (fname, str(ts_end - ts_start)))
        print_memory_usage("after")
        gc.collect()
#        except AssertionError as e:
#            print("got assertion: {}".format(e))
#            raise e
#            return None, e
        return True


    def processDir(self, dirname):
        print_memory_usage("at start processDir")
        self.debug("process directory {}".format(dirname))
        mask = '????????_??????_{}_{}.json'.format(self.__region, self.__realm)
        for fname in sorted(glob.glob(dirname + "/" + mask)):
            self.process(fname)
#       self.dump_fields()
        print_memory_usage("at finish processDir")
        return


### EOF ###
if __name__ == '__main__':
    prc = SnapshotProcessor(basedir = './processed',
                            region  = 'eu',
                            realm   = 'Fordragon',
                            safe    = False,
                            debug   = True)
    prc.load()
    prc.processDir('./input')
    prc.save(True)
