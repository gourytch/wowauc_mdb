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
        if isinstance(M[k], dict):
            M[k] = wrap(M[k]) # рекурсивно сворачиваем
        elif isinstance(M[k], datetime.datetime):
            M[k] = M[k].isoformat()
        elif isinstance(M[k], unicode):
            M[k] = M[k].encode('utf8')
    return M


def unwrap(M):
    """развернём то, что надо, послк получения из JSON"""
    rx_ts = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}\d{2}$')
    for k in M:
        if isinstance(M[k], dict):
            M[k] = unwrap(M[k]) # рекурсивно разворачиваем
        elif isinstance(M[k], str) or isinstance(M[k], unicode):
            if rx_ts.match(M[k]):
                M[k] = dateutil.parser.parse(M[k])
            elif isinstance(M[k], str):
                M[k] = M[k].decode('utf8')
    return M



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


def print_memory_usage():
    print ("Memory used: %.2f MiB" %  memory_usage_resource())

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

        self.__region   = region
        self.__realm    = realm
        self.__time     = None
        self.__prevtime = None

        self.__opened   = {} # dict {auc -> opened auction}
        self.__unsaved  = False

        self.__openset  = sets.Set() # auc's in opened
        self.__seenset  = sets.Set() # auc's in snapshot
        self.__started  = False

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
        assert type(lot) is dict, "lot is not a dict but %s" % type(lot)
        lot['region']   = self.__region
        lot['realm']    = self.__realm
        lot['time']     = self.__time

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
            self.__lot_close(self.__opened[id])

        self.__save_closed()
        self.__save_expired()

        self.__ts_done = datetime.datetime.now()

        self.debug("... session finished")
        self.__started = False

        if self.__safe:
            self.save()
        return


    def __lot_open(self, lot):
        id = lot['auc']
        t = lot['time']
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
        lot['time'] = {
            'opened'    : t,
            'lastseen'  : t,
            'closed'    : None,
            'raised'    : None,
            'deadline'  : t_deadline
        }
        self.__opened[id] = lot
        self.__unsaved = True
        self.__openset.add(id)
        self.__num_opened = self.__num_opened + 1
        return


    def __lot_update(self, lot):
        id = lot['auc']
        opn = self.__opened[id]
        if opn['bid'] != lot['bid']:
            opn['bid'] = lot['bid']
            opn['time']['raised'] = lot['time']
            self.__num_raised = self.__num_raised + 1
        if opn['timeLeft'] != lot['timeLeft']:
            # сменился диапазон времени
            # предполагаем, что это произошло где-то
            # между предыдущим и текущим обновлением
            # но не позже старого deadline
            t_hit = random_datetime(opn['time']['lastseen'], lot['time'])
            if opn['time']['deadline'] < t_hit:
                t_hit = opn['time']['deadline']
            # это гарантированно изменение интервала,
            # так что можно заложиться
            # на максимально возможное значение
            t = guess_expiration(t_hit, opn['timeLeft'])['max']
            opn['time']['deadline'] = t
            opn['timeLeft'] = lot['timeLeft']
            self.__num_adjusted = self.__num_adjusted + 1
        opn['time']['lastseen'] = lot['time']
        self.__unsaved = True
        return


    def __lot_close(self, lot):
        lot['time']['closed'] = self.__time
        is_raised = lot['time']['raised'] is not None
        is_expired = lot['time']['deadline'] < lot['time']['closed']
        lot['result'] = []
        if is_raised or not is_expired:
            lot['result'].append('success')
            if is_raised:
                lot['result'].append('raised')
            if not is_expired:
                lot['result'].append('buyout')
            self.__bulk_closed.append(lot)
            self.__num_closed = self.__num_closed + 1
        else:
            lot['result'].append('expired')
            self.__bulk_expired.append(lot)
            self.__num_expired = self.__num_expired + 1
        id = lot['auc']
        del self.__opened[id]
        self.__unsaved = True
        self.__openset.remove(id)
        return


    def __save_closed(self):
        if not len(self.__bulk_closed):
            self.debug("no closed auction in this session")
            return
        self.debug("save {} finished auction".format(len(self.__bulk_closed)))

        F = codecs.open(self.__name_finished, 'at', "utf-8")
        for R in self.__bulk_closed:
            F.write(json.dumps(wrap(R), ensure_ascii=False, encoding='utf8') + "\n")
        F.close()
        return


    def __save_expired(self):
        if not len(self.__bulk_closed):
            self.debug("no expired auction in this session")
            return
        self.debug("save {} expired auction".format(len(self.__bulk_expired)))

        F = codecs.open(self.__name_expired, 'at', "utf-8")
        for R in self.__bulk_expired:
            F.write(json.dumps(wrap(R), ensure_ascii=False, encoding='utf8') + "\n")
        F.close()
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
        R = json.load(codecs.open(fname, 'r', 'utf-8'))
        assert type(R) is dict, "not a dict but '{0}'".format(type(R))
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
        self.debug("json snapshot loaded. parse it")
        self.start(ts)
        for auc in R['auctions']['auctions']:
            self.push(auc)
        self.finish()
        ts_end = datetime.datetime.now()
        self.debug('* file %s processed at %s\n' % (fname, str(ts_end - ts_start)))
        print_memory_usage()
        gc.collect()
#        except AssertionError as e:
#            print("got assertion: {}".format(e))
#            raise e
#            return None, e
        return True


    def processDir(self, dirname):
        self.debug("process directory {}".format(dirname))
        mask = '????????_??????_{}_{}.json'.format(self.__region, self.__realm)
        for fname in sorted(glob.glob(dirname + "/" + mask)):
            self.process(fname)
        return


### EOF ###
if __name__ == '__main__':
    prc = SnapshotProcessor('./processed', 'eu', 'Fordragon', True, True)
    prc.load()
    prc.processDir('./input')
    prc.save(True)
