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

import pymongo
import datetime
import pdb
import random
import sets

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
    return datetime.timedelta(0, dt.total_seconds() * random.random())


def random_datetime(t1, t2):
    """ вычисление случайного  времени из диапазона """
    t1, t2 = min(t1, t2), max(t1, t2)
    return t1 + random_timedelta(t2 - t1)


def guess_expiration(t, timeLeft):
    """ временнЫе рамки для именованного диапазона """
    assert type(t) is datetime.datetime
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


def printmap(name, M):
    print mapfmt(name, M)
    return


class Pusher_CachedMongoDB (object):
    """
    класс для пополнения базы данными по аукционам
    """

    def __init__ (self, debug=False):
        self.__client   = None # mongodb client
        self.__db       = None # our database
        self.__debug    = debug

        self.__push_id  = None
        self.__region   = None
        self.__realm    = None
        self.__house    = None
        self.__time     = None
        self.__prevtime = None

        self.__opened   = None # dict {auc -> opened auction}
        self.__unsaved  = False

        self.__openset  = sets.Set() # auc's in opened
        self.__seenset  = sets.Set() # auc's in snapshot
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


    def load_opened(self):
        assert self.is_connected()
        assert not self.is_started()
        assert self.__region is not None
        assert self.__realm is not None
        assert not self.__unsaved
        self.__opened = {}
        self.__openset.clear()
        for R in self.__db['opened'].find({'region': self.__region,
                                             'realm': self.__realm}):
            self.__opened[R['auc']] = R
        self.__openset.update(self.__opened)
        self.__unsaved = False
        if self.__debug:
            print "%d opened positions were loaded" % len(self.__opened)
        return


    def save_opened(self):
        assert self.is_connected()
        assert not self.is_started()
        if self.__unsaved:
            assert self.__region is not None
            assert self.__realm is not None
            self.__db['opened'].remove({'region': self.__region,
                                         'realm': self.__realm})
            if len(self.__opened):
                bulk = self.__db['opened'].initialize_unordered_bulk_op()
                for R in self.__opened.values():
                    bulk.insert(R)
                bulk.execute()
                if self.__debug:
                    print "%d opened positions were saved" % len(self.__opened)
            elif self.__debug:
                print "no opened positions. no save"
            self.__unsaved = False
        elif self.__debug:
            print "no changes in opened positions. no save"
        return


    def touch_realm(self, region, realm, slug, locale):
        assert self.is_connected()
        assert not self.is_started()
        if self.__debug:
            print "touch realm (%s, %s, %s, %s)" \
                % (region, realm, slug, locale)
        assert self.is_connected(), "not connected to database"
        return


    def need(self, region, realm, wowts):
        """
        проверить, нужно ли добавление данных для данного времени
        """
        assert self.is_connected(), "not connected to database"

        return self.__db['push_sessions'].find({
            'region'    : region,
            'realm'     : realm,
            'time'      : {'$gte': wowts},
            'done'      : True,}).count() == 0


    def is_started(self):
        """
        возвращаем True если активен сеанс добавления данных
        """
        return self.is_connected() and self.__push_id is not None


    def start(self, region, realm, wowts):
        """
        начинаем сессию добавления данных для всех аукционов реалма
        """
        if self.__debug:
            print "start push session for (%s, %s, %s)" \
            % (region, realm, wowts)
        assert self.is_connected(), "not connected to database"
        assert self.need(region, realm, wowts), "duplicate/obsolete"
        assert not self.is_started(), "push session already started"

        if self.__region != region or self.__realm != realm:
            if self.__debug:
                print "region changed! save collected opened positions"
            if self.__opened and self.__unsaved:
                self.save_opened()
            self.__region   = region
            self.__realm    = realm
            self.load_opened()
            self.find_prevtime()
        else:
            if self.__time is None:
                self.find_prevtime()
            else:
                self.__prevtime = self.__time

        self.__time     = wowts
        self.__ts_start = datetime.datetime.now()

        self.__seenset.clear()
        self.__num_opened   = 0
        self.__num_raised   = 0
        self.__num_adjusted = 0
        self.__num_closed   = 0
        self.__num_expired  = 0

        self.__bulk_closed  = \
            self.__db['closed'].initialize_unordered_bulk_op()
        self.__bulk_expired = \
            self.__db['expired'].initialize_unordered_bulk_op()

        self.__push_id = self.__db['push_sessions'].insert({
            'region'    : self.__region,
            'realm'     : self.__realm,
            'time'      : self.__time,
            'ts_start'  : self.__ts_start,
            'done'      : False,
            'ts_done'   : None,
            })
        # с этого момента надо иметь ввиду,
        # что у нас может быть активна сессия загрузки данных
        return self.__push_id


    def set_AH(self, house):
        assert self.is_started()
        self.__house = house
        return


    def push(self, lot):
        """
        запихнём данные по лоту в базу
        объект лота - словарь
        """
        assert type(lot) is dict, "lot is not a dict but %s" % type(lot)
        assert self.is_started(), "push session not started"
        lot['region']   = self.__region
        lot['realm']    = self.__realm
        lot['house']    = self.__house
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
        assert self.is_started(), "push session not started"
        if self.__debug:
            print "update collections"

        clset = self.__openset - self.__seenset

        if self.__debug:
            printmap("set sizes" , {
                "openset" : len(self.__openset),
                "seenset" : len(self.__seenset),
                "clset"   : len(clset)})

        for id in clset:
            self.__lot_close(self.__opened[id])

        if 0 < self.__num_closed:
            self.__bulk_closed.execute()

        if 0 < self.__num_expired:
            self.__bulk_expired.execute()

        self.__ts_done = datetime.datetime.now()

        s = self.__db['push_sessions'].find({'_id':self.__push_id})[0]
        s['done']        = True
        s['ts_done']     = self.__ts_done
        s['statistic']   = {
                    'opened'        : self.__num_opened,
                    'closed'        : self.__num_closed,
                    'raised'        : self.__num_raised,
                    'adjusted'      : self.__num_adjusted,
                    'expired'       : self.__num_expired,
                }
        s['sizes']       = {
                    'opened'    : len(self.__opened),
                    'closed'    : self.__db['closed'].count(),
                    'expired'   : self.__db['expired'].count(),
                }

        if self.__debug:
            print "... session finished, spent : %s " \
                % str(s['ts_done'] - s['ts_start'])
            printmap("    stats", s['statistic'])
            printmap("    sizes", s['sizes'])

        self.__db['push_sessions'].save(s)
        self.__push_id = None
        return


    def find_prevtime(self):
        self.__prevtime = None
        try:
            self.__prevtime = self.__db['push_sessions'].find({
                'region'    : self.__region,
                'realm'     : self.__realm,
                'done'      : True,
                }).sort('time', pymongo.DESCENDING).limit(1)[0]['time']
        except IndexError:
            if self.__debug:
                print "? seems first session"
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
            self.__bulk_closed.insert(lot)
            self.__num_closed = self.__num_closed + 1
        else:
            lot['result'].append('expired')
            self.__bulk_expired.insert(lot)
            self.__num_expired = self.__num_expired + 1
        id = lot['auc']
        del self.__opened[id]
        self.__unsaved = True
        self.__openset.remove(id)
        return


    def recreate(self):
        assert self.is_connected()
        assert not self.is_started()
        assert not self.__unsaved
        print "*** [[[ RECREATE DATABASE ]]] ***"
        self.__client.drop_database(self.__dbname)
        self.__db = self.__client[self.__dbname]
#        self.__db['opened'].remove({})
#        self.__db['closed'].remove({})
#        self.__db['expired'].remove({})
#        self.__db['snapshot'].remove({})
#        self.__db['push_sessions'].remove({})
        self.create_indexes()
        print "*** [[[ DATABASE RECREATED ]]] ***"
        return


    def fix(self):
        """
        удаление данных от не полностью залитых сессий
        FIXME: очень рудиментарно, надо додумать
        """
        assert self.is_connected()
        assert not self.is_started()
        assert not self.__unsaved
        print "... fix sessions"
        self.__db['snapshot'].remove()
        self.__db['push_sessions'].remove({'done':False})
        return


    def create_indexes(self):
        assert self.is_connected()
        assert not self.is_started()
        assert not self.__unsaved
        print "... create indexes"
        self.__db['opened'].create_index([('auc', pymongo.ASCENDING)])

        self.__db['closed'].create_index([('auc', pymongo.ASCENDING)])
        self.__db['closed'].create_index([
            ('region', pymongo.ASCENDING),
            ('realm', pymongo.ASCENDING),
            ('house', pymongo.ASCENDING),])
        self.__db['closed'].create_index([('time.opened', pymongo.ASCENDING)])
        self.__db['closed'].create_index([('time.lastseen', pymongo.ASCENDING)])
        self.__db['closed'].create_index([('time.closed', pymongo.ASCENDING)])

        self.__db['expired'].create_index([('auc', pymongo.ASCENDING)])
        self.__db['expired'].create_index([
            ('region', pymongo.ASCENDING),
            ('realm', pymongo.ASCENDING),
            ('house', pymongo.ASCENDING),])
        self.__db['expired'].create_index([('time.opened', pymongo.ASCENDING)])
        self.__db['expired'].create_index([('time.lastseen', pymongo.ASCENDING)])
        self.__db['expired'].create_index([('time.closed', pymongo.ASCENDING)])

        self.__db['push_sessions'].create_index([
            ('region', pymongo.ASCENDING),
            ('realm', pymongo.ASCENDING),
            ('done', pymongo.ASCENDING),
            ('time', pymongo.DESCENDING)])
        return

### EOF ###
if __name__ == '__main__':
    print mapfmt('test format', {
        u'кака'     :       10,
        u'бяка'     :     1100,
        u'и'        :       12,
        u'закаляка' : 12345678})
