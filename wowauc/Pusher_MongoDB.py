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


class Pusher_MongoDB (object):
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
        self.__bulk     = None
        self.__bulk_dirty   = False
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


    def touch_realm(self, region, realm, slug, locale):
        if self.__debug:
            print "touch realm (%s, %s, %s, %s)" \
                % (region, realm, slug, locale)
        assert self.is_connected(), "not connected to database"
        return


    def need(self, region, realm, house, wowts):
        """
        проверить, нужно ли добавление данных для данного времени
        """
        assert self.is_connected(), "not connected to database"

        return self.__db['push_sessions'].find({
            'region'    : region,
            'realm'     : realm,
            'house'     : house,
            'time'      : {'$gte': wowts},
            'done'      : True,}).count() == 0


    def is_started(self):
        """
        возвращаем True если активен сеанс добавления данных
        """
        return self.is_connected() and self.__push_id is not None


    def start(self, region, realm, house, wowts):
        """
        начинаем сессию добавления данных для аукциона
        """
        if self.__debug:
            print "start push session for (%s, %s, %s, %s)" \
            % (region, realm, house, wowts)
        assert self.is_connected(), "not connected to database"
        assert self.need(region, realm, house, wowts), "duplicate/obsolete"
        assert not self.is_started(), "push session already started"

        self.__region   = region
        self.__realm    = realm
        self.__house    = house
        self.__time     = wowts
        self.__ts_start = datetime.datetime.now()

        self.__push_id = self.__db['push_sessions'].insert({
            'region'    : self.__region,
            'realm'     : self.__realm,
            'house'     : self.__house,
            'time'      : self.__time,
            'ts_start'  : self.__ts_start,
            'done'      : False,
            'ts_done'   : None,
            })
        # с этого момента надо иметь ввиду,
        # что у нас может быть активна сессия загрузки данных
        snapshot = self.__db['snapshot']
        snapshot.create_index([('auc', pymongo.ASCENDING)])
        self.__bulk = snapshot.initialize_unordered_bulk_op()
        self.__bulk_dirty = False
        return self.__push_id


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
        # self.__db['snapshot'].insert(lot)
        self.__bulk.insert(lot)
        self.__bulk_dirty = True
        return


    def abort(self):
        """
        отмена push-сеанса и откат данных
        """
        if self.__debug:
            print "abort push session"
        assert self.is_started(), "push session not started"
        self.__bulk = None
        self.__bulk_dirty = False
        self.__push_id = None
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
        ret = self.__process_snapshot()
        if self.__debug:
            print "... session finished, spent : %s " \
                % str(ret['ts_done'] - ret['ts_start'])
            print mapfmt("    stats", ret['statistic'])
            print mapfmt("    sizes", ret['sizes'])
        self.__push_id = None
        return ret


    def __process_snapshot(self):
        #
        # 1. для каждого открытого аукциона ищем соответствие в снапшоте
        #    если нашли - смотрим, изменилась ли текущая ставка,
        #    и удаляем запись из снапшота
        #    если не нашли - перемещаем в закрытые аукционы,
        #    выставляем в нём поле success согласно нашим предположениям
        #    о том, был ли лот выкуплен или же был просрочен
        # 2. оставшиеся записи снапшота перемещаем в открытые аукционы
        #

        if self.__bulk_dirty:
            if self.__debug:
                print "execute for collected bulk"
            result = self.__bulk.execute()
        self.__bulk = None
        self.__bulk_dirty = None

        opened = self.__db['opened']
        closed = self.__db['closed']
        expired = self.__db['expired']
        snapshot = self.__db['snapshot']

        bulk_opened     = opened.initialize_unordered_bulk_op()
        bulk_closed     = closed.initialize_unordered_bulk_op()
        bulk_expired    = expired.initialize_unordered_bulk_op()
        bulk_snapshot   = snapshot.initialize_unordered_bulk_op()
        bulk_opened_dirty   = False
        bulk_closed_dirty   = False
        bulk_expired_dirty  = False
        bulk_snapshot_dirty = False

        num_opened      = 0
        num_closed      = 0
        num_raised      = 0
        num_adjusted    = 0
        num_expired     = 0
        sssize = snapshot.count()

#        print "snapshot.count() = %d" % snapshot.count()
#        print "opened.count()   = %d" % opened.count()

        for auc in opened.find({
            'region'    : self.__region,
            'realm'     : self.__realm,
            'house'     : self.__house}):
            found = False
            for r in snapshot.find({'auc':auc['auc']}):
                found = True
                changed = False
                auc['time']['lastseen'] = r['time']
                if auc['bid'] != r['bid']:
#                    if self.__debug:
#                        print "[%d]: bid raised from %d to %d" % (
#                            auc['auc'], auc['bid'], r['bid'])
                    auc['bid'] = r['bid']
                    auc['time']['raised'] = r['time'];
                    num_raised = num_raised + 1
                if auc['timeLeft'] != r['timeLeft']:
                    # сменился диапазон времени
                    e = auc['time']['deadline']
                    # предполагаем, что это произошло где-то
                    # между предыдущим и текущим обновлением
                    # но не позже старого deadline
                    t_hit = (auc['time']['lastseen'] +
                        datetime.timedelta(0, int(
                            (r['time'] - auc['time']['lastseen'])
                            .total_seconds() * random.random())))
                    if auc['time']['deadline'] < t_hit:
                        t_hit = auc['time']['deadline']
                    # это гарантированно изменение интервала,
                    # так что можно заложиться
                    # на максимально возможное значение
                    t = guess_expiration(t_hit, r['timeLeft'])['max']
                    auc['time']['deadline'] = t
                    num_adjusted = num_adjusted + 1
                # end if
                bulk_snapshot.find({'_id':r['_id']}).remove_one()
                bulk_snapshot_dirty = True
            #end for
            if found:
                bulk_opened.find({'_id': auc['_id']}).replace_one(auc)
                bulk_opened_dirty = True
            else:
                # opened auction not seen in last snapshot
                bulk_opened.find({'_id': auc['_id']}).remove_one()
                auc['time']['closed'] = self.__time
                is_raised = 'raised' in auc['time']
                is_expired = auc['time']['deadline'] < auc['time']['closed']
                auc['result'] = []

                if is_raised or not is_expired:
                    auc['result'].append('success')
                    if is_raised:
                        auc['result'].append('raised')
                    if not is_expired:
                        auc['result'].append('buyout')
                    bulk_closed.insert(auc)
                    bulk_closed_dirty = True
                    num_closed = num_closed + 1
                else:
                    auc['result'].append('expired')
                    bulk_expired.insert(auc)
                    bulk_expired_dirty = True
                    num_expired = num_expired + 1
            #end if
        #end for

        # обработали все opened записи.
        # удалим все обработанные snapshot-записи
        if bulk_snapshot_dirty:
            result = bulk_snapshot.execute()
        del bulk_snapshot
        del bulk_snapshot_dirty

#            if self.__debug:
#                printmap("bulk_snapshot execution result", result)
#        elif self.__debug:
#                print "bulk_snapshot is clean."

        # найдём, когда была предыдущая заливка
        ts_prev = None
        try:
            ts_prev = self.__db['push_sessions'].find({
                'region'    : self.__region,
                'realm'     : self.__realm,
                'house'     : self.__house,
                'done'      : True,
                }).sort('time', pymongo.DESCENDING).limit(1)[0]['time']
        except IndexError:
            if self.__debug:
                print "? previous session not found. seems first session ever )"

        # add all unprocessed recors in snapshot as new opened lots
        for auc in snapshot.find():
            t = auc['time']
            t_dead1 = guess_expiration(self.__time, auc['timeLeft'])['min']
            if ts_prev is None:
                # мы не знаем, когда был открыт этот аукцион даже примерно
                # значит выставим минимальное время
                t_deadline = t_dead1
            else:
                # в прошлый раз этого лота не было
                t_deadline = guess_expiration(ts_prev, auc['timeLeft'])['max']
                if t_deadline < t_dead1:
                    # но слишком уж много времени прошло с того раза :)
                    t_deadline = t_dead1

            auc['time'] = {
                'opened'    : t,
                'lastseen'  : t,
                'closed'    : None,
                'raised'    : None,
                'deadline'  : t_deadline
            }
            # import pdb; pdb.set_trace()
            bulk_opened.insert(auc)
            bulk_opened_dirty = True
#            if self.__debug:
#                print "[%d] opened" % (auc['auc'])
            num_opened = num_opened + 1
        #end for

        # применяем всё обработанное окончательно

        if bulk_opened_dirty:
            result = bulk_opened.execute()
#            if self.__debug:
#                printmap("bulk_opened execution result", result)
#        elif self.__debug:
#                print "bulk_opened is clean"

        if bulk_closed_dirty:
            result = bulk_closed.execute()
#            if self.__debug:
#                printmap("bulk_closed execution result", result)
#        elif self.__debug:
#                print "bulk_closed is clean"

        if bulk_expired_dirty:
            result = bulk_expired.execute()
#            if self.__debug:
#                printmap("bulk_closed execution result", result)
#        elif self.__debug:
#                print "bulk_expired is clean"

        self.__db.drop_collection(snapshot)

        self.__db['push_sessions'].update(
            {'_id':self.__push_id},
            {'$set': {
                'done':True,
                'ts_done': datetime.datetime.now(),
                'statistic': {
                    'opened'        : num_opened,
                    'closed'        : num_closed,
                    'raised'        : num_raised,
                    'adjusted'      : num_adjusted,
                    'expired'       : num_expired,
                },
                'sizes': {
                    'snapshot' : sssize,
                    'opened'   : opened.count(),
                    'closed'   : closed.count(),
                    'expired'  : expired.count(),
                },
            }})
        # end for
        return self.__db['push_sessions'].find({'_id':self.__push_id})[0]


    def recreate(self):
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
        print "... fix sessions"
        self.__db['snapshot'].remove()
        self.__db['push_sessions'].remove({'done':False})
        return


    def create_indexes(self):
        print "... create indexes"
        self.__db['opened'].create_index([('auc', pymongo.ASCENDING)])
        self.__db['closed'].create_index([('auc', pymongo.ASCENDING)])
        self.__db['expired'].create_index([('auc', pymongo.ASCENDING)])

        self.__db['closed'].create_index([
            ('region', pymongo.ASCENDING),
            ('realm', pymongo.ASCENDING),
            ('house', pymongo.ASCENDING),])
        self.__db['closed'].create_index([('time.opened', pymongo.ASCENDING)])
        self.__db['closed'].create_index([('time.lastseen', pymongo.ASCENDING)])
        self.__db['closed'].create_index([('time.closed', pymongo.ASCENDING)])

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
            ('house', pymongo.ASCENDING),
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
