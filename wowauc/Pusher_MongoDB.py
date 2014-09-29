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
#import pprint
#pp = pprint.PrettyPrinter(indent = 4)

minutes_left = {
    'SHORT'     : (       0,      30 ),
    'MEDIUM'    : (      30,  2 * 60 ),
    'LONG'      : (  2 * 60, 12 * 60 ),
    'VERY_LONG' : ( 12 * 60, 48 * 60 ),
}

def guess_expiration(t, timeLeft):
    assert type(t) is datetime.datetime
    p = minutes_left[timeLeft]
    t_min = t + datetime.timedelta(0, 60 * p[0])
    t_max = t + datetime.timedelta(0, 60 * p[1])
    return {'min':t_min, 'max':t_max}


def mapfmt(name, M):
    spc = '\n' + (' ' * (len(name) + 3))
    maxlen1 = max((len(unicode(x)) for x in M.keys()))
    maxlen2 = max((len(unicode(x)) for x in M.values()))
    return name + ' : ' + (
        spc.join(("%-*s = %*s" % ( maxlen1, unicode(x),
                                    maxlen2, unicode(M[x]))
                for x in sorted(M.keys()))))


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
        return


    def connect(self, uri, dbname):
        """
        соединиться с базой данных
        """
        if self.__debug:
            print "create client for uri %s and base %s" % (uri, dbname)
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
        n = self.__db['snapshot'].count()
        if n > 0:
            print "REMOVE %d ITEMS FROM OLD SNAPSHOT" % n
            self.__db['snapshot'].remove({})
        # с этого момента надо иметь ввиду,
        # что у нас может быть активна сессия загрузки данных
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
        self.__db['snapshot'].insert(lot)
        return


    def abort(self):
        """
        отмена push-сеанса и откат данных
        """
        if self.__debug:
            print "abort push session"
        assert self.is_started(), "push session not started"
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
            print "finish push session"

        ret = self.__process_snapshot()
        if self.__debug:
            print "... session finished"
            print "    spent : %s " % str(ret['ts_done'] - ret['ts_start'])
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
        opened = self.__db['opened']
        closed = self.__db['closed']
        expired = self.__db['expired']
        snapshot = self.__db['snapshot']
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
                    auc['success'] = True
                    auc['time']['raised'] = r['time'];
                    num_raised = num_raised + 1
                if auc['timeLeft'] != r['timeLeft']:
                    e = auc['time']['deadline']
                    t = guess_expiration(r['time'], r['timeLeft'])
                    if t['max'] < e['max']:
#                        if self.__debug:
#                            print "[%d] expiration corrected %s->%s" % (
#                            auc['bid'],
#                            e['max'].strftime('%Y%m%d %H%M%S'),
#                            t['max'].strftime('%Y%m%d %H%M%S'))
                        auc['time']['deadline'] = t
                        num_adjusted = num_adjusted + 1
                # end if
                snapshot.remove({'_id': r['_id']})
            #end for
            if found:
                opened.update({'_id': auc['_id']}, auc)
            else:
                # opened auction not seen in last snapshot
                opened.remove ({'_id': auc['_id']})
                auc['time']['closed'] = self.__time
                is_success = auc['success']
                del auc['success']
                e = auc['time']['deadline']
                is_expired = e['min'] < auc['time']['closed']  # stricter
                auc['result'] = []

                if is_success or not is_expired:
                    auc['result'].append('success')
                    if auc['time']['raised']:
                        auc['result'].append('bid')
                    if not is_expired:
                        auc['result'].append('buyout')
                    closed.insert(auc)
                    num_closed = num_closed + 1
                else:
                    auc['result'].append('expired')
                    expired.insert(auc)
                    num_expired = num_expired + 1
            #end if
        #end for

        # add all unprocessed recors in snapshot as new opened lots
        for auc in snapshot.find():
            t = auc['time']
            auc['time'] = {
                'opened'    : t,
                'lastseen'  : t,
                'closed'    : None,
                'raised'    : None,
                'deadline'  : guess_expiration(self.__time, auc['timeLeft'])
            }
            # import pdb; pdb.set_trace()

            auc['success'] = False
            snapshot.remove({'_id': auc['_id']})
            opened.insert(auc)
#            if self.__debug:
#                print "[%d] opened" % (auc['auc'])
            num_opened = num_opened + 1
        #end for
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


    def erase(self):
        print "ERASE ALL DATA IN DATABASE"
        self.__db['opened'].remove({})
        self.__db['closed'].remove({})
        self.__db['expired'].remove({})
        self.__db['snapshot'].remove({})
        self.__db['push_sessions'].remove({})


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
        self.__db['snapshot'].create_index([('auc', pymongo.ASCENDING)])

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
            ('house', pymongo.ASCENDING)])

        self.__db['push_sessions'].create_index([
            ('time', pymongo.ASCENDING),
            ('done', pymongo.ASCENDING)])
        return

### EOF ###
if __name__ == '__main__':
    print mapfmt('test format', {
        u'кака'     :       10,
        u'бяка'     :     1100,
        u'и'        :       12,
        u'закаляка' : 12345678})
