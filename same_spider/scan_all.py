#!/usr/bin/env python
# encoding: utf-8

import sys
import gevent
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from collect_data_into_es import collect_profile_data_multi
from collect_data_into_es import collect_user_recent_ugc

from template import generate_html
from hi_log import *


FILE_PATH = r'/Users/hanchang/Developer/hot-samer_mirhan/trunk/same_spider/uids.log'


def get_file_lines(file_path):
    open_file = open(file_path, 'r+').readlines()

    for i in range(len(open_file)):
        open_file[i] = open_file[i].strip()

    return open_file


es = Elasticsearch()
client = Elasticsearch()

# es.indices.create(index='same', ignore=400)


def collect_user_recent_ugc_by_uids(uids):
    for i, uid in enumerate(uids):
        collect_user_recent_ugc(uid)

is_exists = {}


def get_last_hour():
    OFFSET = '+8h'  # TODO: didn't figuer out why
    time_range = {'gte': 'now-1h-30m' + OFFSET, 'lte': 'now-30m' + OFFSET}
    s = Search().using(client).filter('range', timestamp=time_range).sort('-likes')
    # response = s.execute()

    ret = []
    for i in s:
        print i.likes, i.timestamp, i.channel_id, i.photo
        ret.append(i)

    return ret


def get_last_day_top(top_len=50):
    OFFSET = '+8h'  # TODO: didn't figuer out why

    s = Search().using(client)
    time_range = {'gte': 'now-1d' + OFFSET, 'lte': 'now' + OFFSET}
    s_q = s.filter('range', timestamp=time_range).sort('-likes')[:100]
    r = s_q.execute()
    return r.hits.hits

    # logfile = 'tmp.log'
    # clear_h_log(logfile=logfile)
    # generate_html(r.hits.hits, logfile='top3.html')

    # channel_id = 1125933
    # s_q = s.query('match', channel_id=channel_id).filter('range', timestamp=time_range).sort('-likes')[:200]

if __name__ == "__main__":
    if sys.argv[1] == 'get_last_hour':
        get_last_hour()

    elif sys.argv[1] == 'get_top':
        get_last_day_top()

    elif sys.argv[1] == 'get_profiles':
        uids = get_file_lines(FILE_PATH)

        gs = []
        i = 0
        for i in range(len(uids)):
            if i % 5000 == 0:
                tmp_uids = uids[i: i + 5000]
                gs.append(gevent.spawn(collect_profile_data_multi, tmp_uids))

        gs.append(gevent.spawn(collect_profile_data_multi, uids[i:]))

        gevent.joinall(gs)
    elif sys.argv[1] == 'get_senses':
        uids = get_file_lines(FILE_PATH)
        # uids = [3787318]

        gs = []
        i = 0
        for i in range(len(uids)):
            gap = 5000
            if i % gap == 0:
                tmp_uids = uids[i: i + gap]
                print tmp_uids
                gs.append(gevent.spawn(collect_user_recent_ugc_by_uids, tmp_uids))

        gs.append(gevent.spawn(collect_user_recent_ugc_by_uids, uids[i:]))

        gevent.joinall(gs)

    elif 'scan_all':

        rs = es.search(index="same", body={"query": {"match_all": {}}}, search_type='scan', size=100, scroll='30s')

        sid = rs['_scroll_id']

        i = 0
        while 1:
            try:
                rs = es.scroll(scroll_id=sid, scroll='30s')

                samer_list = rs['hits']['hits']
                if not samer_list:
                    break
                # 1443974400 is a timestamp
                for samer in samer_list:
                    if (int(samer['_source']['created_at']) > 1443974400 and int(samer['_source']['likes']) > 0) \
                            or (int(samer['_source']['created_at']) <= 1443974400 and int(samer['_source']['likes']) > 20):

                        photo_url = samer['_source']['photo']
                        if 'imageMogr' in photo_url:
                            continue

                        # channel_id = 1097342

                        # print '1'

                        # print '0'
                        # print samer
                        # print samer['_source']
                        # print '1.1'
                        if samer['_source']['channel_id'] and int(samer['_source']['channel_id']) != 1097342:
                            continue

                        print '2'
                        if True:
                            # print samer
                            if samer['_source']['txt'] and samer['_source']['txt'] != '':
                                print '3'
                                print 'txt' in samer['_source']
                                print type(samer['_source']['txt'])
                                print '3.1'
                                # print
                                h_log(samer['_source']['txt'])
                                print '4'
                            continue

                        uid = samer['_source']['author_uid']
                        if uid not in is_exists:
                            is_exists[uid] = True

                            # print uid
                            h_log(uid)

                            i = i + 1
                            if i % 100 == 0:
                                print i

                sid = rs['_scroll_id']

            except:
                print 'error!!!'
                break
