#!/usr/bin/env python
# encoding: utf-8

import sys
import gevent
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from collect_data_into_es import collect_profile_data_multi
from collect_data_into_es import collect_user_recent_ugc

from hi_log import h_log


FILE_PATH = r'/Users/hanchang/Developer/hot-samer_mirhan/trunk/same_spider/uids.log'


def get_file_lines(file_path):
    open_file = open(file_path, 'r+').readlines()

    for i in range(len(open_file)):
        open_file[i] = open_file[i].strip()

    return open_file


es = Elasticsearch()
# es.indices.create(index='same', ignore=400)


def collect_user_recent_ugc_by_uids(uids):
    for i, uid in enumerate(uids):
        collect_user_recent_ugc(uid)


is_exists = {}

if __name__ == "__main__":
    # if sys.argv[1] == 'get_photo':
        # pass

    if sys.argv[1] == 'get_the_one':
        client = Elasticsearch()
        s = Search().using(client).query("match", created_at='1462802370')
        response = s.execute()

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

    else:

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

                        uid = samer['_source']['author_uid']
                        if not uid in is_exists:
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
