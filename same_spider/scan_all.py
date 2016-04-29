#!/usr/bin/env python
# encoding: utf-8

import sys
import json
import time
import requests
import datetime
import gevent
import random
from gevent import monkey
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from spider_same import get_channels_ids_with_cate_id
from spider_same import get_photo_url_with_channel_id
from send_same import get_user_profile
from send_same import get_user_recent_ugc_list
from secret import header

from hi_log import h_log






es = Elasticsearch()
# es.indices.create(index='same', ignore=400)

if __name__ == "__main__":
    # if sys.argv[1] == 'get_photo':
        # pass

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
                if int(samer['_source']['created_at']) > 1443974400 and 2 < int(samer['_source']['likes']) <=4:
                    
                    photo_url = samer['_source']['photo']
                    if 'imageMogr' in photo_url:
                        continue

                    # h_log(photo_url, samer['_source']['channel_id'])
                    if photo_url:
                        h_log(photo_url, "002_004")
                        i = i + 1
                        print i

            sid = rs['_scroll_id']
            
        except:
            print 'error!!!'
            break



