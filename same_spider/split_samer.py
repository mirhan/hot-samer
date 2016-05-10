#!/usr/bin/python
# -*- coding: UTF-8 -*-

import string
import re
import threading
import random
import os
import time
import shutil

from elasticsearch import Elasticsearch

ORIGINAL_DIR = r'/Users/hanchang/Pictures/pics'
NEW_DIR = r'/Users/hanchang/Pictures/samer_pics/old'
# ORIGINAL_DIR = '/mnt/developer/pics/20_9999'

es = Elasticsearch()

# def get_jpg_name(url):
#     if url:
#         return url.rsplit('/', 1)[-1]
#     return None


def get_jpg_name(url):
    if url and 'imageMogr' not in url:
        return url.rsplit('/', 1)[-1]
    return None

if __name__ == '__main__':
    rs = es.search(index="same", body={"query": {"match_all": {}}}, search_type='scan', size=100, scroll='30s')

    sid = rs['_scroll_id']

    i = 0
    while 1:
        try:
            rs = es.scroll(scroll_id=sid, scroll='30s')
            # print '1'
            samer_list = rs['hits']['hits']
            if not samer_list:
                break
            
            for samer in samer_list:
                if samer ['_type'] != 'user_ugc':  # type = 'user_ugc'
                    continue
                # print '2'
                # 1443974400 is a timestamp
                # if int(samer['_source']['created_at']) > 1443974400 and int(samer['_source']['likes']) > 20:
                # print
                # print samer['_source']['created_at']
                # print '2.1'
                if int(samer['_source']['created_at']) <= 1443974400:
                    
                    # print '2.2'
                    # print samer
                    # print '==='
                    # print 'join_at' in samer ['_source']
                    # print '2.3'
                    pic_name = get_jpg_name(samer['_source']['photo'])
                    # print '4.5'
                    if not pic_name:
                        continue;

                    pic_path = os.path.join(ORIGINAL_DIR, pic_name)
                    # print '5'
                    if os.path.isfile(pic_path):
                        # mkdir
                        likes = samer['_source']['likes']
                        new_dir = os.path.join(NEW_DIR, str(likes))
                        # print '3'
                        if not os.path.exists(new_dir):
                            # print new_dir
                            os.mkdir(new_dir)
                            # print '3.5'
                        shutil.move(pic_path, new_dir)
                        pass

                # print '4'
            sid = rs['_scroll_id']
            
        except:
            print 'error!!!'
            break

