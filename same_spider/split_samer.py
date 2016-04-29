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

orignal_dir = r'D:\developer\pics\05_20_2'
# orignal_dir = '/mnt/developer/pics/20_9999'

es = Elasticsearch()

def get_jpg_name(url):
    if url:
        return url.rsplit('/', 1)[-1]
    return None


def get_jpg_name(url):
    print 'url'
    print url
    if url:
        # print 'get_jpg_name 1'
        if 'imageMogr' in url:
            return None
        # print 'get_jpg_name 2'
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
                # print '2'
                # 1443974400 is a timestamp
                # if int(samer['_source']['created_at']) > 1443974400 and int(samer['_source']['likes']) > 20:
                if int(samer['_source']['created_at']) > 1443974400:
                    
                    pic_name = get_jpg_name(samer['_source']['photo'])
                    # print '4.5'
                    if not pic_name:
                        continue;

                    pic_path = os.path.join(orignal_dir, pic_name)
                    # print '5'
                    if os.path.isfile(pic_path):
                        # mkdir
                        likes = samer['_source']['likes']
                        new_dir = os.path.join(orignal_dir, str(likes))
                        # print '3'
                        if not os.path.exists(new_dir):
                            print new_dir
                            os.mkdir(new_dir)
                            # print '3.5'
                        shutil.move(pic_path, new_dir)
                        pass

                # print '4'
            sid = rs['_scroll_id']
            
        except:
            print 'error!!!'
            break

