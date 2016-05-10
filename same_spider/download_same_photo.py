#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os, sys
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from same_datas import exclude_cid_list

# file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log\photos_100_' + str(cid) + '.log')
log_path = r'/mnt/developer/hot-samer_mirhan/trunk/same_spider/log/photos_100_002_004.log'
PIC_DIR = r'/Users/hanchang/Pictures/samer_pics'
DL_DIR = r'/Users/hanchang/Pictures/samer_pics/unclassified'


def is_file_exists(filename, rootdir):
    for _, _, files in os.walk(rootdir):
        for file in files:
            if filename == file:
                return True
    return False

def make_new_file_path(old_path):
    return 'one_more_time.log'
    # return old_path + "_filter_" + filter_str + '.log'

def download_url(_to, _from):
    if _to == None or _from == None:
        print 'error, _to = %s, _from = %s' % _to, _from
    else:
        _cmd = 'wget -P %s %s' % (_to, _from)
        os.system(_cmd)

def get_jpg_name(url):
    if url and 'imageMogr' not in url:
        return url.rsplit('/', 1)[-1]
    return None

def download_last_2_hour():
    OFFSET = '+8h'  # TODO: didn't figuer out why
    client = Elasticsearch()
    s = Search().using(client).filter('range', timestamp={'gte': 'now-3h' + OFFSET}).filter('range', likes={'gte':5})[:9999]

    for i in s:
        jpgname = get_jpg_name(i.photo)
        if not jpgname or is_file_exists(jpgname, PIC_DIR):
            continue

        if not i.channel_id or  int(i.channel_id) in exclude_cid_list:
            continue

        download_url(DL_DIR, i.photo)

if __name__ == '__main__':
    if sys.argv[1] == 'test':
        download_last_2_hour()

    # download_last_2_hour()
    # with open(log_path, 'r+') as openfileobject:
    #     threads = []
    #     for line in openfileobject:
    #         line = line.rstrip()

    #         file_path = os.path.join(PIC_DIR, get_jpg_name(line))
    #         if os.path.isfile(file_path):
    #             print 'file exists:' + str(file_path)
    #         else:
    #             a = threading.Thread(target=download_url,args=(PIC_DIR, line))
    #             threads.append(a)
    #             a.start()

    #             time.sleep(random.random())

    #     for x in threads:
    #         x.join()








