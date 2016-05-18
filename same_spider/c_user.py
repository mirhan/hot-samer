#!/usr/bin/env python
# encoding: utf-8

import requests
import sys
import platform

# from elasticsearch import Elasticsearch
# from elasticsearch_dsl import Search, A
# import gevent
# from gevent import monkey

from d_spider_list import get_has_been_user
from d_spider_list import addto_todo_queue_user
from secret import header
import json


def get_user_url_response(url):
    if url:
        try:
            res = requests.get(url, verify=False, headers=header)
            data = json.loads(res.text)
            print 'data'
            print data
            if int(data.get('code', 1)) == 0:
                return data['data'].get('results', []), data['data'].get('next')
            else:
                return [], None
        except ValueError as e:
            print 'get_user_url_response value Error', e, url, res.text
        except Exception, e:
            print 'get_user_url_response value Error:', e, url
    return [], None


def scan_user(uid):
    next_url = 'https://v2.same.com/user/%s/senses' % str(uid)
    start_url = next_url

    while True:
        print 'scan_user: next_url =', next_url
        _, next_url = get_user_url_response(url=next_url)
        if next_url in get_has_been_user():
            continue

        addto_todo_queue_user(next_url)

        if start_url == next_url or not next_url:
            break

if __name__ == '__main__':
    if platform.system() == 'Windows':
        requests.packages.urllib3.disable_warnings()

    if sys.argv[1] == 'uid':
        uid = 4171989
        scan_user(uid)
    # elif sys.argv[1] == 'cids':
    #     scan_channels(get_all_cids())
    # elif sys.argv[1] == 'test':
    #     print get_all_cids()
