#!/usr/bin/env python
# encoding: utf-8

import requests
import sys
import platform

import gevent
from gevent import monkey

from d_spider_list import get_has_been
from d_spider_list import update_todo_queue, get_todo_queue
from d_spider_list import get_all_cids
from secret import header
import json

monkey.patch_all()


def get_channel_url_response(url):
    if url:
        try:
            res = requests.get(url, verify=False, headers=header)
            data = json.loads(res.text)
            result = data['data']['results']
            if not result:
                print 'get_channel_url_response empty channel content:', url
                return [], None

            if 'next' in data['data']:
                next_url = 'https://v2.same.com' + data['data']['next']
            else:
                next_url = None
            return result, next_url
        except Exception, e:
            print 'get_channel_url_response ERROR:', e, url
            return [], None
    else:
        print 'get_channel_url_response ERROR: url =', str(url)


def scan_channel(cid):
    next_url = 'https://v2.same.com/channel/%s/senses' % str(cid)
    start_url = next_url
    has_been = [x for x in get_has_been() if str(cid) in x]
    todo_queue = [x for x in get_todo_queue() if str(cid) in x]
    urls = set()
    while True:
        print 'scan_channel: next_url =', next_url
        _, next_url = get_channel_url_response(url=next_url)

        if next_url not in has_been and next_url not in todo_queue:
            urls.add(next_url)
        else:
            print 'cid %s finished'
            break

        if start_url == next_url or not next_url:
            break

    update_todo_queue(urls)


def scan_channels(cids):

    cids = list(cids)

    n = 10
    cids_list = [cids[i:i + 10] for i in range(0, len(cids), n)]

    for cids in cids_list:
        gs = []
        for cid in cids:
            gs.append(gevent.spawn(scan_channel, cid=cid))
        gevent.joinall(gs)


if __name__ == '__main__':
    if platform.system() == 'Windows':
        requests.packages.urllib3.disable_warnings()

    if sys.argv[1] == 'c':
        cid = 1125933
        scan_channel(cid)
    elif sys.argv[1] == 'cids':
        scan_channels(get_all_cids())
    elif sys.argv[1] == 'test':
        print get_all_cids()
