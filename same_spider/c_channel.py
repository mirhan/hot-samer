#!/usr/bin/env python
# encoding: utf-8

import requests
import sys
import platform

import gevent
from gevent import monkey

from d_spider_list import get_has_been
from d_spider_list import addto_todo_queue
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
    while True:
        print 'scan_channel: next_url =', next_url
        _, next_url = get_channel_url_response(url=next_url)
        if next_url in get_has_been():
            continue

        addto_todo_queue(next_url)

        if start_url == next_url or not next_url:
            break


def scan_channels(cids):
    gs = []
    for i, cid in enumerate(cids):
        gs.append(gevent.spawn(scan_channel, cid=cid))
        if i + 1 % 100 == 0:
            gevent.joinall(gs)
            gs = []

    if gs:
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
