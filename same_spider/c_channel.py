#!/usr/bin/env python
# encoding: utf-8

import requests
import sys
import platform
import json
from functools import partial

import gevent
from gevent import monkey

from d_spider_list import *
from secret import header
from collect_data_into_es import insert_ugc_into_es

monkey.patch_all()
g_has_been = HasBeen()
g_todo_queue = TodoQueue()


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
        return [], None


def scan_channel(cid):
    next_url = 'https://v2.same.com/channel/%s/senses' % str(cid)
    start_url = next_url
    has_been = [x for x in g_has_been.get() if str(cid) in x]
    todo_queue = [x for x in g_todo_queue.get() if str(cid) in x]
    urls = []
    while True:
        print 'scan_channel: next_url =', next_url
        _, next_url = get_channel_url_response(url=next_url)

        if next_url not in has_been and next_url not in todo_queue:
            urls.append(next_url)
        else:
            print 'cid %s finished' % str(cid)
            break

        if start_url == next_url or not next_url:
            break

    g_todo_queue.update(urls)


def dump_channel(urls):
    ugcs = []
    for url in urls:
        print 'dump_channel:', url
        response, _ = get_channel_url_response(url=url)
        ugcs.extend(response)
    if ugcs:
        insert_ugc_into_es(ugcs)
        return 'OK'
    else:
        return 'ERR'


def move_to_has_been(urls, gs_0):
    print 'move_to_has_been, len(urls) = %d' % len(urls)
    g_todo_queue.update(urls, remove=True)
    g_has_been.update(urls)


def scan_dump_channels(cids):

    urls = list(g_todo_queue.get())
    n = 15
    urls_list = [urls[i:i + n] for i in range(0, len(urls), n)]
    gs = []
    for i, urls in enumerate(urls_list):
        gs.append(gevent.spawn(dump_channel, urls))
        if i % n == 0:
            [g.link_value(partial(move_to_has_been, g.args[0])) for g in gs]
            gevent.joinall(gs)
            gs = []

    if gs:
        [g.link_value(partial(move_to_has_been, g.args[0])) for g in gs]
        gevent.joinall(gs)
        gs = []

    cids = list(cids)

    n = 10
    cids_list = [cids[i:i + n] for i in range(0, len(cids), n)]
    for cids in cids_list:
        gs = [gevent.spawn(scan_channel, cid=cid) for cid in cids]
        gevent.joinall(gs)


if __name__ == '__main__':
    if platform.system() == 'Windows':
        requests.packages.urllib3.disable_warnings()

    if sys.argv[1] == 'c':
        cid = 1125933
        scan_channel(cid)
    elif sys.argv[1] == 'cids':
        scan_dump_channels(get_all_cids())
    elif sys.argv[1] == 'test':
        print get_all_cids()

    g_has_been.sync()
    g_todo_queue.sync()
