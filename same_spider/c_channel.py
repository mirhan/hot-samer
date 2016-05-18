#!/usr/bin/env python
# encoding: utf-8

import requests
import sys

from d_spider_list import get_has_been
from d_spider_list import addto_todo_queue
from secret import header
import json


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


if __name__ == '__main__':
    if sys.argv[1] == 'c':
        cid = 1125933
        scan_channel(cid)
