#!/usr/bin/env python
# encoding: utf-8

import sys
import json
import time
import requests
import datetime
import gevent
# import random
from gevent import monkey
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from spider_same import get_channels_ids_with_cate_id
from spider_same import get_photo_url_with_channel_id
from send_same import get_user_profile
from send_same import get_user_recent_ugc_list
from secret import header
# from same_datas import lovely_cid_list
from same_datas import *

from hi_log import h_log


es = Elasticsearch()
# es.indices.create(index='same', ignore=400)

monkey.patch_all()


def get_multi_rank_likes(cid, pages=3):
    results_list = []
    for page in range(pages):
        if page == 0:
            url = "https://v2.same.com/activity/senses/channel/%s?order=hostest&from=-30 day" % cid
        else:
            url = "https://v2.same.com/activity/senses/channel/%s?order=hostest&from=-30 day&offset=%s" % (cid, page)
        try:
            res = requests.get(url, verify=False, headers=header)
            data = json.loads(res.text)
            results_list.extend(data.get('data', {}).get('results', []))
        except Exception, e:
            print 'parse err', e, url
        # gevent.sleep(random.randint(1,3))
    return results_list


def collect_likes_rank_data(cid):
    bulk_list = []
    results_list = get_multi_rank_likes(cid)
    print 'got data done, cid: %s, data len: %s' % (cid, len(results_list))
    # gevent.sleep(random.randint(1,2))
    for ugc in results_list:
        if 'photo' not in ugc:
            continue
        if len(ugc.get('txt', '')) > 1024:
            # same也不扫下垃圾,还要自己skip下
            continue
        action = {
            "_index": "same",
            "_type": "user_ugc",
            "_id": ugc['id'],
            '_source': {
                'author_uid': ugc['user']['id'],
                'id': ugc['id'],
                'txt': ugc.get('txt', ''),
                'photo': ugc.get('photo', ''),
                'likes': ugc['likes'],
                'views': ugc['views'],
                'timestamp': datetime.datetime.fromtimestamp(int(float(ugc['created_at']))),
                'created_at': ugc['created_at'],
                'channel_id': ugc['channel']['id'],
                'author_name': ugc['user']['username'],
            }
        }
        bulk_list.append(action)
    print 'collect rank data: ', helpers.bulk(es, bulk_list)


def collect_user_recent_ugc(uid):
    recent_ugc_list = get_user_recent_ugc_list(uid)
    bulk_list = []
    for ugc in recent_ugc_list:
        action = {
            "_index": "same",
            "_type": "user_ugc",
            "_id": ugc['id'],
            '_source': {
                'author_uid': uid,
                'id': ugc['id'],
                'txt': ugc.get('txt', ''),
                'photo': ugc.get('photo', ''),
                'likes': ugc['likes'],
                'views': ugc['views'],
                'timestamp': datetime.datetime.fromtimestamp(int(float(ugc['created_at']))),
                'created_at': ugc['created_at'],
                'channel_id': ugc['channel']['id'],
                'author_name': ugc['user']['username'],
            }
        }
        bulk_list.append(action)
    if recent_ugc_list:
        print 'collect ugc length {}, uid: {}'.format(helpers.bulk(es, bulk_list), uid)
    return len(recent_ugc_list)


def collect_profile_data(uid):
    # now = time.time()
    # max_interval = 86400 * 30 * 12
    profile = get_user_profile(uid)
    # gevent.sleep(0.1)
    if not profile or not profile.get('user'):
        print 'not profile data', uid
        return
    # recent_ugc_times = collect_user_recent_ugc(uid)
    # gevent.sleep(0.1)
    # if now - int(float(last_sense.get('created_at', 0))) > max_interval:
    # if recent_ugc_times < 1:
    #     print 'skip user', uid
    #     return
    if int(profile['user']['senses']) < 1 or int(profile['user']['channels']) < 3:
        # 没有给过别人同感和频道数过少的, 就skip算了
        return
    body = profile['user']
    # body = {
    #     'id': profile['id'],
    #     'views': r['views'],
    #     'likes': r['likes'],
    #     'timestamp': datetime.datetime.fromtimestamp(int(float(r['created_at']))),
    #     'created_at': datetime.datetime.fromtimestamp(int(float(r['created_at']))),
    #     'photo_url': r.get('photo', ''),
    #     'channel_id': r['channel']['id'],
    #     'author_name': r['user']['username'],
    #     'author_id': r['user']['id'],
    # }
    # body['ugc_times'] = recent_ugc_times
    body['ugc_times'] = 0
    body['timestamp'] = datetime.datetime.fromtimestamp(body.get('created_at', time.time()))
    return body
    # try:
    #     print es.index(index='same', doc_type='user_profile', id=int(profile['user']['id']), body=body)
    # except Exception, e:
    #     print 'es error:', e, uid, body


def collect_profile_data_multi(uids):
    bulk_list = []
    for uid in uids:
        source = collect_profile_data(uid)
        if source:
            bulk_list.append({
                "_index": "same",
                "_type": "user_profile",
                "_id": source['id'],
                "_source": source
            })
        # gevent.sleep(0.05)
        # if len(bulk_list) % 1000 == 0:
        #     print bulk_list
        #     print 'collect profile count:', helpers.bulk(es, bulk_list)
        #     bulk_list = []
        if len(bulk_list) > 100:
            print 'had collect profile count:', helpers.bulk(es, bulk_list)
            bulk_list = []

    if bulk_list:
        print 'had collect profile count:', helpers.bulk(es, bulk_list)


def insert_ugc_into_es(result_list):
    bulk_list = []
    for ugc in result_list:
        action = {
            "_index": "same",
            "_type": "user_ugc",
            "_id": int(ugc['id']),
            '_source': {
                'author_uid': ugc['user']['id'],
                'id': ugc['id'],
                'txt': ugc.get('txt', ''),
                'photo': ugc.get('photo', ''),
                'likes': ugc['likes'],
                'views': ugc['views'],
                'timestamp': datetime.datetime.fromtimestamp(int(float(ugc['created_at']))),
                'created_at': ugc['created_at'],
                'channel_id': ugc['channel']['id'],
                'channel_cate': ugc['channel']['cate'],
                'author_name': ugc['user']['username'],
            }
        }
        bulk_list.append(action)
    print 'collect to es data length {}'.format(helpers.bulk(es, bulk_list))


def collect_single_channel_data(cid, max_expire=3600):
    recent_ugc_list = []
    result_list, next_uri = get_photo_url_with_channel_id(cid)
    recent_ugc_list.extend(result_list)
    while len(result_list) > 0 and next_uri:
        result_list, next_uri = get_photo_url_with_channel_id(cid, next_uri=next_uri)
        if result_list:
            if time.time() - int(float(result_list[-1]['created_at'])) > max_expire:
                break
        recent_ugc_list.extend(result_list)
        # gevent.sleep(0.1)
        if len(recent_ugc_list) % 1000 == 0:
            insert_ugc_into_es(recent_ugc_list)
            recent_ugc_list = []
    if recent_ugc_list:
        insert_ugc_into_es(recent_ugc_list)


def get_latest_channels(filter_cate_ids=None, max_expire=3600):
    channel_info_list = []
    result_list, next_uri = get_latest_channels_url()
    # if filter_cate_ids:
    #     result_list = [x for x in result_list if x['cate'] in filter_cate_ids or []]
    channel_info_list.extend(result_list)
    while len(result_list) > 0 and next_uri:
        result_list, next_uri = get_latest_channels_url(next_uri)
        if result_list:
            if time.time() - int(next_uri.split('=')[1][:10]) > max_expire:
                break
            # if filter_cate_ids:
            #     result_list = [i for i in result_list if int(i['cate']) in filter_cate_ids]
        channel_info_list.extend(result_list)
    for channel_info in channel_info_list:
        collect_single_channel_data(channel_info['id'], max_expire=3600 * 2)


def get_latest_channels_url(next_uri=None):
    if not next_uri:
        url = 'https://v2.same.com/latest/channels'
    else:
        url = 'https://v2.same.com' + next_uri
    try:
        res = requests.get(url, verify=False, headers=header)
        data = json.loads(res.text)
        result = data['data']['results']
        if not result:
            return [], None
        return result, data['data']['next']
    except Exception, e:
        print 'get_latest_channels_url err', e, url
        return [], None


def get_music_channels():
    url = 'http://v2.same.com/channels/cate/3'
    try:
        res = requests.get(url, headers=header)
        if res.status_code == 200:
            return json.loads(res.text)['data']['results']
    except Exception, e:
        print 'get_music_channels err:', url, e
        return []


def get_popular_music_list_with_cid(cid, days=7, page=3):
    results_list = []
    for p in range(page):
        if p == 0:
            url = 'https://v2.same.com/activity/senses/channel/%s?order=hostest&from=-%sday' % (cid, days)
        else:
            url = 'https://v2.same.com/activity/senses/channel/%s?order=hostest&from=-%sday&offset=%s' % (cid, days, p)
        try:
            res = requests.get(url, verify=False, headers=header)
            if res.status_code == 200:
                data = json.loads(res.text)
                results_list.extend(data['data']['results'])
        except Exception, e:
            print 'parse err', e, url
    return results_list


def collect_popular_music_into_es():
    channels_ids = [i['id'] for i in get_music_channels()]
    for cid in channels_ids:
        music_list = get_popular_music_list_with_cid(cid, page=3)
        bulk_list = []
        for music in music_list:
            bulk_list.append({
                "_index": "same",
                "_type": "music",
                "_id": int(music['id']),
                '_source': {
                    'id': int(music['id']),
                    'views': int(music['views']),
                    'likes': music['likes'],
                    'created_at': music['created_at'],
                    'timestamp': datetime.datetime.fromtimestamp(int(music['created_at'])),
                    'author_uid': music['user']['id'],
                    'author_name': music['user']['username'],
                    'channel_id': music['channel']['id'],
                    'channel_cate': music['channel']['cate'],
                    'txt': music['txt'],
                    'music_id': music['media']['music']['id'],
                    'music_title': music['media']['music']['title'],
                    'music_src': music['media']['music']['src'],
                    'music_author': music['media']['music']['author'],
                    'music_sid': music['media']['music']['sid'],
                    'music_cover': music['media']['music']['cover'],
                }
            })
        print 'collect music:', helpers.bulk(es, bulk_list)


def update_channel_data(cid, start_uri=None, max_count=99999, insert_only=True):

    next_uri = start_uri or '/channel/%s/senses' % cid
    last_url = '/channel/%s/senses?offset=0' % cid

    recent_ugc_list = []
    data_exists = False
    for i in range(max_count):
        result_list, next_uri = get_photo_url_with_channel_id(cid, next_uri=next_uri)

        print '===', str(i), str(cid)

        recent_ugc_list.extend(result_list)

        if start_uri == last_url or next_uri is None:
            break

        if insert_only:
            for ugc in result_list:
                data_exists = es.exists(index="same", doc_type="user_ugc", id=ugc['id'])
                if data_exists:
                    break

        if len(recent_ugc_list) > 100:
            insert_ugc_into_es(recent_ugc_list)
            recent_ugc_list = []

        if data_exists:
            break

    if recent_ugc_list:
        insert_ugc_into_es(recent_ugc_list)


def update_channels(cid_list):
    gs = []
    for i, cid in enumerate(cid_list):
        gs.append(gevent.spawn(update_channel_data, cid=cid, start_uri=None))
        if i + 1 % 100 == 0:
            gevent.joinall(gs)
            gs = []

    if gs:
        gevent.joinall(gs)

if __name__ == "__main__":
    if sys.argv[1] == 'get_photo':
        channels_ids = get_channels_ids_with_cate_id(2, offset=None)
        gs = []
        for page in range(1, 3):
            channels_ids.extend(get_channels_ids_with_cate_id(2, offset=page))
            time.sleep(1)

        print channels_ids
        for cid in list(set(channels_ids)):
            collect_likes_rank_data(cid)
            # gs.append(gevent.spawn(collect_likes_rank_data, cid))
        # gevent.joinall(gs)
    elif sys.argv[1] == 'get_zipai':
        # 你拍我画频道
        collect_single_channel_data(1312542)
    elif sys.argv[1] == 'get_latest_channels':
        get_latest_channels(600)
    elif sys.argv[1] == 'get_x':
        #
        # 1125933 ST 1085548 ADMIN QXG 1033563 QXG 1228982 PRK  967 YMDZPK 1032823 ACUP
        # 1015326 我这么美我不能死 1097342 你觉得好看的samers 1104060 卡他 1166214 DALUK 1140084 酒精胶囊
        # 1021852 每天为生活拍一张照片 1312542 NFZPWLH 1276224 我发照片你来点赞 1099203 眼镜自拍
        for cid in [1032823, 1033563, 1228982, 1312542, 967, 1021852, 1276224, 1099203]:
            if cid == 1312542:
                collect_single_channel_data(cid, 3600)  # 这个频道貌似内容太频繁了
            else:
                collect_single_channel_data(cid, 86400 * 1)
            collect_likes_rank_data(cid)
    elif sys.argv[1] == 'get_photography':
        # iphone摄影和instagrammer 频道,影子频道
        for cid in [1002974, 1001617, 1187908]:
            collect_single_channel_data(cid, 86400 * 1)
            collect_likes_rank_data(cid)

    elif sys.argv[1] == 'get_profile':
        gs = []
        offset = 1000
        init_uid = 4500000
        while init_uid < 4510000:
            gs.append(gevent.spawn(collect_profile_data_multi, range(init_uid, init_uid + offset)))
            init_uid += offset
        print 'start gevent done, count:%d' % len(gs)
        gevent.joinall(gs)

    elif sys.argv[1] == 'get_music':
        collect_popular_music_into_es()

    elif sys.argv[1] == 'get_qxg':
        # start_uri = '/channel/1085548/senses?offset=14516590920026096804'
        update_channel_data(1015326, start_uri=None, max_count=99999)

    elif sys.argv[1] == 'update':
        update_channels(all_cid_list)
