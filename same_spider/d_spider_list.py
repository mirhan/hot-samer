#!/usr/bin/env python
# encoding: utf-8

import sys
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, A
from gevent.coros import BoundedSemaphore

from pickle_c import load_p, dump_p

HAS_BEEN = 'has_been.pk'
TODO_QUEUE = 'todo_queue.pk'
HAS_BEEN_USER = 'has_been_user.pk'
TODO_QUEUE_USER = 'todo_queue_user.pk'

s = Search().using(Elasticsearch())
sem_has_been = BoundedSemaphore(1)
sem = BoundedSemaphore(1)


def get_has_been():
    has_been = load_p(filename=HAS_BEEN) or set()
    return has_been


def set_has_been(urls):
    if urls:
        dump_p(urls, filename=HAS_BEEN)


def update_has_been(urls):
    sem_has_been.acquire()

    has_been = get_has_been()
    has_been.update(urls)
    set_has_been(has_been)
    print 'update_has_been: update lenth =', str(len(urls))

    sem_has_been.release()


def get_todo_queue():
    has_been = load_p(filename=TODO_QUEUE) or set()
    return has_been


def set_todo_queue(urls):
    if urls:
        dump_p(urls, filename=TODO_QUEUE)


def addto_todo_queue(url):
    urls = get_todo_queue()
    urls.add(url)
    set_todo_queue(urls)


def update_todo_queue(urls, remove=False):
    sem.acquire()
    todo_queue = get_todo_queue()
    if remove:
        todo_queue = [x for x in todo_queue if x not in urls]
    else:
        todo_queue.update(urls)
    set_todo_queue(todo_queue)
    print 'update_todo_queue: update lenth =', str(len(urls))
    sem.release()


####


def get_has_been_user():
    return load_p(filename=HAS_BEEN_USER) or set()


def set_has_been_user(urls):
    if urls:
        dump_p(urls, filename=HAS_BEEN_USER)
    else:
        print 'set_has_been_user ERROR, urls =', str(urls)


def get_todo_queue_user():
    return load_p(filename=TODO_QUEUE_USER) or set()


def set_todo_queue_user(urls):
    if urls:
        dump_p(urls, filename=TODO_QUEUE_USER)
    else:
        print 'set_todo_queue_user ERROR, urls =', str(urls)


def addto_todo_queue_user(url):
    urls = get_todo_queue()
    urls.add(url)
    set_todo_queue(urls)


def get_all_cids():
    a = A('terms', field='channel_id', size=0, order={"_count": "desc"})
    s.aggs.bucket('channel_ids', a)
    response = s.execute()
    try:
        return [i.key for i in response.aggregations.channel_ids.buckets]
    except Exception, e:
        print 'get_all_cids ERROR:', e
        return []


def get_all_uids():
    a = A('terms', field='author_uid', size=0)
    s.aggs.bucket('author_uids', a)
    response = s.execute()
    try:
        return [i.key for i in response.aggregations.author_uids.buckets]
    except Exception, e:
        print 'get_all_uids ERROR:', e
        return []
    pass

if __name__ == '__main__':
    if sys.argv[1] == 'test':
        print get_all_cids()
