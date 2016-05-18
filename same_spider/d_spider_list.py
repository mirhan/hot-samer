#!/usr/bin/env python
# encoding: utf-8

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, A

from pickle_c import load_p, dump_p

HAS_BEEN = 'has_been.pk'
TODO_QUEUE = 'todo_queue.pk'
HAS_BEEN_USER = 'has_been_user.pk'
TODO_QUEUE_USER = 'todo_queue_user.pk'

s = Search().using(Elasticsearch())


def get_has_been():
    has_been = load_p(filename=HAS_BEEN) or set()
    return has_been


def set_has_been(urls):
    if urls:
        dump_p(urls, filename=HAS_BEEN)


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
