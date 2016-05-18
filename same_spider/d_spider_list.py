#!/usr/bin/env python
# encoding: utf-8

from pickle_c import load_p, dump_p

HAS_BEEN = 'has_been.pk'
TODO_QUEUE = 'todo_queue.pk'


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
