#!/usr/bin/env python
# encoding: utf-8

from pickle_c import load_p, dump_p

HAS_BEEN = 'has_been.pk'


def get_has_been():
    has_been = load_p(filename=HAS_BEEN) or set()
    return has_been


def set_has_been(urls):
    dump_p(urls, filename=HAS_BEEN)
