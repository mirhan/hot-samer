#!/usr/bin/env python
# encoding: utf-8

import sys

from same_datas import all_cid_list
from collect_data_into_es import update_channels
from scan_all import get_last_day_top
from template import generate_html


def generate_last_day_top_html(top_len=50):
    update_channels(all_cid_list)
    samers = get_last_day_top(top_len)
    generate_html(samers, logfile='last_day_top.html')

if __name__ == '__main__':
    if sys.argv[1] == 'last_day':
        generate_last_day_top_html()
