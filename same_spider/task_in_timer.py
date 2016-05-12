#!/usr/bin/env python
# encoding: utf-8

import sys
from threading import Timer
from random import randint
from time import sleep

from scan_all import get_last_hour
from collect_data_into_es import update_spider
from download_same_photo import is_file_exists


LAST_HOUR_START = 0
LAST_HOUR_INTERVAL = 3
LAST_HOUR_SLEEP =0

UPDATE_START = 0
UPDATE_INTERVAL = 3
UPDATE_SLEEP =0


def callback_interval(interval, callback):
    print 'doing:', callback.__name__
    callback()
    Timer(interval, callback_interval, [interval, callback]).start()


def Timer_interval(start_delay=0, interval=5, callback=None):
    if callback is None:
        return

    Timer(start_delay, callback_interval, [interval, callback]).start()


def update_spider_sleep():
    sleep(randint(0, UPDATE_SLEEP))
    update_spider()

if __name__ == '__main__':

    if sys.argv[1] == 'task':
        Timer_interval(start_delay=LAST_HOUR_START, interval=LAST_HOUR_INTERVAL, callback=get_last_hour)
        Timer_interval(start_delay=UPDATE_START, interval=UPDATE_INTERVAL, callback=update_spider_sleep)

    elif sys.argv[1] == 'test':
        print is_file_exists(r'06219e758995a43005819fd57fd1fb02__c0_0_960_960__w960_h960.jpg', r'/Users/hanchang/Pictures/samer_pics')
        print is_file_exists(r'111.jpg', r'/Users/hanchang/Pictures/samer_pics')
        print is_file_exists(r'185', r'/Users/hanchang/Pictures/samer_pics')
        print is_file_exists(r'06219e758995a43005819fd57fd1fb02__c0_0_960_960__w960_h960.jpg', r'/Users/hanchang/Pictures/samer_pics1')
    