#!/usr/bin/env python
# encoding: utf-8

import os


def h_log(log_string):

    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'text_of_samer.log')
    new_fp = open(file_path, 'a')
    try:
        new_fp.write(str(log_string) + '\n')
        new_fp.flush()
    except:
        print 'h_log failed.'
