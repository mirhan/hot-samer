#!/usr/bin/env python
# encoding: utf-8

import string
import os



def h_log(log_string, cid):

    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log\photos_100_' + str(cid) + '.log')
    new_fp = open(file_path, 'a')
    new_fp.write(str(log_string) + '\n')
    new_fp.flush()
