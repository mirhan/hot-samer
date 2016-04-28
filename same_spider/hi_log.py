#!/usr/bin/env python
# encoding: utf-8

import string
import os

file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '131.log')


def h_log(log_string):

    new_fp = open(file_path, 'a')
    new_fp.write(str(log_string) + '\n')
    new_fp.flush()
