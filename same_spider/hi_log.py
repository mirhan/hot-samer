#!/usr/bin/env python
# encoding: utf-8

import os
import codecs

DEFAULT_LOG = r'h_log.log'


def make_log_path(filename):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    pass


def clear_h_log(logfile=DEFAULT_LOG):
    new_fp = codecs.open(make_log_path(logfile), 'w', encoding='utf8')
    new_fp.close()


def h_log(log_string, logfile=DEFAULT_LOG):

    # print log_string
    new_fp = codecs.open(make_log_path(logfile), 'a', encoding='utf8')
    try:
        # print type(log_string)
        new_fp.write(log_string + '\n')
        new_fp.flush()
    except Exception as e:
        print type(e)
        print str(e)
        print ''
    except:
        print 'h_log failed.'
