#!/usr/bin/env python
# encoding: utf-8

import os
import codecs

LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'samer_of_hour.html')

def clear_h_log():
    new_fp = codecs.open(LOG_PATH, 'w', encoding='utf8')
    new_fp.close()


def h_log(log_string):

    # print log_string
    new_fp = codecs.open(LOG_PATH, 'a', encoding='utf8')
    try:
        # print type(log_string)
        new_fp.write(log_string)
        new_fp.flush()
    except Exception as e:
        print type(e)
        print str(e)
        print ''
    except:
        print 'h_log failed.'
