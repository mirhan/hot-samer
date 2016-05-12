#!/usr/bin/env python
# encoding: utf-8

import codecs
import os
import pickle


DATA_DIR = r'data'
DEFAULT_PK = r'default.pk'

all_cid_list = set((
    1125933,  # ST
    1085548,  # QXG of admin,
    1033563,  # QXG
    1228982,  # PRK,
    967,      # YMDZPK
    1032823,  # ACUP
    1015326,  # 我这么美我不能死
    1097342,  # 你觉得好看的samers
    1104060,  # 卡他
    1166214,  # DALUK
    1140084,  # 酒精胶囊
)
)


def load_p(filename=DEFAULT_PK):
    pk_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), DATA_DIR, filename)

    try:
        with codecs.open(pk_path, 'r+') as f:
            x = pickle.load(f)
            f.close()
            return x
        return None
    except:
        print 'load_p failed'
        return None


def dump_p(c_list, filename=DEFAULT_PK):
    pk_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), DATA_DIR, filename)

    with codecs.open(pk_path, 'w+') as f:
        pstr = pickle.dump(c_list, f)
        f.close()
        return pstr

if __name__ == '__main__':
    x = load_p('d')
    print x
    # if x:
    #     print x
    #     for i in x:
    #         print i
    #     x.add('中文')
    # dump_p(all_cid_list, filename='all_cid_list.pk')
