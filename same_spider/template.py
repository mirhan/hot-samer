#!/usr/bin/env python
# encoding: utf-8

from hi_log import *

FILENAME = r'top.html'

head = r'''<html>
<head>
<meta charset="utf-8"/>
<style>
div.img {
    margin: 5px;
    border: 1px solid #ccc;
    float: left;
    width: 180px;
}

div.img:hover {
    border: 1px solid #777;
}

div.img img {
    width: 100%;
    height: auto;
}

div.desc {
    padding: 15px;
    text-align: center;
    font-family: "Microsoft YaHei" ! important;
}
</style>
</head>
<body>
'''

tail = r'''</body>
</html>
'''


def generate_html(samer_list):
    clear_h_log(logfile=FILENAME)
    h_log(head, logfile=FILENAME)

    for samer in samer_list:
        if '_source' not in samer:
            continue
        if 'photo' not in samer['_source']:
            continue

        if samer['_source']['photo']:

            s = r'''<div class="img">
    <a target="_blank" href="%s">
        <img src="%s" alt="Mountains">
    </a>
    <div class="desc">%s</div>
</div>''' % (samer['_source']['photo'], samer['_source']['photo'], samer['_source']['author_name'])
            h_log(s, logfile=FILENAME)
    pass
    h_log(tail, logfile=FILENAME)
