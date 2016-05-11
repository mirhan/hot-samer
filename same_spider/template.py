#!/usr/bin/env python
# encoding: utf-8

from hi_log import *

FILENAME = r'top.html'

head = r'''<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<style>
div.img {
    border: 1px solid #ccc;
}

div.img:hover {
    border: 1px solid #777;
}

div.img img {
    width: 100%;
    height: auto;
    border-bottom: 1px solid #ccc;
}

div.desc {
    padding: 15px;
    text-align: center;
    font-family: "Microsoft YaHei" ! important;
}

* {
    box-sizing: border-box;
}

.responsive {
    padding: 0 6px;
    float: left;
    width: 19.99999%;
}

@media only screen and (max-width: 700px){
    .responsive {
        width: 49.99999%;
        margin: 6px 0;
    }
}

@media only screen and (max-width: 500px){
    .responsive {
        width: 100%;
    }
}

.clearfix:after {
    content: "";
    display: table;
    clear: both;
    height:30px;
}
</style>
</head>
<body>

<h2 style="text-align:center">Responsive Image Gallery</h2>

'''

tail = r'''</body>
</html>
'''


def generate_html(samer_list, logfile=FILENAME):
    clear_h_log(logfile=logfile)
    h_log(head, logfile=logfile)

    for i, samer in enumerate(samer_list):
        if '_source' not in samer:
            continue
        if 'photo' not in samer['_source']:
            continue

        photo = samer['_source']['photo']
        author_name = samer['_source']['author_name']
        likes = samer['_source']['likes']
        if photo:
            s = u'''<div class="responsive">
  <div class="img">
    <a target="_blank" href="%s">
      <img src="%s" alt="%s">
    </a>
    <div class="desc">@%s<br/>%s ❤</div>
  </div>
</div>''' % (photo, photo, author_name, author_name, likes)
            h_log(s, logfile=logfile)

        if i % 5 == 4:
            h_log(r'<div class="clearfix"></div>', logfile=logfile)

    pass
    h_log(tail, logfile=logfile)
