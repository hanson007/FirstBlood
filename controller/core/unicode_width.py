#!/usr/bin/env python
#-*-coding:utf-8-*-


def get_max_length(arg):
    length = str_len(arg[0]) + 2
    return length


def sort_arg(arg):
    arg = list(arg)
    arg.sort(cmp=cmp_length)
    return arg


def cmp_length(a, b):
    la = str_len(a)
    lb = str_len(b)

    if la > lb:
        return -1
    elif la < lb:
        return 1
    else:
        return 0


def str_len(string):
    try:
        string = u'%s' % string
        row_l=len(string)
        utf8_l=len(string.encode('utf-8'))
        return (utf8_l-row_l)/2+row_l
    except:
        return row_l


def get_width(*var):
    out = zip(*var)
    res = map(sort_arg, out)
    width = map(get_max_length, res)
    return width


