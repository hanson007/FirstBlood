#!/usr/bin/python env
# -*- coding: UTF-8 -*-
# Description:                    
# Author:           黄小雪
# Date:             2017年07月04日
# Company:          东方银谷


class MailTable(object):
    """
    邮件html表格
    """
    def __init__(self):
        pass

    @property
    def style(self):
        _style = """
        <style type="text/css">
        table.imagetable {
            font-family: verdana,arial,sans-serif;
            font-size:11px;
            color:#333333;
            border-width: 1px;
            border-color: #999999;
            border-collapse: collapse;
        }
        table.imagetable th {
            background:#b5cfd2 url('cell-blue.jpg');
            border-width: 1px;
            padding: 8px;
            border-style: solid;
            border-color: #999999;
            white-space:nowrap;
        }
        table.imagetable td {
            background:#dcddc0 url('cell-grey.jpg');
            border-width: 1px;
            padding: 8px;
            border-style: solid;
            border-color: #999999;
            white-space:nowrap;
        }
        </style>
        """
        return _style

    def table(self, caption, rows):
        row0 = rows[0]
        tr0 = self._tr0_list(row0)
        total_tr_list = [self._tr_list(row) for row in rows[1:]]
        tr_body = ''.join(total_tr_list)
        _table = """
        <table class="imagetable">
        <caption align="top">%s</caption>
        %s
        %s
        </table>
        """ % (caption, tr0, tr_body)
        return _table

    def _tr_list(self, row):
        _tr_list = ['<td>%s</td>' % r for r in row]
        _tr = '<tr>%s</tr>' % ''.join(_tr_list)
        return _tr

    def _tr0_list(self, row):
        _tr_list = ['<th>%s</th>' % r for r in row]
        _tr = '<tr>%s</tr>' % ''.join(_tr_list)
        return _tr

