#!/usr/bin/python
# -*- coding: utf-8 -*-

import pprint
import MySQLdb

"""************************************************************
名称
    f_rule_big_schema_by_table_num
描述
    获得数据库指定Schema的表个数并计算扣分
参数
    p_parms        list            参数列表
    p_parms[0]     l_dbinfo        list    数据库连接信息
    p_parms[1]     l_username      string  数据库用户
    p_parms[2]     l_table_size    int     表的个数
    p_parms[3]     l_threshold     int     扣分阀值
    p_parms[4]     l_max_value     int     扣分上限
返回值
    dict{'scores':xxx,'records':xxx}
    'scores'    float   扣分
    'records'   list    表信息(Schema,表个数)
************************************************************"""
def f_rule_big_schema_by_table_num(p_parms):
    [l_dbinfo, l_username, l_table_size, l_weight, l_max_value] = [p_parms[0], p_parms[1], int(p_parms[2]), int(p_parms[3]), int(p_parms[4])]
    l_return_stru = {"scores": 0, "records": []}
    conn = MySQLdb.connect(host=l_dbinfo[0], user=l_dbinfo[3], passwd=l_dbinfo[4], db=l_dbinfo[2], charset="utf8", port=l_dbinfo[1])
    cursor = conn.cursor()
    cursor.execute("""
        select count(*)
        from information_schema.tables
        where table_schema='"""+l_username+"""'"""
        )
    records = cursor.fetchall()
    l_return_stru["records"] = records

    if int(records[0][0]) > l_table_size:
        l_return_stru["scores"] = float('%0.2f' % float(l_max_value))
    else:
        l_return_stru["scores"] = 0.0

    cursor.close()
    conn.close()
    return l_return_stru