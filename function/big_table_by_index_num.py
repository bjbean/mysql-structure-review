#!/usr/bin/python
# -*- coding: utf-8 -*-

import pprint
import MySQLdb

"""************************************************************
名称
    f_rule_big_table_by_index_num
描述
    获得数据库指定表的索引数并计算扣分
参数
    p_parms        list            参数列表
    p_parms[0]     l_dbinfo        list    数据库连接信息
    p_parms[1]     l_username      string  数据库用户
    p_parms[2]     l_index_num     int     索引数量
    p_parms[3]     l_threshold     int     扣分阀值
    p_parms[4]     l_max_value     int     扣分上限
返回值
    dict{'scores':xxx,'records':xxx}
    'scores'    float   扣分
    'records'   list    表信息(表名,索引数)
************************************************************"""
def f_rule_big_table_by_index_num(p_parms):
    [l_dbinfo, l_username, l_index_num, l_weight, l_max_value] = [p_parms[0], p_parms[1], int(p_parms[2]), int(p_parms[3]), int(p_parms[4])]
    l_return_stru = {"scores": 0, "records": []}
    conn = MySQLdb.connect(host=l_dbinfo[0], user=l_dbinfo[3], passwd=l_dbinfo[4], db=l_dbinfo[2], charset="utf8", port=l_dbinfo[1])
    cursor = conn.cursor()
    cursor.execute("""
        select * from
        (
            select table_name,count(distinct index_name) cnt
            from information_schema.STATISTICS
            where table_schema='"""+l_username+"""' and index_name<>'PRIMARY'
            group by table_name
        ) v where cnt>"""+str(l_index_num)
        )
    records = cursor.fetchall()
    l_return_stru["records"] = records

    if (len(records)*float(l_weight)) > float(l_max_value):
        l_return_stru["scores"] = float('%0.2f' % float(l_max_value))
    else:
        l_return_stru["scores"] = float('%0.2f' % (len(records)*float(l_weight)))

    cursor.close()
    conn.close()
    return l_return_stru
