#!/usr/bin/python
# -*- coding: utf-8 -*-

import pprint
import MySQLdb

"""************************************************************
名称
    f_rule_big_table_by_size
描述
    获得数据库指定用户的大表并计算扣分
参数
    p_parms        list            参数列表
    p_parms[0]     l_dbinfo        list    数据库连接信息
    p_parms[1]     l_username      string  数据库用户
    p_parms[2]     l_table_size    int     表的大小(单位是G)
    p_parms[3]     l_threshold     int     扣分阀值
    p_parms[4]     l_max_value     int     扣分上限
返回值
    dict{'scores':xxx,'records':xxx}
    'scores'    float   扣分
    'records'   list    大表信息(表名,大小)
示例
    v_dbinfo=["localhost","1521","SCHEMA","user1","123456"]
    v_parms=[v_dbinfo,'user1',0,0.1,10]
    v_result = f_rule_big_tab(v_parms)
说明
    函数调用中,有几个参数调用顺序是固定的。分别如下:
    parm1       数据库连接信息
    parm2       数据库用户
    parm3...    规则配置中的参数部分parm
    parm(n-1)   扣分阀值,对应规则配置中的threshold
    parm(n)     扣分上限,对应规则配置中的max_value
************************************************************"""
def f_rule_big_table_by_size(p_parms):
    [l_dbinfo, l_username, l_table_size, l_weight, l_max_value] = [p_parms[0], p_parms[1], int(p_parms[2]), int(p_parms[3]), int(p_parms[4])]
    l_return_stru = {"scores": 0, "records": []}
    conn = MySQLdb.connect(host=l_dbinfo[0], user=l_dbinfo[3], passwd=l_dbinfo[4], db=l_dbinfo[2], charset="utf8", port=l_dbinfo[1])
    cursor = conn.cursor()
    cursor.execute("""
        select table_name,round(data_length/1024/1024/1024,2)
        from information_schema.tables
        where table_schema='"""+l_username+"""'
        and CREATE_OPTIONS<>'partitioned'
        and data_length>"""+str(l_table_size*1024*1024*1024)+"""
        union all
        select concat(table_name,':',partition_name),round(data_length/1024/1024/1024,2)
        from information_schema.partitions
        where table_schema='"""+l_username+"""'
        and table_name not in (select table_name from information_schema.tables where table_schema='"""+l_username+"""' and CREATE_OPTIONS<>'partitioned')
        and data_length>"""+str(l_table_size*1024*1024*1024)
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
    