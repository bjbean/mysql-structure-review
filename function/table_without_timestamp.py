#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
"""************************************************************
=====规则配置文件=====
[rule1]
name        = TABLE_NO_TIME             #规则名称
description = 不包含时间戳字段的表          #规则描述
weight      = 1                         #扣分权重
max_value   = 10                        #扣分上限
status      = ON/OFF                    #规则状态
title1      = 表名称                      #返回数据标题1
*************************************************************"""
def f_rule_table_without_timestamp(p_parms):
    [l_dbinfo,l_db,l_weight,l_max_value]=[p_parms[0],p_parms[1],int(p_parms[2]),int(p_parms[3])]
    l_return_stru={"scores":0,"records":[]}
    conn = MySQLdb.connect(host=l_dbinfo[0], user=l_dbinfo[3], passwd=l_dbinfo[4], db=l_dbinfo[2], charset="utf8", port=l_dbinfo[1])
    cursor = conn.cursor()
    cursor.execute("""
        select t.table_name
        from information_schema.`TABLES` t
        where t.TABLE_SCHEMA ='"""+l_db+"""'
        and t.TABLE_NAME not in(
        select c.table_name from information_schema.`COLUMNS` c
        where (c.COLUMN_NAME like '%UPDATE%' OR c.COLUMN_NAME like '%CREATE%')
        and c.DATA_TYPE in ('datetime','date','timestamp','time')
        and c.table_schema ='"""+l_db+"""')""")
    records = cursor.fetchall()
    l_return_stru["records"]=records

    if (len(records)*float(l_weight))>float(l_max_value):
        l_return_stru["scores"]=float('%0.2f' %float(l_max_value))
    else:
        l_return_stru["scores"]=float('%0.2f' %(len(records)*float(l_weight)))

    cursor.close()
    conn.close()
    return l_return_stru
