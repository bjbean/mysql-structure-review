#!/usr/bin/python
# -*- coding: utf-8 -*-

import pprint
import MySQLdb

"""************************************************************
名称
    f_rule_record_length
描述
    获得数据库指定表的记录定义长度并计算扣分
参数
    p_parms        list            参数列表
    p_parms[0]     l_dbinfo        list    数据库连接信息
    p_parms[1]     l_username      string  数据库用户
    p_parms[2]     l_record_length int     记录长度
    p_parms[3]     l_threshold     int     扣分阀值
    p_parms[4]     l_max_value     int     扣分上限
返回值
    dict{'scores':xxx,'records':xxx}
    'scores'    float   扣分
    'records'   list    表信息(表名,字段内容平均长度，字段定义长度)
************************************************************"""
def f_get_byte_length(p_data_type,p_character_octet_length,p_numeric_precision,p_numeric_scale):
    case_data_type = {
        'tinyint': 1,
        'smallint': 2,
        'mediumint': 3,
        'int': 4,
        'integer': 4,
        'bigint': 8,
        'float': 4,
        'double': 8,
        'decimal': (p_numeric_precision+2 if p_numeric_precision>p_numeric_scale else p_numeric_scale+2),
        'date': 3,
        'time': 3,
        'year': 1,
        'datetime': 8,
        'timestamp': 8,
        'char': p_character_octet_length,
        'varchar': p_character_octet_length,
        'varbinary': p_character_octet_length,
        'tinyblob': p_character_octet_length,
        'tinytext': p_character_octet_length,
        'blob': p_character_octet_length,
        'text': p_character_octet_length,
        'mediumblob': p_character_octet_length,
        'mediumtext': p_character_octet_length,
        'logngblob': p_character_octet_length,
        'longtext': p_character_octet_length,
        'enum': p_character_octet_length,
        'set': p_character_octet_length
    }
    return case_data_type.get(p_data_type)

def f_rule_table_record_length(p_parms):
    [l_dbinfo, l_username, l_record_length, l_weight, l_max_value] = [p_parms[0], p_parms[1], int(p_parms[2]), int(p_parms[3]), int(p_parms[4])]
    l_return_stru = {"scores": 0, "records": []}
    l_return_tabs = []
    conn = MySQLdb.connect(host=l_dbinfo[0], user=l_dbinfo[3], passwd=l_dbinfo[4], db=l_dbinfo[2], charset="utf8", port=l_dbinfo[1])
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name,avg_row_length
        FROM information_schema.tables
        WHERE table_schema = '"""+l_username+"""'"""
        )
    cur_tables = cursor.fetchall()
    for rec_tab in cur_tables:
        l_table_name = rec_tab[0]
        cursor.execute("""
            select column_name,data_type,character_octet_length,ifnull(numeric_precision,-1),ifnull(numeric_scale,-1)
            from information_schema.columns
            where table_schema='"""+l_username+"""' and
            table_name='"""+l_table_name+"""'"""
        )
        #print l_username,l_table_name
        cur_columns = cursor.fetchall()
        l_tmp_column_length = 0
        for rec_col in cur_columns:
            l_data_type = rec_col[1]
            l_character_octet_length = rec_col[2]
            l_numeric_precision = rec_col[3]
            l_numeric_scale = rec_col[4]
            #print l_data_type,l_character_octet_length,l_numeric_precision,l_numeric_scale
            #print '000000'
            #print f_get_byte_length(l_data_type,l_character_octet_length,l_numeric_precision,l_numeric_scale)
            l_tmp_column_length = l_tmp_column_length + f_get_byte_length(l_data_type,l_character_octet_length,l_numeric_precision,l_numeric_scale)

        if l_tmp_column_length > l_record_length:
            l_return_tabs.append([rec_tab[0], rec_tab[1], l_tmp_column_length])

    l_return_stru["records"] = l_return_tabs

    if (len(l_return_tabs)*float(l_weight)) > float(l_max_value):
        l_return_stru["scores"] = float('%0.2f' % float(l_max_value))
    else:
        l_return_stru["scores"] = float('%0.2f' % (len(l_return_tabs)*float(l_weight)))

    cursor.close()
    conn.close()
    return l_return_stru
