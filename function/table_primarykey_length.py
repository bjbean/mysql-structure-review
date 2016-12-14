#!/usr/bin/python
# -*- coding: utf-8 -*-

import pprint
import MySQLdb

"""************************************************************
名称
    f_rule_table_primarykey_length
描述
    获得数据库指定表的主键长度并计算扣分
参数
    p_parms        list            参数列表
    p_parms[0]     l_dbinfo        list    数据库连接信息
    p_parms[1]     l_username      string  数据库用户
    p_parms[2]     l_pk_length     int     主键长度(字节)
    p_parms[3]     l_threshold     int     扣分阀值
    p_parms[4]     l_max_value     int     扣分上限
返回值
    dict{'scores':xxx,'records':xxx}
    'scores'    float   扣分
    'records'   list    表信息(表名,主键字段，字段类型，字段长度)
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

def f_rule_table_primarykey_length(p_parms):
    [l_dbinfo, l_username, l_primarykey_length, l_weight, l_max_value] = [p_parms[0], p_parms[1], int(p_parms[2]), int(p_parms[3]), int(p_parms[4])]
    l_return_stru = {"scores": 0, "records": []}
    l_return_tabs = []
    conn = MySQLdb.connect(host=l_dbinfo[0], user=l_dbinfo[3], passwd=l_dbinfo[4], db=l_dbinfo[2], charset="utf8", port=l_dbinfo[1])
    cursor = conn.cursor()
    cursor.execute("""
        select table_name,group_concat(column_name)
        from information_schema.statistics
        where table_schema='"""+l_username+"""' and index_name='PRIMARY'
        group by table_name"""
        )
    cur_tables = cursor.fetchall()
    for rec_tab in cur_tables:
        l_table_name = rec_tab[0]
        l_primarykey_columns = rec_tab[1]

        l_tmp_column_length = 0
        l_tmp_column_data_type = ''
        l_tmp_column_list = ''

        for rec_col in l_primarykey_columns.split(',')[::-1]:
            cursor.execute("""
                select data_type,character_octet_length,ifnull(numeric_precision,-1),ifnull(numeric_scale,-1)
                from information_schema.columns
                where table_schema='"""+l_username+"""' and
                table_name='"""+l_table_name+"""' and
                column_name = '"""+rec_col+"""'"""
            )
            cur_column = cursor.fetchall()
            l_tmp_column_list = l_tmp_column_list + rec_col + ','
            l_data_type = cur_column[0][0]
            l_character_octet_length = cur_column[0][1]
            l_numeric_precision = cur_column[0][2]
            l_numeric_scale = cur_column[0][3]
            l_tmp_column_length = l_tmp_column_length + f_get_byte_length(l_data_type,l_character_octet_length,l_numeric_precision,l_numeric_scale)
            l_tmp_column_data_type = l_tmp_column_data_type + l_data_type + ','

        if l_tmp_column_length > l_primarykey_length:
            l_return_tabs.append([l_table_name, l_tmp_column_list[0:-1], l_tmp_column_data_type[0:-1], l_tmp_column_length])

    l_return_stru["records"] = l_return_tabs

    if (len(l_return_tabs)*float(l_weight)) > float(l_max_value):
        l_return_stru["scores"] = float('%0.2f' % float(l_max_value))
    else:
        l_return_stru["scores"] = float('%0.2f' % (len(l_return_tabs)*float(l_weight)))

    cursor.close()
    conn.close()
    return l_return_stru
