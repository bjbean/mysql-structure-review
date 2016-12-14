
# -*- coding: UTF-8 -*-
import MySQLdb

"""************************************************************
名称
    f_rule_exist_lob_column
描述
    获得数据库中存在大对象类型的表并计算扣分
参数
    p_parms        list    参数列表
    p_parms[0]     l_dbinfo         list    数据库连接信息
    p_parms[1]     l_username       string  数据库用户(要大写)
    p_parms[2]     l_weight         int     扣分权重
    p_parms[3]     l_max_value      int     扣分上限
返回值
    dict{'records':xxx',scores':xxx}
    'records'   list    表信息(表名,字段数量)
    'scores'    float   扣分
示例
    v_dbinfo=["localhost","1521","xe","hf","123"]
    v_parms=[v_dbinfo,'HF',100,0.1,10]
    v_result = f_rule_table_blob(v_parms)
说明
    函数调用中,有几个参数调用顺序是固定的。分别如下:
    parm1       数据库连接信息
    parm2       数据库用户
    parm3...    规则配置中的参数部分parm
    parm(n-1)   扣分阀值,对应规则配置中的weight
    parm(n)     扣分上限,对应规则配置中的max_value
************************************************************"""

def f_rule_exist_lob_column(p_parms):
    [l_dbinfo, l_username, l_weight, l_max_value] = [p_parms[0], p_parms[1], int(p_parms[2]), int(p_parms[3])]
    l_return_stru = {"scores": 0, "records": []}
    conn = MySQLdb.connect(host=l_dbinfo[0], user=l_dbinfo[3], passwd=l_dbinfo[4], db=l_dbinfo[2], charset="utf8", port=l_dbinfo[1])
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TABLE_NAME,column_name , DATA_TYPE
        FROM information_schema.COLUMNS
        where TABLE_SCHEMA='"""+l_username+"""' and DATA_TYPE in ('blob','mediumblob','longblob','text','mediumblob','longtext')
        order by table_name"""
    )
    records = cursor.fetchall()    
    l_return_stru["records"]=records

    if len(records)*l_weight>l_max_value:
        l_return_stru["scores"]=l_max_value
    else:
        l_return_stru["scores"]=len(records)*l_weight

    cursor.close()
    conn.close()
    return l_return_stru