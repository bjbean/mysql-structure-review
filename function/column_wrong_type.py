#!/usr/bin/python
# -*- coding: utf-8 -*-
import pprint
import MySQLdb
import re
"""************************************************************
=====规则配置文件=====
[rule1]
name        = COL_WRONG_TYPE                    #规则名称
description = 表字段类型不匹配                  #规则描述
parm1       = 10000                             #输入参数1-提取做正则判断记录数上限
weight      = 1                                 #扣分权重
max_value   = 10                                #扣分上限
status      = ON/OFF                            #规则状态
title1      = 表名称                            #返回数据标题1
title2      = 表字段名                          #返回数据标题2 
title3      = 字段定义类型                      #返回数据标题3
title4      = 字段实际类型                      #返回数据标题4
*************************************************************"""
def f_rule_column_wrong_type(p_parms):
    pat_date_c  = re.compile('((((19|20)?\d{2})[-/]*((1[0-2])|(0?[1-9]))[-/]*(([12][0-9])|(3[01])|(0?[1-9])))|(((1[0-2])|(0?[1-9]))[-/]*(([12][0-9])|(3[01])|(0?[1-9]))[-/]*((19|20)?\d{2}))|((([12][0-9])|(3[01])|(0?[1-9]))[-/]*((1[0-2])|(0?[1-9]))[-/]*((19|20)?\d{2})))(\s{1,}([01]?\d|2[0-3]):?([0-5]?\d):?([0-5]?\d))?$')
    pat_date_n  = re.compile('((19|20)?\d{2})((1[0-2])|(0[1-9]))(([12][0-9])|(3[01])|(0[1-9]))(([01]\d|2[0-3])([0-5]\d)([0-5]\d))?$')
    pat_phone   = re.compile('(\+?0?86\-?)?1[3|4|5|7|8][0-9]\d{8}$')
    pat_fax     = re.compile('(((0?10)|(0?[2-9]\d{2,3}))[-\s]?)?[1-9]\d{6,7}$')    
    pat_account = re.compile('(\d{16}|\d{19}|\d{12})$')
    pat_head_0  = re.compile('0\d*')
    [l_dbinfo, l_db, l_num_row, l_weight, l_max_value] = [p_parms[0], p_parms[1], int(p_parms[2]), int(p_parms[3]), int(p_parms[4])]
    l_return_stru={"scores":0,"records":[]}
    conn = MySQLdb.connect(host=l_dbinfo[0], user=l_dbinfo[3], passwd=l_dbinfo[4], db=l_dbinfo[2], charset="utf8", port=l_dbinfo[1])
    cursor = conn.cursor()
    records=[]
    cursor.execute("""
        select d.table_name,
        d.column_name,
        case when d.TABLE_ROWS >"""+str(l_num_row)+""" then 'BIG' else 'SMALL' end,
        d.TABLE_ROWS,
        d.column_TYPE,
        b.column_name as prikey from 
        (select c.table_name,c.column_name,c.column_TYPE,t.TABLE_ROWS from information_schema.`COLUMNS` c,information_schema.`TABLES` t
         where c.COLUMN_TYPE like '%char%' and c.TABLE_SCHEMA='"""+l_db+"""' and c.TABLE_NAME=t.TABLE_NAME and t.`ENGINE`='InnoDB') d 
        LEFT OUTER JOIN 
        (select table_name,column_name from information_schema.COLUMNS where column_key ='PRI' and TABLE_SCHEMA='"""+l_db+"""' and DATA_TYPE like '%int%' 
            and table_name in
            (select a.table_name from information_schema.COLUMNS a where a.column_key ='PRI' and a.TABLE_SCHEMA='"""+l_db+"""' group by a.table_name having count(1) =1)) b 
        on d.table_name=b.table_name """)
    results = cursor.fetchall()
    table_limit=l_num_row/8
    table_name_init='dba$init_table'
    table_min_init =0
    table_max_init =0
    table_section_num=0
    for r in results:
      if r[2]=="BIG":
          if r[5]<>None:
              if r[0]<>table_name_init:
                  cursor.execute("""select min("""+r[5]+""") from """+l_db+"""."""+r[0])
                  table_min_init=cursor.fetchall()[0][0]                  
                  cursor.execute("""select max("""+r[5]+""") from """+l_db+"""."""+r[0])
                  table_max_init=cursor.fetchall()[0][0]
                  table_section_num=(table_max_init-table_min_init)/8
                  table_name_init=r[0]
              cursor.execute("""select * from (
                    (select a."""+r[1]+""" from """+l_db+"""."""+r[0]+""" a where """+r[5]+""">"""+str(table_min_init)+""" limit """+str(table_limit)+""")
                     union all
                     (select a."""+r[1]+""" from """+l_db+"""."""+r[0]+""" a where """+r[5]+""">"""+str(table_min_init+table_section_num)+""" limit """+str(table_limit)+""")
                     union all
                     (select a."""+r[1]+""" from """+l_db+"""."""+r[0]+""" a where """+r[5]+""">"""+str(table_min_init+2*table_section_num)+""" limit """+str(table_limit)+""")
                     union all
                     (select a."""+r[1]+""" from """+l_db+"""."""+r[0]+""" a where """+r[5]+""">"""+str(table_min_init+3*table_section_num)+""" limit """+str(table_limit)+""")
                     union all
                     (select a."""+r[1]+""" from """+l_db+"""."""+r[0]+""" a where """+r[5]+""">"""+str(table_min_init+4*table_section_num)+""" limit """+str(table_limit)+""")
                     union all
                     (select a."""+r[1]+""" from """+l_db+"""."""+r[0]+""" a where """+r[5]+""">"""+str(table_min_init+5*table_section_num)+""" limit """+str(table_limit)+""")
                     union all
                     (select a."""+r[1]+""" from """+l_db+"""."""+r[0]+""" a where """+r[5]+""">"""+str(table_min_init+6*table_section_num)+""" limit """+str(table_limit)+""")
                     union all
                     (select a."""+r[1]+""" from """+l_db+"""."""+r[0]+""" a where """+r[5]+""">"""+str(table_min_init+7*table_section_num)+""" limit """+str(table_limit)+""")
                    ) b
                     where trim(b."""+r[1]+""") <>'' and trim(b."""+r[1]+""") <>'0'""" )
      elif (r[2]=="SMALL" or r[5]==None):
          cursor.execute("""
            select * from (select a."""+r[1]+""" from """+l_db+"""."""+r[0]+""" a limit """+str(l_num_row)+""") b
             where trim(b."""+r[1]+""") <>'' and trim(b."""+r[1]+""") <>'0'""")
      data = cursor.fetchall()
      l_char=0
      l_date=0
      l_phone=0
      l_fax=0
      l_num=0
      l_account=0
      l_head_0=0
      if data:
        if r[4]=="NUMBER":
          for d in data:
            if not pat_date_n.match(d[0]):
              l_date=1
              break
          if l_date==0:
            records.append(r+('DATE',))
        else :
          for d in data:
            if re.search('[^0-9\-\/\+\:\s]',d[0]):
              l_char=1
              break
            if re.search('[\-\/]',d[0]):
              l_num=1
              if not pat_date_c.match(d[0]):
                l_date=1
              if not pat_fax.match(d[0]):
                l_fax=1
              if (l_date==1 and l_fax==1):          
                break
            else:
              if not pat_phone.match(d[0]):
                l_phone=1
              if not pat_date_n.match(d[0]):
                l_date=1
              if not pat_account.match(d[0]):
                l_account=1
              if not pat_fax.match(d[0]):
                l_fax=1
              if pat_head_0.match(d[0]):
                l_head_0=1
          if (l_date==0 and l_char==0):
            records.append((r[0],)+(r[1],)+(r[4],)+('DATE',))
          if (l_date==1 and l_num==0 and l_char==0 and l_phone==1 and l_account==1 and l_fax==1 and l_head_0==0):
            records.append((r[0],)+(r[1],)+(r[4],)+('NUMBER',))
    l_return_stru["records"] = records
    if (len(records)*float(l_weight)) > float(l_max_value):
        l_return_stru["scores"] = float('%0.2f' %float(l_max_value))
    else:
        l_return_stru["scores"] = float('%0.2f' %(len(records)*float(l_weight)))

    cursor.close()
    conn.close()
    return l_return_stru
