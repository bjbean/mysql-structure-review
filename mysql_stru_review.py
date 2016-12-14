#!/usr/bin/python
# -*- coding: utf-8 -*-
# -d 10.10.40.182 -u test -f default
import sys
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
import ConfigParser
import getopt
from pyh import *
import time
import function.big_table_by_size
import function.big_schema_by_table_num
import function.big_table_by_row_num
import function.big_table_by_index_num
import function.table_exist_foreign_key
import function.exist_func_proc_trigger
import function.index_selectivity
import function.exist_lob_column
import function.table_column_num
import function.table_record_length
import function.table_primarykey_length
import function.table_no_def_primarykey
import function.table_without_timestamp
import function.column_wrong_type

def print_html_header():
    page = PyH('MySQL Database Structure Check Report')
    page << """<style type="text/css">
            body.awr {font:bold 10pt Arial,Helvetica,Geneva,sans-serif;color:black;}
            pre.awr  {font:10pt Courier;color:black; background:White;}
            h1.awr   {font:bold 20pt Arial,Helvetica,Geneva,sans-serif;color:#336699;border-bottom:1px solid #cccc99;margin-top:0pt; margin-bottom:0pt;padding:0px 0px 0px 0px;}
            h2.awr   {font:bold 18pt Arial,Helvetica,Geneva,sans-serif;color:#336699;margin-top:4pt; margin-bottom:0pt;}
            h3.awr   {font:bold 16pt Arial,Helvetica,Geneva,sans-serif;color:#336699;margin-top:4pt; margin-bottom:0pt;}
            h4.awr   {font:bold 14pt Arial,Helvetica,Geneva,sans-serif;color:#336699;margin-top:4pt; margin-bottom:0pt;}
            h5.awr   {font:bold 12pt Arial,Helvetica,Geneva,sans-serif;color:#336699;margin-top:4pt; margin-bottom:0pt;}
            h6.awr   {font:bold 10pt Arial,Helvetica,Geneva,sans-serif;color:#336699;margin-top:4pt; margin-bottom:0pt;}
            h7.awr   {font: 10pt Arial,Helvetica,Geneva,sans-serif; color:#336699;margin-top:4pt; margin-bottom:0pt;}
            li.awr   {font:bold 12pt Arial,Helvetica,Geneva,sans-serif; color:black; background:White;}
            th.awrnobg  {font:bold 10pt Arial,Helvetica,Geneva,sans-serif; color:black; background:White;padding-left:4px; padding-right:4px;padding-bottom:2px}
            td.awrbg    {font:bold 10pt Arial,Helvetica,Geneva,sans-serif; color:White; background:#0066CC;padding-left:4px; padding-right:4px;padding-bottom:2px}
            td.awrnc    {font:10pt Arial,Helvetica,Geneva,sans-serif;color:black;background:White;vertical-align:top;}
            td.awrc     {font:10pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;}
            td.awrrc     {font:10pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFCCCC; vertical-align:top;}
            a.awr       {font:bold 10pt Arial,Helvetica,sans-serif;color:#663300; vertical-align:top;margin-top:0pt; margin-bottom:0pt;}
            </style>"""
    page << """<SCRIPT>
            function isHidden(oDiv,oTab){
              var vDiv = document.getElementById(oDiv);
              var vTab = document.getElementById(oTab);
              vDiv.innerHTML=(vTab.style.display == 'none')?"<h5 class='awr'>-</h5>":"<h5 class='awr'>+</h5>";
              vTab.style.display = (vTab.style.display == 'none')?'table':'none';
            }
            </SCRIPT>"""
    page << """<head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
            </head>"""

    page << h1('MySQL数据库结构审核报告', cl='awr')
    page << br()
    return page

def print_rule_table(p_page, p_title, p_data):
    l_page = p_page
    l_data = p_data
    l_header = p_title
    
    mytab = l_page << table(border='1',width=1200)
    headtr = mytab << tr()
    for i in range(0, len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class']='awrbg'
        td_tmp.attributes['align']='center'

    for j in range(0, len(l_data)):
        tabtr = mytab << tr()

        for i in range(0,len(l_data[j])):                        
            if i==0:
                td_tmp = tabtr << td()
                a_tmp = td_tmp<<a(l_data[j][i])
                a_tmp.attributes['class']='awr'
                a_tmp.attributes['href']='#'+l_data[j][i]
            else:
                td_tmp = tabtr << td(l_data[j][i])

            if j%2==0:
                td_tmp.attributes['class']='awrc'
            else:
                td_tmp.attributes['class']='awrnc'

            if int(l_data[j][-1]) == int(l_data[j][-2]):
                td_tmp.attributes['class']='awrrc'

            td_tmp.attributes['align']='right'
    l_page << br()


def print_rule_pei(p_page,p_title,p_data,p_rules):
    l_page = p_page
    l_data = p_data
    l_header = p_title
    l_rules = p_rules
    l_page << """<div id="main" style="height:400px;width:800px"></div>"""
    l_page << """<script src="./js/dist/echarts.js"></script>"""
    l_page << """<script type="text/javascript">"""
    l_page << """require.config({ paths: { echarts: './js/dist' }});"""
    l_page << """require("""
    l_page << """['echarts','echarts/chart/pie'],"""
    l_page << """function (ec) {"""
    l_page << """var myChart = ec.init(document.getElementById('main')); """
    l_page << """var option = {"""
    l_page << """tooltip : {trigger: 'item',formatter: "{a} <br/>{b} : {c} ({d}%)"},"""
    l_page << """legend: {orient : 'vertical',x : 'right',data:["""+"'"+("','").join(l_title)+"'"+"""]},"""
    l_page << """calculable : true,"""
    l_page << """series : [{"""
    l_page << """name:'规则:',"""
    l_page << """type:'pie',radius : '70%',center: ['30%', '60%'],"""
    l_page << """data:["""
    for l_num in range(0,len(l_data)):
        if l_num==len(l_data)-1:
            l_page <<"""{value:"""+str(l_data[l_num])+""", name:'+"""+l_rules[l_title[l_num]]['summary']+"""'}"""
        else:
            l_page <<"""{value:"""+str(l_data[l_num])+""", name:'+"""+l_rules[l_title[l_num]]['summary']+"""'},"""
    l_page << """]}]};"""
    l_page << """myChart.setOption(option);"""
    l_page << """});</script>"""
    l_page << br()


def print_html_table(p_page,p_title,p_data):
    l_page = p_page
    l_data = p_data
    l_header = p_title
    
    mytab = l_page << table(border='1',width=800)
    headtr = mytab << tr()
    for i in range(0,len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class']='awrbg'
        td_tmp.attributes['align']='center'

    for j in range(0,len(l_data)):
        tabtr = mytab << tr()
        for i in range(0,len(l_data[j])):
            td_tmp = tabtr << td(l_data[j][i])
            td_tmp.attributes['class']='awrc'
            td_tmp.attributes['align']='right'
            if j%2==0:
                td_tmp.attributes['class']='awrc'
            else:
                td_tmp.attributes['class']='awrnc'
    l_page << br()


def f_get_max_parm_num(p_rules):
    l_max_parm_num=0
    l_rules=p_rules
    for l_rulename in l_rules:
        l_num=0
        for key in v_rules[l_rulename]:            
            if key[0:4]=='parm':
                l_num+=1
        if l_max_parm_num<l_num:
            l_max_parm_num=l_num
    return l_max_parm_num


def print_help():
    print "Usage:"
    print "    ./stru.py -d <db_info> -u <schema_name> -f <output_file>"
    print "    -d : database ip address/domain name"
    print "    -u : analyze schema name"
    print "    -f : create report file"

if __name__ == "__main__":    
    
    # initial variable
    v_dbinfos = {}
    v_dbinfo = []
    v_rules = {}
    v_username=''
    v_parms=[]
    v_db_conf=sys.path[0]+"/conf/db.conf"
    v_rule_conf=sys.path[0]+"/conf/rule.conf"
    v_date=''
    v_rpt_filename=''
    v_rpt_emails=''
    v_results={}
    v_score=0
    v_total_score=0
    
    # initial database config file
    config = ConfigParser.ConfigParser()
    config.readfp(open(v_db_conf))
    for section in config.sections():
        v_dbinfos[section]=[config.get(section,'server'),int(config.get(section,'port')),config.get(section,'db_name'),config.get(section,'db_user'),config.get(section,'db_pwd')]

    # initial rule config file
    config = ConfigParser.ConfigParser()
    config.readfp(open(v_rule_conf))
    for section in sorted(config.sections()):        
        l_rule={}
        for option in config.options(section):
            l_rule[option]=config.get(section,option)
        v_rules[section]=l_rule
    
    # initial other variable
    v_date = time.strftime('%Y-%m-%d')

    # deal input parameter
    try:
        opts, args = getopt.getopt(sys.argv[1:], "d:u:f:m:")

        for o,v in opts:
            if o == "-d":
                v_dbinfo = v_dbinfos[v]                
            elif o == "-u":
                v_username = v
            elif o == "-f":
                v_rpt_filename = v
            elif o == "-m":
                v_rpt_emails = v.split(',')            
        
        if v_rpt_filename<>"" and len(v_rpt_emails)>0:
            print_help()
            exit()
            
        if v_rpt_filename=="" and len(v_rpt_emails)==0:
            print_help()
            exit()
            
        if v_rpt_filename.upper() == 'DEFAULT':
            v_rpt_filename='rpt_stru_'+v_dbinfo[0].replace('.','_')+'_'+str(v_dbinfo[1])+'_'+v_username.lower()+'_'+v_date.replace('-','_')+'.html'
            
        if '.' not in v_rpt_filename:
            v_rpt_filename=v_rpt_filename+'.html'            
    except getopt.GetoptError,msg:
        print_help()
        exit()    

    # calc total score
    for l_rulename in v_rules:        
        if v_rules[l_rulename]['status']=='ON':
            v_total_score+=int(v_rules[l_rulename]['max_value'])
                
    # do rule work
    v_score = v_total_score
    for l_rulename in v_rules:        
        if v_rules[l_rulename]['status']=='ON':
            print 'deal rule ...',v_rules[l_rulename]['name']
            v_parms=[]
            v_keys=[]
            v_parms.append(v_dbinfo)
            v_parms.append(v_username)
            for key in v_rules[l_rulename]:          
                if key[0:4]=='parm':
                    v_keys.append(key)
            for key in sorted(v_keys):
                v_parms.append(v_rules[l_rulename][key])
            
            v_parms.append(v_rules[l_rulename]['weight'])
            v_parms.append(v_rules[l_rulename]['max_value'])
            v_results[l_rulename] = eval("function."+v_rules[l_rulename]['name'].lower()+".f_rule_"+v_rules[l_rulename]['name'].lower())(v_parms)
            #print l_rulename,v_results[l_rulename]['scores'],type(v_results[l_rulename]['scores'])
            v_score-=v_results[l_rulename]['scores']
    
    # get html global object
    v_page = print_html_header()
    
    # output general info
    v_page << h5('数据库信息', cl='awr')
    print_html_table(v_page,['数据库IP/域名','端口号','分析Schema','分析时间','总体得分','满分','合格率(%)'],[[v_dbinfo[0],v_dbinfo[1],v_username,time.strftime('%Y-%m-%d'),float('%0.2f' %v_score),v_total_score,float('%0.2f' %(v_score/v_total_score*100))]])

    # output all rule info
    l_title=[]
    l_data=[]
    l_title.append('规则名称')
    l_title.append('规则描述')
    l_title.append('规则状态')
    l_max_parm_num=f_get_max_parm_num(v_rules)
    for l_num in range(1,l_max_parm_num+1):
        l_title.append('参数'+str(l_num))
        l_title.append('参数描述'+str(l_num))
    l_title.append('扣分阀值')
    l_title.append('扣分上限')
    l_title.append('实际扣分')
    
    for l_rulename in sorted(v_rules.keys()):
        l_data_rule=[]
        l_num=0
        l_data_rule.append(l_rulename)
        l_data_rule.append(v_rules[l_rulename]['summary'])
        l_data_rule.append(v_rules[l_rulename]['status'])
        for l_num in range(1,l_max_parm_num+1):
            if v_rules[l_rulename].has_key('parm'+str(l_num)):
                l_data_rule.append(v_rules[l_rulename]['parm'+str(l_num)])
                l_data_rule.append(v_rules[l_rulename]['pdesc'+str(l_num)])
            else:
                l_data_rule.append('')
                l_data_rule.append('')
        l_data_rule.append(v_rules[l_rulename]['weight'])
        l_data_rule.append(v_rules[l_rulename]['max_value'])
        if v_rules[l_rulename]['status']=='ON':
            l_data_rule.append(v_results[l_rulename]['scores'])
        else:
            l_data_rule.append('')
        l_data.append(l_data_rule)
    
    v_page << h5('规则列表', cl='awr')
    print_rule_table(v_page,l_title,l_data)

    # output rule pie
    l_title=[]
    l_data=[]
    for l_rulename in sorted(v_rules.keys()):
        if v_rules[l_rulename]['status']=='ON':
            l_title.append(l_rulename)
            l_data.append(v_results[l_rulename]['scores'])
    print_rule_pei(v_page,l_title,l_data,v_rules)

    # output rule detail info
    for l_rulename in sorted(v_rules.keys()):
        if v_rules[l_rulename]['status']=='ON':
            l_title=[]
            l_data=[]
            for l_num in range(1,10):
                if v_rules[l_rulename].has_key('title'+str(l_num)):
                    l_title.append(v_rules[l_rulename]['title'+str(l_num)])
            l_data=v_results[l_rulename]['records']
            v_page << h5(' 规则 [ '+l_rulename+'——'+v_rules[l_rulename]['summary']+" ]    (扣分:"+str(v_results[l_rulename]['scores'])+')', cl='awr')
            a_tmp=v_page << a()
            a_tmp.attributes['name']=l_rulename
            print_html_table(v_page,l_title,l_data)

    # output rule description
    v_page << h1('', cl='awr')
    v_page << br()
    for l_rulename in sorted(v_rules.keys()):
        v_page << li(' 规则 - '+l_rulename, cl='awr')
        v_page << h5('   '+v_rules[l_rulename]['description'],cl='awr')
        v_page << br()
    v_page.printOut(file='/var/www/html/' + v_rpt_filename)

    print "create oracle structure check report file ... " + '/var/www/html/' + v_rpt_filename
