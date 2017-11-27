# encoding:utf-8

import OracleHelper
from table_dic import table_dic
import math
import logging
import datetime,time
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

'''
通过定义列,获取数据条数
设置分页数,查询数据写入文件
select * from(select rownum rn, t_customer.* from t_customer where rownum <= 30 )where rn >= 20
'''

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='migrate.log',
                filemode='w')


# 获取数据并且组装成insert语句
def get_data(query_sql, insert_sql, cls_size):
    insert_values = insert_sql
    oracleHelper = OracleHelper.OracleHelper()
    rows = oracleHelper.query(query_sql)
    logging.info("分页获取数据:"+ str(oracleHelper.rowcount()))
    currrent_time =time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    for row in rows:
        str_row = "("
        for index in range(0, cls_size):
            if isinstance(row[index], int) or isinstance(row[index], float):
                if str(row[index]) == "None":
                    str_row += "null,"
                else:
                    str_row += str(row[index])+","
            else:
                if str(row[index]) == "None":
                    str_row += "null,"
                else:
                    str_row = str_row + "'"+str(row[index])+"',"

        str_row += "'"+currrent_time + "')\n,"
        insert_values = insert_values + str_row

    oracleHelper.close()
    return insert_values[0:len(insert_values) - 1]


def write2file(table_name, insert_values, page):
    fo = open(table_name+"_"+str(page) + ".sql", "wb+")
    fo.write(insert_values)
    fo.close()


# 获取数据总数
def get_count(count_sql):
    oracleHelper = OracleHelper.OracleHelper()
    row_count = oracleHelper.queryRow(count_sql)
    count = row_count[0]
    logging.info("数据总数:"+ str(count))
    oracleHelper.close()
    return count


def generate_sql(table_name, table_cols, where_conditions):
    insert_sql = "insert INTO ods_"+table_name + "("+table_cols+",etl_last_updt_time) VALUES"
    count_sql = "select count(1)" + " from "+table_name + " where "+where_conditions
    # 列数
    cls_size = len(table_cols.split(","))
    count = get_count(count_sql)

    page_size = 100
    page_num = int(math.ceil(float(count)/float(page_size)))

    logging.info(table_name+"页数 :" + str(page_num))

    for i in range(0, page_num):
        start = i * page_size
        end = start + page_size
        page_sql = "select " + table_cols + " from (select rownum rn, " + table_cols + " from "+table_name \
               +" where rownum <="+str(end)+" AND "+where_conditions + ") where rn>" + str(start) + " AND "+where_conditions
        logging.info(page_sql)
        sql_text = get_data(page_sql, insert_sql, cls_size)
        write2file(table_name, sql_text, i)


# 循环字典 生成文件
for dic in table_dic:
    generate_sql(dic["table_name"], dic["table_cols"], dic["where_conditions"])



