# encoding:utf-8

import OracleHelper
import math
import datetime,time
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

'''
通过定义列,获取数据条数
设置分页数,查询数据写入文件
select * from(select rownum rn, t_customer.* from t_customer where rownum <= 30 )where rn >= 20
'''

# table_cols = "USER_ID, USER_NAME, USER_NUMBER,IPAD_TECH_ID,CREATE_TIME"
# table_name = "BOMSOWNER.users"
# where_conditions = "1=1"
table_cols = "ID,ACTUAL_PAYMENT,BIG_CAT_NAME,BIZ_TYPE,BIZ_TYPE_NAME,BRAND_NAME,BRAND_NO,COST,ESTIMATE_PAYMENT,GROSS_PROFIT,MID_CAT_NAME,PREPAY,PRODUCT_NAME,PRODUCT_NO,QUANTITY,MAN_HOUR,SETTLEMENT_ID,SMALL_CAT_NAME,TAX,SAP_TAX,SAP_COST,SOURCE_ORDER_ID"
table_name = "settlement_detail"
where_conditions = "settlement_id IN ( SELECT ID FROM SETTLEMENT where to_char(creation_date,'yyyymm')='201701')"


insert_sql = "insert INTO ods_"+table_name + "("+table_cols+",etl_last_updt_time) VALUES"

sql = "select " + table_cols + " from "+table_name + " where "+where_conditions
count_sql = "select count(1)" + " from "+table_name + " where "+where_conditions
# 列数
cls_list = table_cols.split(",")


# 获取数据并且组装成insert语句
def get_data(query_sql):
    insert_values = insert_sql
    oracleHelper = OracleHelper.OracleHelper()
    rows = oracleHelper.query(query_sql)
    print("分页获取数据:"+ str(oracleHelper.rowcount()))
    currrent_time =time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    for row in rows:
        str_row = "("
        for index in range(0, len(cls_list)):
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

        str_row += "'"+currrent_time + "')/n,"
        insert_values = insert_values + str_row

    oracleHelper.close()
    return insert_values[0:len(insert_values) - 1]


def write2file(insert_values, page):
    fo = open(table_name+"_"+str(page) + ".sql", "wb+")
    fo.write(insert_values)
    fo.close()


# 获取数据总数
def get_count(count_sql):
    oracleHelper = OracleHelper.OracleHelper()
    row_count = oracleHelper.queryRow(count_sql)
    count = row_count[0]
    print("数据总数:"+ str(count))
    oracleHelper.close()
    return count


count = get_count(count_sql)

page_size = 100
page_num = int(math.ceil(float(count)/float(page_size)))
current_page = 0
print "pagesize :" + str(page_num)

for i in range(0, page_num):
    start = i * page_size
    end = start + page_size
    page_sql = "select " + table_cols + " from (select rownum rn, " + table_cols + " from "+table_name \
           +" where rownum <="+str(end)+" AND "+where_conditions + ") where rn>" + str(start) + " AND "+where_conditions
    print page_sql
    sql_text = get_data(page_sql)
    # print sql_text
    write2file(sql_text, i)

# print insert_values[0:len(insert_values) - 1]

