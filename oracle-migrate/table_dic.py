# encoding:utf-8

__author__ = 'michael'

table_dic = [
    {"table_name": "settlement_detail",
     "table_cols": "ID,ACTUAL_PAYMENT,BIG_CAT_NAME,BIZ_TYPE,BIZ_TYPE_NAME,BRAND_NAME,BRAND_NO,COST,ESTIMATE_PAYMENT,GROSS_PROFIT,MID_CAT_NAME,PREPAY,PRODUCT_NAME,PRODUCT_NO,QUANTITY,MAN_HOUR,SETTLEMENT_ID,SMALL_CAT_NAME,TAX,SAP_TAX,SAP_COST,SOURCE_ORDER_ID",
     "where_conditions": "settlement_id IN ( SELECT ID FROM SETTLEMENT where to_char(creation_date,'yyyymm')='201701')"},

]
