# encoding:utf-8

import cx_Oracle
from datetime import *


class OracleHelper:
    def __init__(self):
        self.charset = 'utf8'
        try:
            # self.conn = cx_Oracle.connect('scott/tiger@DESKTOP-IASSOVJ/orcl')
            self.conn = cx_Oracle.connect('bomsowner', 'zaq12wsx', '192.168.220.168:1521/bomsdb')
            self.cur = self.conn.cursor()
        except cx_Oracle.Error as e:
            print("Oracle Error %d: %s" % (e.args[0], e.args[1]))

    def selectDb(self, db):
        try:
            self.conn.select_db(db)
        except cx_Oracle.Error as e:
            print("Oracle Error %d: %s" % (e.args[0], e.args[1]))

    def query(self, sql):
        try:
            n = self.cur.execute(sql)
            return n
        except cx_Oracle.Error as e:
            print("Mysql Error:%s\nSQL:%s" % (e, sql))
            raise e

    def queryRow(self, sql):
        self.query(sql)
        result = self.cur.fetchone()
        return result

    def queryAll(self, sql):
        self.query(sql)
        result = self.cur.fetchall()
        desc = self.cur.description
        d = []
        for inv in result:
            _d = {}
            for i in range(0, len(inv)):
                _d[desc[i][0]] = str(inv[i])
            d.append(_d)
        return d

    def insert(self, p_table_name, p_data):
        for key in p_data:
            if (isinstance(p_data[key], str) or isinstance(p_data[key], datetime)):
                if str(p_data[key]) == "None":
                    p_data[key] = 'null'
                else:
                    p_data[key] = "'" + str(p_data[key]).replace('%', '％').replace('\'', '') + "'"
            else:
                p_data[key] = str(p_data[key])

        key = ','.join(p_data.keys())
        value = ','.join(p_data.values())
        real_sql = "INSERT INTO " + p_table_name + " (" + key + ") VALUES (" + value + ")"
        return self.query(real_sql)

    def getLastInsertId(self):
        return self.cur.lastrowid

    def rowcount(self):
        return self.cur.rowcount

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()
