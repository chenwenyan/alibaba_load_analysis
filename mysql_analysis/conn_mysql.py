# --coding=utf8--
import configparser

import sys
import MySQLdb

def init_db():
    try:
        conn = MySQLdb.connect(host=conf.get('Database', 'host'),
                               user=conf.get('Database', 'user'),
                               passwd=conf.get('Database', 'passwd'),
                               db=conf.get('Database', 'db'),
                               charset='utf8')
        return conn
    except:
        print("Error:数据库连接错误")
        return None

def select_demo(conn, sql):
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    except:
        print("Error:数据库连接错误")
        return None

if __name__ == '__main__':
    conf = configparser.ConfigParser()
    conf.read('mysql.conf')
    conn = init_db()
    sql = "select * from %s" % 'batch_task'
    data = select_demo(conn, sql)
    pass