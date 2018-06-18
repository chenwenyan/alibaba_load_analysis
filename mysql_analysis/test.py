import MySQLdb as mdb

def graph():
    # 连接数据库
    conn = mdb.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='alibaba_trace', charset='utf8')

    # 如果使用事务引擎，可以设置自动提交事务，或者在每次操作完成后手动提交事务conn.commit()
    conn.autocommit(1)  # conn.autocommit(True)

    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 因该模块底层其实是调用CAPI的，所以，需要先得到当前指向数据库的指针。

    try:
        # 查询数据条目
        # cursor.execute(
        #     "update batch_instance set category = 'mss' where job_id in (select DISTINCT job_id from batch_job_category where category_1 = 'mss')")


        # cursor.execute("select DISTINCT job_id from batch_job_category where category_1 = 'mss'")
        # result = cursor.fetchall()
        # list_result = list(result)
        # for i in list_result:
        #     print(i)
        #     cursor.execute("update batch_instance set category = 'mss' where job_id =(%d)" %(i))

        # cursor.execute("select DISTINCT machineID from batch_instance where machineID > 0")
        # result = cursor.fetchall()
        # list_result = list(result)
        # for i in list_result:
        #     print(i)
        #     cursor.execute("select DISTINCT job_id from batch_instance where job_id > 0 and machineID = (%d)" % (i))
        #     result = cursor.fetchall()
        #     list_job_id = list(result)
        #     for j in list_job_id:
        #         print(j)
        #         cursor.execute("update batch_instance a, batch_job_category b set a.category = b.category_1 where a.job_id = b.job_id ")

        # cursor.execute("update batch_instance set category = 'mms' where job_id = 562")
        # cursor.execute("update batch_instance a, batch_job_category b set a.category = b.category_1 where a.job_id = b.job_id")
        # cursor.execute("delete from machine_job where job_id not in (select job_id from batch_job_category)")
        machine_job = []
        a = []
        for i in range(0,1314):
            cursor.execute("select distinct job_id from batch_instance where machineID = (%d)" %(i))
            result = cursor.fetchall()
            result = list(result)
            a[i] = result
            print(a[i])
            machine_job = machine_job.append(a[i])

        # 如果没有设置自动提交事务，则这里需要手动提交一次
        conn.commit()

    except:
        import traceback
        traceback.print_exc()
        # 发生错误时回滚
        conn.rollback()
    finally:
        # 关闭游标连接
        cursor.close()
        # 关闭数据库连接
        conn.close()

if __name__ == '__main__':
    graph()
