import matplotlib.pyplot as plt
import MySQLdb as mdb
import numpy as np
from sklearn.cluster import KMeans


def graph():
    # 连接数据库
    conn = mdb.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='alibaba_trace', charset='utf8')

    # 如果使用事务引擎，可以设置自动提交事务，或者在每次操作完成后手动提交事务conn.commit()
    conn.autocommit(1)  # conn.autocommit(True)

    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 因该模块底层其实是调用CAPI的，所以，需要先得到当前指向数据库的指针。

    try:
        # cursor.execute("select job_id, max(modify_timestamp)-min(create_timestamp) from batch_task where status = 'Terminated' group by job_id  ")
        # records = cursor.fetchall()
        # result = list(records)
        # print(result)

        cursor.execute("SELECT t.job_id, t.avg_cpu, t.avg_mem FROM batch_job_category t WHERE t.job_duration <= 0.1534")
        records = cursor.fetchall()
        s_result = list(records)
        print(s_result)

        cursor.execute("SELECT t.job_id, t.avg_cpu, t.avg_mem FROM batch_job_category t WHERE t.job_duration > 0.1534")
        records = cursor.fetchall()
        m_result = list(records)
        print(m_result)

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

    res = []
    res[:] = map(list, s_result)
    s_job_ids = [x[0] for x in res]
    s_avg_cpu = [x[1] for x in res]
    s_avg_mem = [x[2] for x in res]

    res = []
    res[:] = map(list, m_result)
    m_job_ids = [x[0] for x in res]
    m_avg_cpu = [x[1] for x in res]
    m_avg_mem = [x[2] for x in res]

    fig = plt.figure(figsize=(9,4))
    ax1 = fig.add_subplot(121)
    ax2 = fig.add_subplot(122, sharex=ax1, sharey=ax1)
    # ax1.hist(s_avg_cpu,bins=100)
    # ax2.hist(m_avg_cpu,bins=100)
    # ax1.set_xlabel("average cpu for short job")
    # ax2.set_xlabel("average cpu for medium job")
    # ax1.set_ylabel("portion of job")
    # plt.savefig('../../imgs_mysql/k-means_job_duration_cpu_mem.png')

    ax1.hist(s_avg_mem,bins=100)
    ax2.hist(m_avg_mem,bins=100)
    ax1.set_xlabel("average memory for short job")
    ax2.set_xlabel("average memory for medium job")
    ax1.set_ylabel("portion of job")
    plt.savefig('../../imgs_mysql/k-means_job_duration_memory.png')

    plt.show()

if __name__ == '__main__':
    graph()
