import matplotlib.pyplot as plt
import MySQLdb as mdb
import numpy as np


def graph():
    # 连接数据库
    conn = mdb.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='alibaba_trace', charset='utf8')

    # 如果使用事务引擎，可以设置自动提交事务，或者在每次操作完成后手动提交事务conn.commit()
    conn.autocommit(1)  # conn.autocommit(True)

    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 因该模块底层其实是调用CAPI的，所以，需要先得到当前指向数据库的指针。

    try:
        cursor.execute("select instance_id, avg(cpu_util), avg(mem_util), avg(disk_util) from container_usage group by instance_id")
        records = cursor.fetchall()
        result = list(records)
        print(result)

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
    res[:] = map(list, result)
    instance_ids = [x[0] for x in res]
    instance_ids = sorted(instance_ids)
    avg_cpu = [x[1] for x in res]
    print(max(avg_cpu))
    print(min(avg_cpu))
    avg_mem = [x[2] for x in res]
    print(max(avg_mem))
    print(min(avg_mem))
    avg_disk = [x[3] for x in res]
    print(max(avg_disk))
    print(min(avg_disk))
    # 绘图
    fig = plt.figure(figsize=(9,4))
    ax1 = fig.add_subplot(1, 3, 1)
    ax2 = fig.add_subplot(1, 3, 2)
    ax3 = fig.add_subplot(1, 3, 3)
    ax1.set_xlabel('average cpu utilization')
    ax2.set_xlabel('average memory utilization')
    ax3.set_xlabel('average disk utilization')
    ax1.set_ylabel('portion of instance')
    ax1.set_ylim(0, 50)
    ax2.set_ylim(0, 50)
    ax3.set_ylim(0, 50)
    ax1.hist(avg_cpu, normed=False, alpha=1.0,  bins=len(np.unique(avg_cpu)))
    ax2.hist(avg_mem, normed=False, alpha=1.0,  bins=len(np.unique(avg_mem)))
    ax3.hist(avg_disk, normed=False, alpha=1.0, bins=len(np.unique(avg_disk)))

    plt.savefig('../imgs_mysql/container_instance_usage.png')
    plt.show()

if __name__ == '__main__':
    graph()
