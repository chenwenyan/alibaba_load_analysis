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
        cursor.execute("select job_id, sum(instance_num) from batch_task group by job_id ")
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
    ids = [x[0] for x in res]
    instances_num = [x[1] for x in res]
    print(max(instances_num))
    print(min(instances_num))

    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    ax1.set_ylabel('job number')
    ax1.set_xlabel('total instance number per job')
    # ax1.set_ylim(0, 9000)
    ax1.set_yscale("log")
    # ax1.set_xscale("log")
    # ax1.set_xlim(0,10000)
    instances_num = [float(x) for x in instances_num]
    ax1.hist(instances_num, density=False, alpha=1.0, facecolor='g', bins=len(np.unique(instances_num)))

    plt.savefig('../../imgs_mysql/job_instance_num.png')
    plt.show()

if __name__ == '__main__':
    graph()
