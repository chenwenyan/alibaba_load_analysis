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

    plan_result, used_result = [], []
    try:
        cursor.execute("select id, real_cpu_max, real_cpu_avg, real_mem_max, real_mem_avg from batch_instance where real_mem_avg > 0")
        records = cursor.fetchall()
        plan_result = list(records)
        print(plan_result)
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
    res[:] = map(list, plan_result)
    ids = [x[0] for x in res]
    real_cpu_max = [x[1] for x in res]
    real_cpu_avg = [x[2] for x in res]
    real_mem_max = [x[3] for x in res]
    real_mem_avg = [x[4] for x in res]

    instances_num = len(real_cpu_max)
    unique_cpu_max = np.unique(real_cpu_max)
    portion_of_instances_cpu_max = []
    for i in unique_cpu_max:
        num = real_cpu_max.count(i)/instances_num
        portion_of_instances_cpu_max.append(num)

    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    ax1.set_ylabel('portion of instances')
    ax1.set_xlabel('average utilization of 12 hours')
    ax1.plot(unique_cpu_max, portion_of_instances_cpu_max)

    plt.savefig('../imgs_mysql/figure_7.png')
    plt.show()

if __name__ == '__main__':
    graph()
