import MySQLdb as mdb
import numpy as np
import matplotlib.pyplot as plt


def CountRangeKey(list, key):
    res = 0
    for i in list:
        if i <= key:
            res = res + 1
    return res


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
        cursor.execute(
            "SELECT (end_timestamp-start_timestamp) as instance_duration  FROM batch_instance WHERE status = 'Terminated' and real_mem_avg != 0")
        instance_duration = cursor.fetchall()
        list_instance_duration = list(instance_duration)
        arr_instance_duration = [x[0] for x in list_instance_duration]

        unique_instance_duration = np.unique(arr_instance_duration)
        arr_sum_instance_duration = []
        for i in unique_instance_duration:
            sum_instance = CountRangeKey(arr_instance_duration, i) / len(instance_duration)
            arr_sum_instance_duration.append(sum_instance)
        X = [x / 3600 for x in unique_instance_duration]

        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.plot(X, arr_sum_instance_duration)
        ax.set_xlabel('instance duration(hour)')
        ax.set_ylabel('portion of instance')
        # ax = plt.axes(xscale='log')
        ax.set_xscale('log')
        ax.grid(linestyle='--')
        plt.savefig('../imgs_mysql/figure_9.png')
        plt.show()

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
