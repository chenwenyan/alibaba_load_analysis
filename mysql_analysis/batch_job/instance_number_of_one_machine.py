import matplotlib.pyplot as plt
import MySQLdb as mdb
import numpy as np
from mpl_toolkits.axes_grid1.inset_locator import inset_axes


def graph():
    # 连接数据库
    conn = mdb.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='alibaba_trace', charset='utf8')

    # 如果使用事务引擎，可以设置自动提交事务，或者在每次操作完成后手动提交事务conn.commit()
    conn.autocommit(1)  # conn.autocommit(True)

    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 因该模块底层其实是调用CAPI的，所以，需要先得到当前指向数据库的指针。

    # 统计id为656的machine每隔1min其上运行的instance数目随时间的变化曲线
    # 时间 min 11  max 83488
    # 16.492097701149426
    # 77
    # 14.419722222222223
    # 0
    # 0.01972222222222222
    instance_num_list = []
    X = []
    x = 11
    X.append(x)
    step = 60
    while x <= 83488-step:
        x = x + step
        X.append(x)
    try:
        for i in X:
            print(i)
            cursor.execute(
                'select count(id) from batch_instance_2 where machineID = 656 and start_timestamp <= %d and end_timestamp >= %d' % (i,i))
            records = cursor.fetchall()
            list_records = list(records)
            res = []
            res[:] = map(list, list_records)
            instance_num = [x[0] for x in res]
            instance_num_list.append(instance_num)

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

    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    X = [x/3600 for x in X]
    print(len(X))
    print(len(instance_num_list))
    print(np.average(instance_num_list))
    print(np.max(instance_num_list))
    print(X[instance_num_list.index(max(instance_num_list))])
    print(np.min(instance_num_list))
    print(X[instance_num_list.index(min(instance_num_list))])
    ax1.plot(X, instance_num_list)
    ax1.axhline(np.average(instance_num_list),linewidth=1, color='r', ls='--', label='average value')
    plt.annotate('average value', xy=(20, 20), xytext=(22, 28),arrowprops=dict(facecolor='black', shrink=0.05))
    ax1.set_xlabel("time(hour)")
    ax1.set_ylabel("instance number")
    # ax1.set_xlim(0, 24)
    ax1.set_ylim(0, 80)
    plt.savefig("../../imgs_mysql/instance_number_all_time")
    plt.show()

if __name__ == '__main__':
    graph()
