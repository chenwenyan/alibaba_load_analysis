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
    try:
        cursor.execute("select  machineID, count(*) from batch_instance_2 where start_timestamp <= 43200 and end_timestamp >= 43200  group by machineID")
        records = cursor.fetchall()
        list_records = list(records)

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
    res[:] = map(list, list_records)
    machine_id = [x[0] for x in res]
    instance_num = [x[1] for x in res]
    print(np.average(instance_num))
    print(np.max(instance_num))
    print(np.min(instance_num))
#   22.639318885448915
#   32
#   16

    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    ax1.hist(instance_num, bins=100)
    ax1.set_xlabel("instance number of machine")
    ax1.set_ylabel("machine number")
    ax1.set_xticks([x for x in range(16, max(instance_num) + 1)])  # x标记step设置为1
    # cdf
    axins = inset_axes(ax1, width=1.5, height=1.5, loc='upper right')
    hist, bin_edges = np.histogram(instance_num, bins=len(np.unique(instance_num)))
    cdf = np.cumsum(hist / sum(hist))
    axins.plot(bin_edges[1:], cdf, color='red', ls='-')
    axins.set_xlabel("instance number of machine", fontsize=8)
    axins.set_ylabel("portion of machine", fontsize=8)
    axins.tick_params(labelsize=8)
    plt.savefig("../../imgs_mysql/machine_number_of_12_hour")
    plt.show()

if __name__ == '__main__':
    graph()
