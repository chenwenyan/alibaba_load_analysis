import matplotlib.pyplot as plt
import MySQLdb as mdb
from matplotlib import colors
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
    instance_num_list = []
    X = []
    x = 0
    step = 60
    while x < 86400-step:
        x = x + step
        X.append(x)
    try:
        fig = plt.figure()
        ax1 = fig.add_subplot(1, 1, 1)
        norm = colors.Normalize(vmin=0, vmax=100)
        for machine_id in range(1, 1314):
            for i in X:
                print(i)
                cursor.execute(
                    'select count(id) from batch_instance_2 where machineID = %d and start_timestamp <= %d and end_timestamp >= %d' % (machine_id,i,i))
                records = cursor.fetchall()
                list_records = list(records)
                res = []
                res[:] = map(list, list_records)
                instance_num = [x[0] for x in res]
                instance_num_list.append(instance_num)
            X_label = [x/3600 for x in X]
            ax1.scatter(X_label, range(1, 1314), c=instance_num_list, norm=norm, alpha=0.5, s=2.0)

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

    # 绘制渐变色标注
    gci = plt.scatter(X_label, range(1, 1314), c=instance_num_list, norm=norm, alpha=0.5, s=2.0)
    cbar = plt.colorbar(gci)
    cbar.set_label('used')
    cbar.set_ticks(np.linspace(0, 100, 6))
    ax1.set_xlabel("time(h)")
    ax1.set_ylabel("machineID")
    plt.savefig("../../imgs_mysql/instance_number_machine_id")
    plt.show()

if __name__ == '__main__':
    graph()
