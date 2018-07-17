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
    #61050
    #43200 12h
    try:
        cursor.execute(
            "select machineID, count(instance_id) from container_event where instance_id in (select DISTINCT instance_id from container_usage where ts <= 43200) group by machineID order by machineID")
        records = cursor.fetchall()
        list_records = list(records)

        cursor.execute(
            "select machineID, count(id) from batch_instance where start_timestamp <= 43200 and end_timestamp >= 43200 group by machineID order by machineID")
        records = cursor.fetchall()
        batch_instance_list_records = list(records)

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
    machineID = [x[0] for x in res]
    instance_num = [x[1] for x in res]
    print(max(instance_num))
    print(np.average(instance_num))
    print(min(instance_num))

    res[:] = map(list, batch_instance_list_records)
    batch_machineID = [x[0] for x in res]
    batch_instance_num = [x[1] for x in res]
    print(max(batch_instance_num))
    print(np.average(batch_instance_num))
    print(min(batch_instance_num))

    fig = plt.figure(figsize=(16, 4))
    ax1 = fig.add_subplot(1, 1, 1)
    # # 直方图
    # for i in
    ax1.plot(batch_machineID, batch_instance_num, linewidth=0.5, label='batch workload')
    ax1.plot(machineID, instance_num, linewidth=0.5, label='online service')
    ax1.set_xlabel('machineID')
    ax1.set_ylabel('instance number')
    # ax1.set_yticks([y for y in range(0, 21) if y % 2 == 0])
    # ax1.set_ylim(0, 20)
    ax1.set_xlim(1, 1313)
    ax1.legend(loc='best')

    # plt.savefig("../imgs_mysql/batch_job_container_instance_at_43200", quality=99)
    plt.savefig("../paper_img/batch_job_container_instance_at_43200", dpi=2000)
    plt.show()


if __name__ == '__main__':
    graph()
