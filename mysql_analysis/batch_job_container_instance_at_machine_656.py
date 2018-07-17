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
        instance_num_list = []
        X = []
        x = 11
        X.append(x)
        step = 60
        while x <= 83488-step:
            x = x + step
            X.append(x)
        for i in X:
            cursor.execute(
                'select count(id) from batch_instance_2 where machineID = 656 and start_timestamp <= %d and end_timestamp >= %d' % (i,i))
            records = cursor.fetchall()
            list_records = list(records)
            res = []
            res[:] = map(list, list_records)
            instance_num = [x[0] for x in res]
            print(instance_num)
            instance_num_list.append(instance_num)

        container_X = []
        container_instance_num_list = []
        container_x = 39600
        container_X.append(x)
        while container_x <= 82500-step:
            container_x = container_x + step
            container_X.append(container_x)
        for j in container_X:
            cursor.execute(
                'select count(DISTINCT instance_id) from container_usage where instance_id in (select instance_id from container_event where machineID = 656) and  ts <= (%d)' % (j))
            records = cursor.fetchall()
            list_records = list(records)
            res = []
            res[:] = map(list, list_records)
            container_instance_num = [x[0] for x in res]
            container_instance_num_list.append(container_instance_num)
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

    print(max(instance_num_list))
    print(np.average(instance_num_list))
    print(min(instance_num_list))

    print(max(container_instance_num_list))
    print(np.average(container_instance_num_list))
    print(min(container_instance_num_list))
    # 77
    # 16.492097701149426
    # [0]
    # [10]
    # 10.0
    # [10]

    fig = plt.figure(figsize=(16, 4))
    ax1 = fig.add_subplot(1, 1, 1)
    # # 直方图
    # for i in
    ax1.plot(X, instance_num_list, linewidth=0.5, label='batch workload')
    ax1.plot(container_X, container_instance_num_list, linewidth=0.5, label='online service')
    ax1.set_xlabel('time(s)')
    ax1.set_ylabel('instance number')
    # ax1.set_yticks([y for y in range(0, 21) if y % 2 == 0])
    # ax1.set_ylim(0, 20)
    # ax1.set_xlim(1, 1313)
    ax1.legend(loc='best')

    # plt.savefig("../imgs_mysql/batch_job_container_instance_at_machine_656", quality=99)
    plt.savefig("../paper_img/batch_job_container_instance_at_machine_656", dpi=2000)
    plt.show()


if __name__ == '__main__':
    graph()
