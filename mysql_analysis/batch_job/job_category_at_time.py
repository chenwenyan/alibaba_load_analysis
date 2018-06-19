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
    res = []
    try:
        cursor.execute("select machineID from workload_pattern where sss = 1 and sms = 1 and mss = 1 and mms = 1")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_1111 = [x[0] for x in res]

        cursor.execute("select machineID from workload_pattern where sss = 1 and sms = 1 and mss = 1 and mms = 0")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_1110 = [x[0] for x in res]

        cursor.execute("select machineID from workload_pattern where sss = 1 and sms = 1 and mss = 0 and mms = 1")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_1101 = [x[0] for x in res]

        cursor.execute("select machineID from workload_pattern where sss = 1 and sms = 1 and mss = 0 and mms = 0")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_1100 = [x[0] for x in res]

        cursor.execute("select machineID from workload_pattern where sss = 1 and sms = 0 and mss = 1 and mms = 1")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_1011 = [x[0] for x in res]

        cursor.execute("select machineID from workload_pattern where sss = 1 and sms = 0 and mss = 1 and mms = 0")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_1010 = [x[0] for x in res]

        cursor.execute("select machineID from workload_pattern where sss = 1 and sms = 0 and mss = 0 and mms = 1")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_1001 = [x[0] for x in res]

        cursor.execute("select machineID from workload_pattern where sss = 1 and sms = 0 and mss = 0 and mms = 0")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_1000 = [x[0] for x in res]

        cursor.execute("select machineID from workload_pattern where sss = 0 and sms = 1 and mss = 1 and mms = 1")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_0111 = [x[0] for x in res]

        cursor.execute("select machineID from workload_pattern where sss = 0 and sms = 1 and mss = 1 and mms = 0")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_0110 = [x[0] for x in res]

        cursor.execute("select machineID from workload_pattern where sss = 0 and sms = 1 and mss = 0 and mms = 0")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_0100 = [x[0] for x in res]

        cursor.execute("select machineID from workload_pattern where sss = 0 and sms = 1 and mss = 0 and mms = 1")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_0101 = [x[0] for x in res]

        cursor.execute("select machineID from workload_pattern where sss = 0 and sms = 0 and mss = 1 and mms = 1")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_0011 = [x[0] for x in res]

        cursor.execute("select machineID from workload_pattern where sss = 0 and sms = 0 and mss = 1 and mms = 0")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_0010 = [x[0] for x in res]

        cursor.execute("select machineID from workload_pattern where sss = 0 and sms = 0 and mss = 0 and mms = 1")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_0001 = [x[0] for x in res]

        cursor.execute("select machineID from workload_pattern where sss = 0 and sms = 0 and mss = 0 and mms = 0")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        machineID_0000 = [x[0] for x in res]


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
    X = range(1,17)
    Y = [len(machineID_0000), len(machineID_0001), len(machineID_0010), len(machineID_0011), len(machineID_0100), len(machineID_0101), len(machineID_0110),
         len(machineID_0111), len(machineID_1000), len(machineID_1001), len(machineID_1010), len(machineID_1011), len(machineID_1100), len(machineID_1101),
        len(machineID_1110), len(machineID_1111)]
    ax1.bar(X, Y)
    for a,b in zip(X,Y):
        ax1.text(a, b+0.05, '%d' % b , ha='center', va= 'bottom',fontsize=11)
    ax1.set_xlabel("workload pattern")
    ax1.set_ylabel("machine number")
    ax1.set_ylim(0, 800)
    # plt.pie(Y,labels=X,autopct='%1.1f%%',shadow=False,startangle=90,pctdistance = 0.6)
    plt.savefig("../../imgs_mysql/job_category_at_time")
    plt.show()

if __name__ == '__main__':
    graph()
