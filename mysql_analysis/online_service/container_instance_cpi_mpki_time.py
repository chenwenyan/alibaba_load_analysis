import matplotlib.pyplot as plt
import MySQLdb as mdb


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
        cursor.execute(
            "select ts/3600-11 as time_stamp, avg(avg_cpi), avg(avg_mpki) from container_usage group by ts having time_stamp <= 4")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        time = [x[0] for x in res]
        avg_cpi_4 = [x[1] for x in res]
        print(avg_cpi_4)
        avg_mpki_4 = [x[2] for x in res]

        cursor.execute(
            "select ts/3600-11 as time_stamp, avg(avg_cpi), avg(avg_mpki) from container_usage group by ts having time_stamp > 4 and time_stamp <= 8")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        time = [x[0] for x in res]
        avg_cpi_4_8 = [x[1] for x in res]
        print(avg_cpi_4_8)
        avg_mpki_4_8 = [x[2] for x in res]

        cursor.execute(
            "select ts/3600-11 as time_stamp, avg(avg_cpi), avg(avg_mpki) from container_usage group by ts having time_stamp > 8")
        records = cursor.fetchall()
        list_records = list(records)
        res[:] = map(list, list_records)
        time = [x[0] for x in res]
        avg_cpi_8_12 = [x[1] for x in res]
        print(avg_cpi_8_12)
        avg_mpki_8_12 = [x[2] for x in res]

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

    fig = plt.figure(figsize=(9, 4))
    ax1 = fig.add_subplot(1, 3, 1)
    ax2 = fig.add_subplot(1, 3, 2, sharex=ax1, sharey=ax1)
    ax3 = fig.add_subplot(1, 3, 3, sharex=ax1, sharey=ax1)
    # ax1.set_ylim(0, 20)
    # ax1.set_xlim(0, 0.15)
    # plt.yticks([y for y in range(20 + 1) if y % 5 == 0])
    # 直方图
    # ax1.hist(avg_cpi_4, normed=False, alpha=1.0, bins=50)
    # ax2.hist(avg_cpi_4_8, normed=False, alpha=1.0, bins=50)
    # ax3.hist(avg_cpi_8_12, normed=False, alpha=1.0, bins=50)
    # ax1.set_xlabel('cpi ts less 4(h)')
    # ax2.set_xlabel('cpi ts more 4(h) less 8(h)')
    # ax3.set_xlabel('cpi ts more 8(h) less 12(h)')
    # ax1.set_ylabel('portion of instance')
    #
    # plt.savefig("../../imgs_mysql/container_instance_cpi_time")

    ax1.hist(avg_mpki_4, normed=False, alpha=1.0, bins=50)
    ax2.hist(avg_mpki_4_8, normed=False, alpha=1.0, bins=50)
    ax3.hist(avg_mpki_8_12, normed=False, alpha=1.0, bins=50)
    ax1.set_xlabel('mpki ts less 4(h)')
    ax2.set_xlabel('mpki ts more 4(h) less 8(h)')
    ax3.set_xlabel('mpki ts more 8(h) less 12(h)')
    ax1.set_ylabel('portion of instance')

    plt.savefig("../../imgs_mysql/container_instance_mpki_time")
    plt.show()

if __name__ == '__main__':
    graph()
