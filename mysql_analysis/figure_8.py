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
    container_usage, server_usage = [],[]
    try:
        cursor.execute("select ts, max(load1) as max_load1, avg(load1) as avg_load1, max(load5) as max_load5, avg(load5) as avg_load5, max(load15) as max_load15, avg(load15) as avg_load15 from container_usage group by ts")
        container_usage = cursor.fetchall()
        list_container_usage = list(container_usage)
        cursor.execute("select time_stamp, max(load1) as max_load1, avg(load1) as avg_load1, max(load5) as max_load5, avg(load5) as avg_load5, max(load15) as max_load15, avg(load15) as avg_load15 from server_usage group by time_stamp")
        server_usage = cursor.fetchall()
        list_server_usage = list(server_usage)
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
    res[:] = map(list, container_usage)
    timestamp = [x[0] for x in res]
    max_load1_arr = [x[1] for x in res]
    avg_load1_arr = [x[2] for x in res]
    max_load5_arr = [x[3] for x in res]
    avg_load5_arr = [x[4] for x in res]
    max_load15_arr = [x[5] for x in res]
    avg_load15_arr = [x[6] for x in res]

    server_res = []
    server_res[:] = map(list, server_usage)
    server_timestamp = [x[0] for x in server_res]
    server_max_load1_arr = [x[1] for x in server_res]
    server_avg_load1_arr = [x[2] for x in server_res]
    server_max_load5_arr = [x[3] for x in server_res]
    server_avg_load5_arr = [x[4] for x in server_res]
    server_max_load15_arr = [x[5] for x in server_res]
    server_avg_load15_arr = [x[6] for x in server_res]

    fig = plt.figure(figsize=(15, 6))
    ax1 = fig.add_subplot(2, 3, 1)
    ax2 = fig.add_subplot(2, 3, 2)
    ax3 = fig.add_subplot(2, 3, 3)
    ax4 = fig.add_subplot(2, 3, 4)
    ax5 = fig.add_subplot(2, 3, 5)
    ax6 = fig.add_subplot(2, 3, 6)

    timestamp = [(x / 3600 - 11) for x in timestamp]
    max_load1_arr = [x / 100 for x in max_load1_arr]
    avg_load1_arr = [x / 100 for x in avg_load1_arr]

    ax1.plot(timestamp, max_load1_arr, label='max')
    ax1.plot(timestamp, avg_load1_arr, label='avg')
    # 最大cpu和最小cpu之间用颜色填充
    ax1.fill_between(timestamp, avg_load1_arr, max_load1_arr, facecolor='#FFB6C1')
    ax1.legend(loc='best')
    ax1.set_ylim(0, 1.4)
    ax1.set_xlim(0, max(timestamp))
    ax1.axhline(1.0, ls="--", color="r")
    ax1.set_xlabel('time(hour)')
    ax1.set_ylabel('portion of CPU loads (container)')
    ax1.set_title('1 min')

    max_load5_arr = [x / 100 for x in max_load5_arr]
    avg_load5_arr = [x / 100 for x in avg_load5_arr]
    ax2.plot(timestamp, max_load5_arr, label='max')
    ax2.plot(timestamp, avg_load5_arr, label='avg')
    # 最大cpu和最小cpu之间用颜色填充
    ax2.fill_between(timestamp, avg_load5_arr, max_load5_arr, facecolor='#FFB6C1')
    ax2.legend(loc='best')
    ax2.set_ylim(0, 1.4)
    ax2.set_xlim(0, max(timestamp))
    ax2.axhline(1.0, ls="--", color="r")
    ax2.set_xlabel('time(hour)')
    ax2.set_title('5 min')

    max_load15_arr = [x / 100 for x in max_load15_arr]
    avg_load15_arr = [x / 100 for x in avg_load15_arr]
    ax3.plot(timestamp, max_load15_arr, label='max')
    ax3.plot(timestamp, avg_load15_arr, label='avg')
    # 最大cpu和最小cpu之间用颜色填充
    ax3.fill_between(timestamp, avg_load15_arr, max_load15_arr, facecolor='#FFB6C1')
    ax3.legend(loc='best')
    ax3.set_ylim(0, 1.4)
    ax3.set_xlim(0, max(timestamp))
    ax3.axhline(1.0, ls="--", color="r")
    ax3.set_xlabel('time(hour)')
    ax3.set_title('15 min')

    server_timestamp = [(x / 3600 - 11) for x in server_timestamp]
    server_max_load1_arr = [x / 100 for x in server_max_load1_arr]
    server_avg_load1_arr = [x / 100 for x in server_avg_load1_arr]

    ax4.plot(server_timestamp, server_max_load1_arr, label='max')
    ax4.plot(server_timestamp, server_avg_load1_arr, label='avg')
    # 最大cpu和最小cpu之间用颜色填充
    ax4.fill_between(server_timestamp, 0, server_max_load1_arr, facecolor='#FFB6C1')
    ax4.legend(loc='best')
    ax4.set_ylim(0, 1.4)
    ax4.set_xlim(0, max(server_timestamp))
    ax4.axhline(1.0, ls="--", color="r")
    ax4.set_xlabel('time(hour)')
    ax4.set_ylabel('portion of CPU loads (machine)')

    server_max_load5_arr = [x / 100 for x in server_max_load5_arr]
    server_avg_load5_arr = [x / 100 for x in server_avg_load5_arr]
    ax5.plot(server_timestamp, server_max_load5_arr, label='max')
    ax5.plot(server_timestamp, server_avg_load5_arr, label='avg')
    # 最大cpu和最小cpu之间用颜色填充
    ax5.fill_between(server_timestamp, 0, server_max_load5_arr, facecolor='#FFB6C1')
    ax5.legend(loc='best')
    ax5.set_ylim(0, 1.4)
    ax5.set_xlim(0, max(server_timestamp))
    ax5.axhline(1.0, ls="--", color="r")
    ax5.set_xlabel('time(hour)')

    server_max_load15_arr = [x / 100 for x in server_max_load15_arr]
    server_avg_load15_arr = [x / 100 for x in server_avg_load15_arr]
    ax6.plot(server_timestamp, server_max_load15_arr, label='max')
    ax6.plot(server_timestamp, server_avg_load15_arr, label='avg')
    # 最大cpu和最小cpu之间用颜色填充
    ax6.fill_between(server_timestamp, 0, server_max_load15_arr, facecolor='#FFB6C1')
    ax6.legend(loc='best')
    ax6.set_ylim(0, 1.4)
    ax6.set_xlim(0, max(server_timestamp))
    ax6.axhline(1.0, ls="--", color="r")
    ax6.set_xlabel('time(hour)')

    plt.savefig('../imgs_mysql/figure_8.png')
    plt.show()

if __name__ == '__main__':
    graph()
