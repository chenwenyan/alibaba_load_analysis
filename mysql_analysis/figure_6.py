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
    list_records = []
    try:
        cursor.execute("select ts, max(cpu_util) as max_cpu,avg(cpu_util) as avg_cpu, min(cpu_util) as min_cpu,max(mem_util) as max_mem, avg(mem_util) as avg_mem, min(mem_util) as min_mem from container_usage group by ts")
        records = cursor.fetchall()
        list_records = list(records)
        print(list_records)
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
    timestamp = [x[0] for x in res]
    max_cpu = [x[1] for x in res]
    avg_cpu = [x[2] for x in res]
    min_cpu = [x[3] for x in res]
    max_mem = [x[4] for x in res]
    avg_mem = [x[5] for x in res]
    min_mem = [x[6] for x in res]

    fig = plt.figure(figsize=(10, 4))
    ax1 = fig.add_subplot(1, 2, 1)
    ax2 = fig.add_subplot(1, 2, 2)
    # 设置图表标题并给坐标轴加上标签
    # plt.title("cpu utilization")
    # plt.title("memory utilization")
    ax1.set_ylabel('machine cpu utilization')
    ax2.set_ylabel('machine memory utilization')
    ax1.set_xlabel('time(h)')
    ax2.set_xlabel('time(h)')

    timestamp = [(x / 3600 - 11) for x in timestamp]
    max_cpu = [x/100 for x in max_cpu]
    avg_cpu = [x/100 for x in avg_cpu]
    min_cpu = [x/100 for x in min_cpu]
    max_mem = [x/100 for x in max_mem]
    avg_mem = [x/100 for x in avg_mem]
    min_mem = [x/100 for x in min_mem]

    ax1.plot(timestamp, max_cpu, label='max')
    ax1.plot(timestamp, avg_cpu, label='avg')
    ax1.plot(timestamp, min_cpu, label='min')

    ax2.plot(timestamp, max_mem, label='max')
    ax2.plot(timestamp, avg_mem, label='avg')
    ax2.plot(timestamp, min_mem, label='min')

    # 最大cpu和最小cpu之间用颜色填充
    ax1.fill_between(timestamp, min_cpu, max_cpu, facecolor='#FFB6C1')
    ax2.fill_between(timestamp, min_mem, max_mem, facecolor='#FFB6C1')
    ax1.set_ylim(0, 1.4)
    ax1.set_xlim(0, max(timestamp))
    ax2.set_ylim(0, 1.4)
    ax2.set_xlim(0, max(timestamp))
    ax1.legend(loc='best')
    ax2.legend(loc='best')
    # plt.savefig('../images/server_usage_cpu_maxminavg.png')
    plt.savefig('../imgs_mysql/figure_6.png')
    plt.show()

if __name__ == '__main__':
    graph()
