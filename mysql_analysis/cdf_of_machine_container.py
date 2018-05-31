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
    server_records, container_records = [], []
    try:
        cursor.execute("select machineID, avg(cpu), avg(memory), avg(disk) from server_usage group by machineID")
        records = cursor.fetchall()
        server_records = list(records)

        cursor.execute(
            "select instance_id, avg(cpu_util), avg(mem_util), avg(disk_util) from container_usage group by instance_id")
        records = cursor.fetchall()
        container_records = list(records)

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

    server_res = []
    server_res[:] = map(list, server_records)
    instance_id = [x[0] for x in server_res]
    server_avg_cpu = [x[1] for x in server_res]
    server_avg_mem = [x[2] for x in server_res]
    server_avg_disk = [x[3] for x in server_res]
    print(max(server_avg_cpu))
    print(min(server_avg_cpu))

    res = []
    res[:] = map(list, container_records)
    machine_id = [x[0] for x in res]
    avg_cpu = [x[1] for x in res]
    avg_mem = [x[2] for x in res]
    avg_disk = [x[3] for x in res]
    print(avg_cpu)
    print(max(avg_cpu))
    print(min(avg_cpu))

    fig = plt.figure(figsize=(12, 4))
    ax1 = fig.add_subplot(1, 3, 1)
    ax2 = fig.add_subplot(1, 3, 2)
    ax3 = fig.add_subplot(1, 3, 3)
    # server
    server_hist, server_bin_edges = np.histogram(server_avg_cpu, bins=len(np.unique(server_avg_cpu)))
    server_cdf = np.cumsum(server_hist / sum(server_hist))
    ax1.plot(server_bin_edges[1:], server_cdf, color='blue', label='instance on server')
    # container
    hist, bin_edges = np.histogram(avg_cpu, bins=len(np.unique(avg_cpu)))
    cdf = np.cumsum(hist / sum(hist))
    print(bin_edges)
    ax1.plot(bin_edges[1:], cdf, color='green', label='instance on container')

    # server
    server_hist, server_bin_edges = np.histogram(server_avg_cpu, bins=len(np.unique(server_avg_mem)))
    server_cdf = np.cumsum(server_hist / sum(server_hist))
    ax2.plot(server_bin_edges[1:], server_cdf, color='blue', ls='--', label='instance on server')
    # container
    hist, bin_edges = np.histogram(avg_mem, bins=len(np.unique(avg_mem)))
    cdf = np.cumsum(hist / sum(hist))
    print(bin_edges)
    ax2.plot(bin_edges[1:], cdf, color='green', ls='--', label='instance on container')

    # server
    server_hist, server_bin_edges = np.histogram(server_avg_cpu, bins=len(np.unique(server_avg_disk)))
    server_cdf = np.cumsum(server_hist / sum(server_hist))
    ax3.plot(server_bin_edges[1:], server_cdf, color='blue', ls='-.', label='instance on server')
    # container
    hist, bin_edges = np.histogram(avg_disk, bins=len(np.unique(avg_disk)))
    cdf = np.cumsum(hist / sum(hist))
    ax3.plot(bin_edges[1:], cdf, color='green', ls='-.', label='instance on container')

    ax1.set_ylabel('portion of machine/container')
    ax1.set_xlabel('cpu utilization')
    ax1.set_ylim(0, 1.4)
    # ax2.set_ylabel('portion of instance')
    ax2.set_xlabel('memory utilization')
    ax2.set_ylim(0, 1.4)
    # ax3.set_ylabel('portion of instance')
    ax3.set_xlabel('disk utilization')
    ax3.set_ylim(0, 1.4)
    # ax1.grid(linestyle='--')
    ax1.legend(loc="best")
    ax2.legend(loc="best")
    ax3.legend(loc="best")
    # ax1.axhline(1.0, ls="--", color="red")
    # ax2.axhline(1.0, ls="--", color="red")
    # ax3.axhline(1.0, ls="--", color="red")
    plt.savefig('../imgs_mysql/cdf.png')
    plt.show()


if __name__ == '__main__':
    graph()
