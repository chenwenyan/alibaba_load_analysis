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
        cursor.execute("select a.job_id, avg(a.cpu), avg(a.mem) from (select job_id, avg(real_cpu_avg) as cpu, avg(real_mem_avg) as mem from batch_instance where real_mem_avg > 0 and job_id is not NULL group by task_id) a group by job_id")
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
    job_id = [x[0] for x in res]
    avg_cpu = [x[1] for x in res]
    avg_mem = [x[2] for x in res]
    avg_mem.remove(max(avg_mem))
    print(np.average(avg_cpu))
    print(max(avg_cpu))
    print(min(avg_cpu))
    # 0.6074789585338134
    # 2.327999973297119
    # 0.007692307520371217

    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    # cpu
    # 直方图
    ax1.hist(avg_cpu, density=False, alpha=1.0, bins=100)
    ax1.set_xlabel('average cpu cores per job')
    ax1.set_ylabel('job number')
    axins = inset_axes(ax1, width=1.5, height=1.5, loc='upper right')
    hist, bin_edges = np.histogram(avg_cpu, bins=100)
    cdf = np.cumsum(hist / sum(hist))
    axins.plot(bin_edges[1:], cdf, color='red', ls='-')
    axins.set_xlabel("average cpu cores per job", fontsize=8)
    axins.set_ylabel("portion of job", fontsize=8)
    axins.tick_params(labelsize=8)
    # plt.savefig("../../imgs_mysql/hist_of_job_cpu")
    plt.savefig("../../paper_img/batch_job_cpu_cores.pdf")

    # memory
    # hist, bin_edges = np.histogram(avg_mem, bins=len(np.unique(avg_mem)))
    # cdf = np.cumsum(hist / sum(hist))
    # ax1.plot(bin_edges[1:], cdf, color='red', ls='-')
    # ax1.set_xlabel("average memory per job")
    # ax1.set_ylabel("portion of job")
    # plt.savefig('../imgs_mysql/cdf_of_job_memory.png')
    # 直方图
    # print(np.average(avg_mem))
    # print(np.max(avg_mem))
    # print(np.min(avg_mem))
    # 0.012941284939225393
    # 0.06200323498749233
    # 0.0003468537179287523
    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    ax1.hist(avg_mem, density=False, alpha=1.0, bins=100)
    ax1.set_xlabel('normalized average memory per job')
    ax1.set_ylabel('job number')
    # ax1.set_yscale('log')
    axins = inset_axes(ax1, width=1.5, height=1.5, loc='upper right')
    hist, bin_edges = np.histogram(avg_mem, bins=100)
    cdf = np.cumsum(hist / sum(hist))
    axins.plot(bin_edges[1:], cdf, color='red', ls='-')
    # axins.xticks(fontsize=7)
    # axins.yticks(fontsize=7)
    axins.set_xlabel("average memory per job", fontsize=8)
    axins.set_ylabel("portion of job", fontsize=8)
    axins.tick_params(labelsize=8)
    # plt.savefig("../../imgs_mysql/hist_of_job_memory")
    plt.savefig("../../paper_img/batch_job_memory_utilization.pdf")
    plt.show()

if __name__ == '__main__':
    graph()
