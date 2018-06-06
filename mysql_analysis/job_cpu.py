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
        cursor.execute("select a.job_id, avg(a.cpu), avg(a.mem) from (select job_id, avg(real_cpu_avg) as cpu, avg(real_mem_avg) as mem from batch_instance where real_mem_avg > 0 group by task_id) a group by job_id")
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
    print(max(avg_cpu))
    print(min(avg_cpu))
    print(max(avg_mem))
    print(min(avg_mem))


    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    # cpu
    # cdf
    # hist, bin_edges = np.histogram(avg_cpu, bins=len(np.unique(avg_cpu)))
    # cdf = np.cumsum(hist / sum(hist))
    # ax1.plot(bin_edges[1:], cdf, color='red', ls='-')
    # ax1.set_xlabel("average cpu per job")
    # ax1.set_ylabel("portion of job")
    # plt.savefig('../imgs_mysql/cdf_of_job_cpu.png')
    # # 直方图
    ax1.hist(avg_cpu, normed=False, alpha=1.0, bins=100)
    ax1.set_xlabel('average cpu per job')
    ax1.set_ylabel('job number')
    plt.savefig("../imgs_mysql/hist_of_job_cpu")

    # memory
    # hist, bin_edges = np.histogram(avg_mem, bins=len(np.unique(avg_mem)))
    # cdf = np.cumsum(hist / sum(hist))
    # ax1.plot(bin_edges[1:], cdf, color='red', ls='-')
    # ax1.set_xlabel("average memory per job")
    # ax1.set_ylabel("portion of job")
    # plt.savefig('../imgs_mysql/cdf_of_job_memory.png')
    # 直方图
    # ax1.hist(avg_mem, normed=False, alpha=1.0, bins=100)
    # ax1.set_xlabel('average memory per job')
    # ax1.set_ylabel('job number')
    # plt.savefig("../imgs_mysql/hist_of_job_memory")

    plt.show()


if __name__ == '__main__':
    graph()