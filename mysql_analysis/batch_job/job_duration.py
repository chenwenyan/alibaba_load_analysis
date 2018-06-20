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
    list_records = []
    short_records, medium_records, long_records = [], [], []
    try:
        cursor.execute(
            "select t.job_id, t.job_duration from(select job_id, max(modify_timestamp)-min(create_timestamp) as job_duration from batch_task where status='Terminated' group by job_id) t where t.job_duration > 0")
        records = cursor.fetchall()
        list_records = list(records)

        cursor.execute(
            "select t.job_id, job_duration from(select job_id, max(modify_timestamp)-min(create_timestamp) as job_duration from batch_task where status='Terminated'group by job_id) t where t.job_duration/3600 <= 0.2")
        records = cursor.fetchall()
        short_records = list(records)

        cursor.execute(
            "select t.job_id, job_duration from(select job_id, max(modify_timestamp)-min(create_timestamp) as job_duration from batch_task where status='Terminated'group by job_id) t where t.job_duration/3600 > 0.2 and t.job_duration/3600 <=5")
        records = cursor.fetchall()
        medium_records = list(records)

        cursor.execute(
            "select t.job_id, job_duration from(select job_id, max(modify_timestamp)-min(create_timestamp) as job_duration from batch_task where status='Terminated'group by job_id) t where t.job_duration/3600 > 5 and t.job_duration/3600 <= 10")
        records = cursor.fetchall()
        long_records = list(records)

        cursor.execute("SELECT a.job_id, avg(a.cpu), avg(a.mem) FROM ( SELECT job_id, avg(real_cpu_avg) AS cpu, avg(real_mem_avg) AS mem FROM batch_instance WHERE real_mem_avg > 0 GROUP BY task_id ) a WHERE a.job_id IN ( SELECT t.job_id FROM ( SELECT job_id, max(modify_timestamp) - min(create_timestamp) AS job_duration FROM batch_task WHERE STATUS = 'Terminated' GROUP BY job_id ) t WHERE t.job_duration / 3600 <= 0.2 ) GROUP BY a.job_id")
        records = cursor.fetchall()
        resource_record_of_short_job = list(records)

        cursor.execute("SELECT a.job_id, avg(a.cpu), avg(a.mem) FROM ( SELECT job_id, avg(real_cpu_avg) AS cpu, avg(real_mem_avg) AS mem FROM batch_instance WHERE real_mem_avg > 0 GROUP BY task_id ) a WHERE a.job_id IN ( SELECT t.job_id FROM ( SELECT job_id, max(modify_timestamp) - min(create_timestamp) AS job_duration FROM batch_task WHERE STATUS = 'Terminated' GROUP BY job_id ) t WHERE t.job_duration / 3600 > 0.2 and t.job_duration / 3600 <= 10) GROUP BY a.job_id")
        records = cursor.fetchall()
        resource_record_of_medium_job = list(records)

        cursor.execute("SELECT a.job_id, avg(a.cpu), avg(a.mem) FROM ( SELECT job_id, avg(real_cpu_avg) AS cpu, avg(real_mem_avg) AS mem FROM batch_instance WHERE real_mem_avg > 0 GROUP BY task_id ) a WHERE a.job_id IN ( SELECT t.job_id FROM ( SELECT job_id, max(modify_timestamp) - min(create_timestamp) AS job_duration FROM batch_task WHERE STATUS = 'Terminated' GROUP BY job_id ) t WHERE t.job_duration / 3600 > 5 and t.job_duration / 3600 <= 10) GROUP BY a.job_id")
        records = cursor.fetchall()
        resource_record_of_long_job = list(records)
        print(resource_record_of_long_job)

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
    job_duration = [x[1] / 3600 for x in res]
    print(job_duration)
    print(len(job_duration))
    print(max(job_duration))
    print(min(job_duration))

    less = np.zeros(9)
    for i in job_duration:
        if i >=0  and i < 1:
            less[0] = less[0] + 1
        elif i >= 1 and i < 2:
            less[1] = less[1] + 1
        elif i >= 2 and i < 3:
            less[2] = less[3] + 1
        elif i >= 3 and i < 4:
            less[3] = less[3] + 1
        elif i >= 4 and i < 5:
            less[4] = less[4] + 1
        elif i >= 5 and i < 6:
            less[5] = less[5] + 1
        elif i >= 6 and i < 7:
            less[6] = less[6] + 1
        elif i >= 7 and i < 8:
            less[7] = less[7] + 1
        elif i >= 8 and i < 9:
            less[8] = less[8] + 1

    # 绘图
    # fig = plt.figure(figsize=(9, 4))
    # ax1 = fig.add_subplot(121)
    # ax2 = fig.add_subplot(122)
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ax1.hist(job_duration, density=False, alpha=1.0, bins=100)
    print((max(job_duration) - min(job_duration)) /100)
    ax1.set_yscale('log')
    ax1.set_xlabel("job duration(h)")
    ax1.set_ylabel("job number")
    axins = inset_axes(ax1, width=1.5, height=1.5, loc='upper right')
    hist, bin_edges = np.histogram(job_duration, bins=100)
    cdf = np.cumsum(hist / sum(hist))
    axins.plot(bin_edges[1:], cdf, color='red', ls='-')
    axins.set_xscale('log')
    axins.set_xlabel("job duration(h)", fontsize=7)
    axins.set_ylabel("portion of job", fontsize=7)
    print(len(np.unique(job_duration)))

    plt.savefig("../../imgs_mysql/job_duration_hist.png")


    # 统计短job
    # res = []
    # res[:] = map(list, resource_record_of_short_job)
    # id_resource_record_of_short_job = [x[0] for x in res]
    # cpu_resource_record_of_short_job = [x[1] for x in res]
    # mem_resource_record_of_short_job = [x[2] for x in res]
    # ax1.hist(cpu_resource_record_of_short_job, density=False, alpha=0.7, bins=100)
    # ax1.set_xlabel("CPU utilization(short job)")
    # ax1.set_ylabel("job number(short job)")
    # ax2.hist(mem_resource_record_of_short_job, density=False, alpha=0.7, bins=100)
    # ax2.set_xlabel("memory utilization(short job)")
    # ax2.set_ylabel("job number(short job)")
    # ax1.set_ylim(0, 200)
    # ax2.set_ylim(0, 200)
    # plt.savefig("../../imgs_mysql/short_job_duration.png")

    # medium job
    # res = []
    # res[:] = map(list, resource_record_of_medium_job)
    # id_resource_record_of_medium_job = [x[0] for x in res]
    # cpu_resource_record_of_medium_job = [x[1] for x in res]
    # mem_resource_record_of_medium_job = [x[2] for x in res]
    # ax1.hist(cpu_resource_record_of_medium_job, density=False, alpha=0.7, bins=100)
    # ax1.set_xlabel("CPU utilization(medium job)")
    # ax1.set_ylabel("job number(medium job)")
    # ax2.hist(mem_resource_record_of_medium_job, density=False, alpha=0.7, bins=100)
    # ax2.set_xlabel("memory utilization(medium job)")
    # ax2.set_ylabel("job number(medium job)")
    # ax1.set_ylim(0, 15)
    # ax2.set_ylim(0, 15)
    # plt.savefig("../../imgs_mysql/medium_job_duration.png")

    # long job
    # res = []
    # res[:] = map(list, resource_record_of_long_job)
    # id_resource_record_of_long_job = [x[0] for x in res]
    # cpu_resource_record_of_long_job = [x[1] for x in res]
    # mem_resource_record_of_long_job = [x[2] for x in res]
    # ax1.hist(cpu_resource_record_of_long_job, density=False, alpha=0.7, bins=50)
    # ax1.set_xlabel("CPU utilization(long job)")
    # ax1.set_ylabel("job number(long job)")
    # ax1.set_yticks([0,1,2])
    # ax2.hist(mem_resource_record_of_long_job, density=False, alpha=0.7, bins=50)
    # ax2.set_xlabel("memory utilization(long job)")
    # ax2.set_ylabel("job number(long job)")
    # # ax2.set_xlim(0.0,0.001)
    # ax2.set_yticks([0,1,2])
    # plt.savefig("../imgs_mysql/long_job_duration.png")

    plt.show()

if __name__ == '__main__':
    graph()
