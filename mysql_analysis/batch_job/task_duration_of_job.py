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
        cursor.execute("select t.job_id, avg(t.task_duration) from (select job_id, task_id, avg(end_timestamp-start_timestamp)as task_duration from batch_instance where status='Terminated' group by task_id) t where t.task_duration > 0 group by job_id ")
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
    avg_task_duration = [float(x[1])/3600 for x in res]
    print(max(avg_task_duration))
    print(min(avg_task_duration))


    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    # 直方图
    ax1.hist(avg_task_duration, density=False, alpha=1.0, bins=100)
    ax1.set_xlabel('average task duration(hour)')
    ax1.set_ylabel('job number')
    ax1.set_yscale('log')
    # cdf
    axins = inset_axes(ax1, width=1.5, height=1.5, loc='upper right')
    hist, bin_edges = np.histogram(avg_task_duration, bins=len(np.unique(avg_task_duration)))
    cdf = np.cumsum(hist / sum(hist))
    axins.plot(bin_edges[1:], cdf, color='red', ls='-')
    axins.set_xscale('log')
    # axins.set_xticks(fontsize=7)
    # axins.set_yticks(fontsize=7)
    axins.set_xlabel("average task duration(hour)", fontsize=7)
    axins.set_ylabel("portion of job", fontsize=7)
    # axins.set_yticks([])

    plt.savefig("../../imgs_mysql/hist_of_task_duration_of_job")
    plt.show()

if __name__ == '__main__':
    graph()
