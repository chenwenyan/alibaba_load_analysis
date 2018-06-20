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
        cursor.execute("select job_id, count(job_id) from batch_task where job_id != 0 group by job_id")
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
    task_num = [x[1] for x in res]
    print(max(task_num))
    print(min(task_num))


    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    # 直方图
    ax1.hist(task_num, density=False, alpha=1.0, bins=max(task_num))
    ax1.set_xlabel('task number per job')
    ax1.set_ylabel('job number')
    ax1.set_yscale('log')
    # cdf
    axins = inset_axes(ax1, width=1.5, height=1.5, loc='upper right')
    hist, bin_edges = np.histogram(task_num, bins=max(task_num))
    cdf = np.cumsum(hist / sum(hist))
    print(max(bin_edges[1:]))
    print(max(cdf))
    axins.plot(bin_edges[1:], cdf, color='red', ls='-')
    axins.set_xscale('log')
    axins.set_xlabel("task number per job", fontsize=7)
    axins.set_ylabel("portion of job", fontsize=7)
    # axins.set_yticks([])

    plt.savefig("../../imgs_mysql/hist_of_job_task_num")
    plt.show()

if __name__ == '__main__':
    graph()
