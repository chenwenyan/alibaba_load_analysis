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
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category_1 ='sss'"
        )
        records = cursor.fetchall()
        sss_records = list(records)

        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category_1 ='sms'"
        )
        records = cursor.fetchall()
        sms_records = list(records)

        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category_1 ='mss'"
        )
        records = cursor.fetchall()
        mss_records = list(records)

        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category_1 ='mms'"
        )
        records = cursor.fetchall()
        mms_records = list(records)

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
    res[:] = map(list, sss_records)
    sss_job_id = [x[0] for x in res]
    sss_job_duration = [x[1] for x in res]
    sss_avg_cpu = [x[2] for x in res]
    sss_avg_mem = [x[3] for x in res]

    res[:] = map(list, sms_records)
    sms_job_id = [x[0] for x in res]
    sms_job_duration = [x[1] for x in res]
    sms_avg_cpu = [x[2] for x in res]
    sms_avg_mem = [x[3] for x in res]

    res[:] = map(list, mss_records)
    mss_job_id = [x[0] for x in res]
    mss_job_duration = [x[1] for x in res]
    mss_avg_cpu = [x[2] for x in res]
    mss_avg_mem = [x[3] for x in res]

    res[:] = map(list, mms_records)
    mms_job_id = [x[0] for x in res]
    mms_job_duration = [x[1] for x in res]
    mms_avg_cpu = [x[2] for x in res]
    mms_avg_mem = [x[3] for x in res]


    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    # job duration
    hist, bin_edges = np.histogram(sss_job_duration, bins=100)
    cdf = np.cumsum(hist / sum(hist))
    ax1.plot(bin_edges[1:], cdf, label='sss')
    hist, bin_edges = np.histogram(sms_job_duration, bins=100)
    cdf = np.cumsum(hist / sum(hist))
    ax1.plot(bin_edges[1:], cdf, label='sms')
    hist, bin_edges = np.histogram(mss_job_duration, bins=100)
    cdf = np.cumsum(hist / sum(hist))
    ax1.plot(bin_edges[1:], cdf, label='mss')
    hist, bin_edges = np.histogram(mms_job_duration, bins=100)
    cdf = np.cumsum(hist / sum(hist))
    ax1.plot(bin_edges[1:], cdf, label='mms')
    ax1.set_xlabel("job duration(hour)")
    ax1.set_ylabel("portion of job")
    ax1.legend(loc='lower right')
    # plt.savefig('../../imgs_mysql/cdf_batch_job_category_job_duration')
    plt.savefig('../../paper_img/cdf_job_duration_batch_job_groups.pdf')
    plt.show()

    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    # cpu
    hist, bin_edges = np.histogram(sss_avg_cpu, bins=100)
    cdf = np.cumsum(hist / sum(hist))
    ax1.plot(bin_edges[1:], cdf, label='sss')
    hist, bin_edges = np.histogram(sms_avg_cpu, bins=100)
    cdf = np.cumsum(hist / sum(hist))
    ax1.plot(bin_edges[1:], cdf, label='sms')
    hist, bin_edges = np.histogram(mss_avg_cpu, bins=100)
    cdf = np.cumsum(hist / sum(hist))
    ax1.plot(bin_edges[1:], cdf, label='mss')
    hist, bin_edges = np.histogram(mms_avg_cpu, bins=100)
    cdf = np.cumsum(hist / sum(hist))
    ax1.plot(bin_edges[1:], cdf, label='mms')
    ax1.set_xlabel("average cpu cores")
    ax1.set_ylabel("portion of job")
    ax1.legend(loc='lower right')
    # plt.savefig('../../imgs_mysql/cdf_batch_job_category_cpu')
    plt.savefig('../../paper_img/cdf_avg_cpu_batch_job_groups.pdf')
    plt.show()

    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    # memory
    hist, bin_edges = np.histogram(sss_avg_mem, bins=100)
    cdf = np.cumsum(hist / sum(hist))
    ax1.plot(bin_edges[1:], cdf, label='sss')
    hist, bin_edges = np.histogram(sms_avg_mem, bins=100)
    cdf = np.cumsum(hist / sum(hist))
    ax1.plot(bin_edges[1:], cdf, label='sms')
    hist, bin_edges = np.histogram(mss_avg_mem, bins=100)
    cdf = np.cumsum(hist / sum(hist))
    ax1.plot(bin_edges[1:], cdf, label='mss')
    hist, bin_edges = np.histogram(mms_avg_mem, bins=100)
    cdf = np.cumsum(hist / sum(hist))
    ax1.plot(bin_edges[1:], cdf, label='mms')
    ax1.set_xlabel("average memory utilization(%)")
    ax1.set_ylabel("portion of job")
    ax1.legend(loc='lower right')
    # plt.savefig('../../imgs_mysql/cdf_batch_job_category_mem')
    plt.savefig('../../paper_img/cdf_avg_memory_batch_job_groups.pdf')
    plt.show()


if __name__ == '__main__':
    graph()
