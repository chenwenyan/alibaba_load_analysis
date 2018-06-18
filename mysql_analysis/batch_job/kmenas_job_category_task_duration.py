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
    res = []
    try:
        cursor.execute(
            "select t.job_id, avg(t.instance_duration), t.task_id from (select job_id, task_id, avg( end_timestamp - start_timestamp ) as instance_duration FROM batch_instance WHERE STATUS = 'Terminated' group by task_id) t  group by job_id having job_id in (select job_id from batch_job_category where category_1='sss')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        sss_avg_instance_duration = [float(x[1])/3600 for x in res]

        cursor.execute(
            "select t.job_id, avg(t.instance_duration), t.task_id from (select job_id, task_id, avg( end_timestamp - start_timestamp ) as instance_duration FROM batch_instance WHERE STATUS = 'Terminated' group by task_id) t  group by job_id having job_id in (select job_id from batch_job_category where category_1='sms')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        sms_avg_instance_duration = [float(x[1])/3600 for x in res]

        cursor.execute(
            "select t.job_id, avg(t.instance_duration), t.task_id from (select job_id, task_id, avg( end_timestamp - start_timestamp ) as instance_duration FROM batch_instance WHERE STATUS = 'Terminated' group by task_id) t  group by job_id having job_id in (select job_id from batch_job_category where category_1='mss')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        mss_avg_instance_duration = [float(x[1])/3600 for x in res]

        cursor.execute(
            "select t.job_id, avg(t.instance_duration), t.task_id from (select job_id, task_id, avg( end_timestamp - start_timestamp ) as instance_duration FROM batch_instance WHERE STATUS = 'Terminated' group by task_id) t  group by job_id having job_id in (select job_id from batch_job_category where category_1='mms')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        mms_avg_instance_duration = [float(x[1])/3600 for x in res]


        # 绘图 hist直方图
        fig = plt.figure(figsize=(10, 4))
        ax1 = fig.add_subplot(141)
        ax1.set_yscale('log')
        ax2 = fig.add_subplot(142, sharex=ax1, sharey=ax1)
        ax3 = fig.add_subplot(143, sharex=ax1, sharey=ax1)
        ax4 = fig.add_subplot(144, sharex=ax1, sharey=ax1)
        ax2.set_xlabel("task duration for batch job's category")
        ax1.set_ylabel("portion of job")
        # ax4.set_ylabel("portion of job")
        ax1.hist(sss_avg_instance_duration, normed=False, alpha=1.0, bins=100)
        ax2.hist(sms_avg_instance_duration, normed=False, alpha=1.0, bins=100)
        ax3.hist(mss_avg_instance_duration, normed=False, alpha=1.0, bins=100)
        ax4.hist(mms_avg_instance_duration, normed=False, alpha=1.0, bins=100)
        plt.savefig('../../imgs_mysql/kmeans_job_category_task_duration.png')

        # cdf图
        # fig = plt.figure()
        # ax1 = fig.add_subplot(111)
        # ax1.set_ylabel("portion of job")
        # ax1.set_xlabel("average task duration of every type job(h)")
        # hist, bin_edges = np.histogram(sss_avg_instance_duration, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='green',  label='sss')
        # hist, bin_edges = np.histogram(sms_avg_instance_duration, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='red',label='sms')
        # hist, bin_edges = np.histogram(mss_avg_instance_duration, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='yellow', label='mss')
        # hist, bin_edges = np.histogram(mms_avg_instance_duration, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='blue',  label='mms')
        # ax1.legend(loc="best")
        # plt.savefig('../../imgs_mysql/kmeans_job_category_task_duration_cdf.png')

        plt.show()

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

if __name__ == '__main__':
    graph()
