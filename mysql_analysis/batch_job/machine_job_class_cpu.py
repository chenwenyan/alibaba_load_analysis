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
            "select job_id, avg(real_cpu_avg),avg(real_mem_avg) from batch_instance group by job_id having job_id in (select job_id from batch_job_category where category='short_less_less')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        sll_avg_cpu = [x[1] for x in res]
        sll_avg_mem = [x[2] for x in res]

        cursor.execute(
            "select job_id, avg(real_cpu_avg),avg(real_mem_avg) from batch_instance group by job_id having job_id in (select job_id from batch_job_category where category='short_mid_less')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        sml_avg_cpu = [x[1] for x in res]
        sml_avg_mem = [x[2] for x in res]

        cursor.execute(
            "select job_id, avg(real_cpu_avg),avg(real_mem_avg) from batch_instance group by job_id having job_id in (select job_id from batch_job_category where category='short_mid_mid')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        smm_avg_cpu = [x[1] for x in res]
        smm_avg_mem = [x[2] for x in res]

        cursor.execute(
            "select job_id, avg(real_cpu_avg),avg(real_mem_avg) from batch_instance group by job_id having job_id in (select job_id from batch_job_category where category='short_hungry_less')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        shl_avg_cpu = [x[1] for x in res]
        shl_avg_mem = [x[2] for x in res]

        cursor.execute(
            "select job_id, avg(real_cpu_avg),avg(real_mem_avg) from batch_instance group by job_id having job_id in (select job_id from batch_job_category where category='medium_less_less')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        mll_avg_cpu = [x[1] for x in res]
        mll_avg_mem = [x[2] for x in res]

        cursor.execute(
            "select job_id, avg(real_cpu_avg),avg(real_mem_avg) from batch_instance group by job_id having job_id in (select job_id from batch_job_category where category='medium_mid_less')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        mml_avg_cpu = [x[1] for x in res]
        mml_avg_mem = [x[2] for x in res]

        # cdf图
        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        # ax1.set_ylabel("portion of job")
        # ax1.set_xlabel("average cpu utilization of instance for job")
        # hist, bin_edges = np.histogram(sll_avg_cpu, bins=len(np.unique(sll_avg_cpu)))
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='green',  label='short-less-less')
        # hist, bin_edges = np.histogram(sml_avg_cpu, bins=len(np.unique(sml_avg_cpu)))
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='red',label='short-mid-less')
        # hist, bin_edges = np.histogram(smm_avg_cpu, bins=len(np.unique(smm_avg_cpu)))
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='yellow', label='short-mid-mid')
        # hist, bin_edges = np.histogram(shl_avg_cpu, bins=len(np.unique(shl_avg_cpu)))
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='black',  label='short-hungry-less')
        # hist, bin_edges = np.histogram(mll_avg_cpu, bins=len(np.unique(mll_avg_cpu)))
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='blue', label='medium-less-less')
        # hist, bin_edges = np.histogram(mml_avg_cpu, bins=len(np.unique(mml_avg_cpu)))
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='pink', label='medium-mid-less')
        # ax1.legend(loc="best")
        #
        # plt.savefig('../imgs_mysql/machine_job_class_task_cpu_cdf.png')

        ax1.set_ylabel("portion of job")
        ax1.set_xlabel("average memory utilization of instance for job")
        hist, bin_edges = np.histogram(sll_avg_mem, bins=len(np.unique(sll_avg_mem)))
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, color='green',  label='short-less-less')
        hist, bin_edges = np.histogram(sml_avg_mem, bins=len(np.unique(sml_avg_mem)))
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, color='red',label='short-mid-less')
        hist, bin_edges = np.histogram(smm_avg_mem, bins=len(np.unique(smm_avg_mem)))
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, color='yellow', label='short-mid-mid')
        hist, bin_edges = np.histogram(shl_avg_mem, bins=len(np.unique(shl_avg_mem)))
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, color='black',  label='short-hungry-less')
        hist, bin_edges = np.histogram(mll_avg_mem, bins=len(np.unique(mll_avg_mem)))
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, color='blue', label='medium-less-less')
        hist, bin_edges = np.histogram(mml_avg_mem, bins=len(np.unique(mml_avg_mem)))
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, color='pink', label='medium-mid-less')
        ax1.legend(loc="best")

        plt.savefig('../imgs_mysql/machine_job_class_task_mem_cdf.png')
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
