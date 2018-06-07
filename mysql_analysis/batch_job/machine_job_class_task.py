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
            "select job_id, count(task_id) from batch_task GROUP by job_id HAVING job_id in (select job_id from batch_job_category where category='short_less_less')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        sll_instance_num = [x[1] for x in res]
        print(sll_instance_num)
        print(max(sll_instance_num))
        print(min(sll_instance_num))

        cursor.execute(
            "select job_id, count(task_id) from batch_task GROUP by job_id HAVING job_id in (select job_id from batch_job_category where category='short_mid_less')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        sml_instance_num = [x[1] for x in res]
        print(sml_instance_num)
        print(max(sml_instance_num))
        print(min(sml_instance_num))

        cursor.execute(
            "select job_id, count(task_id) from batch_task GROUP by job_id HAVING job_id in (select job_id from batch_job_category where category='short_mid_mid')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        smm_instance_num = [x[1] for x in res]
        print(smm_instance_num)
        print(max(smm_instance_num))
        print(min(smm_instance_num))

        cursor.execute(
            "select job_id, count(task_id) from batch_task GROUP by job_id HAVING job_id in (select job_id from batch_job_category where category='short_hungry_less')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        shl_instance_num = [x[1] for x in res]
        print(shl_instance_num)
        print(max(shl_instance_num))
        print(min(shl_instance_num))

        cursor.execute(
            "select job_id, count(task_id) from batch_task GROUP by job_id HAVING job_id in (select job_id from batch_job_category where category='medium_less_less')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        mll_instance_num = [x[1] for x in res]
        print(mll_instance_num)
        print(max(mll_instance_num))
        print(min(mll_instance_num))

        cursor.execute(
            "select job_id, count(task_id) from batch_task GROUP by job_id HAVING job_id in (select job_id from batch_job_category where category='medium_mid_less')")
        records = cursor.fetchall()
        result = list(records)
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        job_id = sorted(instance_ids)
        mml_instance_num = [x[1] for x in res]
        print(mml_instance_num)
        print(max(mml_instance_num))
        print(min(mml_instance_num))

        # 绘图 hist直方图
        fig = plt.figure(figsize=(12, 5))
        ax1 = fig.add_subplot(2, 3, 1)
        ax2 = fig.add_subplot(2, 3, 2)
        ax3 = fig.add_subplot(2, 3, 3)
        ax4 = fig.add_subplot(2, 3, 4)
        ax5 = fig.add_subplot(2, 3, 5)
        ax6 = fig.add_subplot(2, 3, 6)
        ax5.set_xlabel("task number of task for job")
        ax1.set_ylabel("portion of job")
        ax4.set_ylabel("portion of job")
        ax1.hist(sll_instance_num, normed=False, alpha=1.0, bins=100)
        ax2.hist(sml_instance_num, normed=False, alpha=1.0, bins=100)
        ax3.hist(smm_instance_num, normed=False, alpha=1.0, bins=100)
        ax4.hist(shl_instance_num, normed=False, alpha=1.0, bins=100)
        ax5.hist(mll_instance_num, normed=False, alpha=1.0, bins=100)
        ax6.hist(mml_instance_num, normed=False, alpha=1.0, bins=100)

        # cdf图
        # fig = plt.figure()
        # ax1 = fig.add_subplot(111)
        # ax1.set_ylabel("portion of job")
        # ax1.set_xlabel("task number of every type job")
        # hist, bin_edges = np.histogram(sll_instance_num, bins=len(np.unique(sll_instance_num)))
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='green',  label='short-less-less')
        # hist, bin_edges = np.histogram(sml_instance_num, bins=len(np.unique(sml_instance_num)))
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='red',label='short-mid-less')
        # hist, bin_edges = np.histogram(smm_instance_num, bins=len(np.unique(smm_instance_num)))
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='yellow', label='short-mid-mid')
        # hist, bin_edges = np.histogram(shl_instance_num, bins=len(np.unique(shl_instance_num)))
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='black',  label='short-hungry-less')
        # hist, bin_edges = np.histogram(mll_instance_num, bins=len(np.unique(mll_instance_num)))
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='blue', label='medium-less-less')
        # hist, bin_edges = np.histogram(mml_instance_num, bins=len(np.unique(mml_instance_num)))
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, color='pink', label='medium-mid-less')
        # ax1.legend(loc="best")

        plt.savefig('../imgs_mysql/machine_job_class_task.png')
        # plt.savefig('../imgs_mysql/machine_job_class_task_cdf.png')
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
