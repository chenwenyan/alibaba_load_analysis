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
        # 将查询出的三维特征向量存储到batch_job_category中
        # cursor.execute(
        #     "select a.job_id, avg(a.cpu), avg(a.mem) from (select job_id, avg(real_cpu_avg) as cpu, avg(real_mem_avg) as mem from batch_instance where real_mem_avg > 0 group by task_id) a group by job_id ASC")
        # records = cursor.fetchall()
        # list_records = list(records)
        #
        # cursor.execute(
        #     "select t.job_id, t.job_duration from(select job_id, max(modify_timestamp)-min(create_timestamp) as job_duration from batch_task where status='Terminated' group by job_id) t where t.job_duration > 0 order by t.job_id")
        # job_duration_records = cursor.fetchall()
        # list_job_duration_records = list(job_duration_records)
        #
        # res = []
        # res[:] = map(list, list_records)
        # job_id = [x[0] for x in res]
        # avg_cpu = [x[1] for x in res]
        # avg_mem = [x[2] for x in res]

        # res = []
        # res[:] = map(list, list_job_duration_records)
        # job_id_list = [x[0] for x in res]
        # job_duration = [x[1] for x in res]

        # cursor.execute("delete from batch_job_category")
        # for i in range(len(job_id)):
        #     if job_id[i] == None:
        #         print(job_id[i])
        #     else:
        #         cursor.execute(
        #             "insert into batch_job_category(job_id, avg_cpu, avg_mem) VALUES (%d,  %f,  %f)" % (job_id[i], avg_cpu[i], avg_mem[i]))
        #         print(job_id[i])

        # for i in range(len(job_id_list)):
        #     cursor.execute("update batch_job_category set job_duration = (%f) where job_id = (%d)" % (job_duration[i]/3600, job_id_list[i]))

        # 查询各个类别的job
        # short_less_less
        cursor.execute("select job_id from batch_job_category where job_duration <= 0.2 and avg_cpu <= 0.5 and avg_mem <= 0.04 ")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'short_less_less' where job_id = (%d)" % (job_id_list[i]))

        # short_less_mid
        cursor.execute("select job_id from batch_job_category where job_duration <= 0.2 and  avg_cpu <= 0.5 and 0.04 < avg_mem and avg_mem<= 0.07 ")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'short_less_mid' where job_id = (%d)" % (job_id_list[i]))

        # short_mid_less
        cursor.execute("select job_id from batch_job_category where  job_duration <= 0.2 and  0.5 < avg_cpu and avg_cpu <= 1.5 and avg_mem <= 0.04 ")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'short_mid_less' where job_id = (%d)" % (job_id_list[i]))

        # short_mid_mid
        cursor.execute("select job_id from batch_job_category where job_duration <= 0.2 and  0.5 < avg_cpu and avg_cpu <= 1.5 and 0.04<avg_mem and avg_mem <= 0.07 ")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'short_mid_mid' where job_id = (%d)" % (job_id_list[i]))

        # short_hungry_less
        cursor.execute("select job_id from batch_job_category where job_duration <= 0.2 and  1.5 <= avg_cpu and avg_mem <= 2.5 and avg_mem <= 0.04 ")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'short_hungry_less' where job_id = (%d)" % (job_id_list[i]))

        # short_hungry_mid
        cursor.execute("select job_id from batch_job_category where job_duration <= 0.2 and  1.5 <= avg_cpu and avg_cpu <= 2.5 and 0.04<avg_mem and avg_mem<= 0.07 ")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'short_hungry_mid' where job_id = (%d)" % (job_id_list[i]))

        # medium_less_less
        cursor.execute("select job_id from batch_job_category where 0.2 <job_duration and job_duration <= 5 and  avg_cpu <= 0.5 and avg_mem <= 0.04")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'medium_less_less' where job_id = (%d)" % (job_id_list[i]))

        # medium_less_mid
        cursor.execute("select job_id from batch_job_category where 0.2 <job_duration and job_duration <= 5 and  avg_cpu <= 0.5 and 0.04<avg_mem and avg_mem <= 0.07")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'medium_less_mid' where job_id = (%d)" % (job_id_list[i]))

        # medium_mid_less
        cursor.execute("select job_id from batch_job_category where 0.2 <job_duration and job_duration <= 5 and  0.5<avg_cpu and avg_cpu<= 1.5 and avg_mem <= 0.04")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'medium_mid_less' where job_id = (%d)" % (job_id_list[i]))

        # medium_mid_mid
        cursor.execute("select job_id from batch_job_category where 0.2 <job_duration and job_duration <= 5 and  0.5<avg_cpu and avg_cpu <= 1.5 and 0.04<avg_mem  and avg_mem <= 0.07")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'medium_mid_mid' where job_id = (%d)" % (job_id_list[i]))

        # medium_hungry_less
        cursor.execute("select job_id from batch_job_category where 0.2 <job_duration and job_duration <= 5 and  1.5<avg_cpu and avg_cpu <= 2.5 and avg_mem <= 0.04")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'medium_hungry_less' where job_id = (%d)" % (job_id_list[i]))

        # medium_hungry_mid
        cursor.execute("select job_id from batch_job_category where 0.2 <job_duration and job_duration <= 5 and  1.5<avg_cpu and avg_cpu <= 2.5 and 0.04<avg_mem and avg_mem<= 0.07")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'medium_hungry_mid' where job_id = (%d)" % (job_id_list[i]))

        # long_less_less
        cursor.execute("select job_id from batch_job_category where 5 <job_duration and job_duration <= 10 and avg_cpu <= 0.5 and avg_mem <= 0.04")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'long_less_less' where job_id = (%d)" % (job_id_list[i]))

        # long_less_mid
        cursor.execute("select job_id from batch_job_category where 5 <job_duration and job_duration <= 10 and avg_cpu <= 0.5 and 0.04<avg_mem and avg_mem <= 0.07")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'long_less_mid' where job_id = (%d)" % (job_id_list[i]))

        # long_mid_less
        cursor.execute("select job_id from batch_job_category where 5 <job_duration and job_duration <= 10 and 0.5<avg_cpu and avg_cpu <= 1.5 and avg_mem <= 0.04")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'long_mid_less' where job_id = (%d)" % (job_id_list[i]))

        # long_mid_mid
        cursor.execute("select job_id from batch_job_category where 5 <job_duration and job_duration <= 10 and 0.5<avg_cpu and avg_cpu <= 1.5 and 0.04<avg_mem and avg_mem<= 0.07")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'long_mid_mid' where job_id = (%d)" % (job_id_list[i]))

        # long_hungry_less
        cursor.execute("select job_id from batch_job_category where 5 <job_duration and job_duration <= 10 and 1.5<avg_cpu and avg_cpu <= 2.5 and avg_mem <= 0.04")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'long_hungry_less' where job_id = (%d)" % (job_id_list[i]))

        # long_hungry_mid
        cursor.execute("select job_id from batch_job_category where 5 <job_duration and job_duration <= 10 and 1.5<avg_cpu and avg_cpu <= 2.5 and 0.04<avg_mem and avg_mem <= 0.07")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)
        job_id_list = [x[0] for x in res]
        for i in range(len(job_id_list)):
            cursor.execute("update batch_job_category set category = 'long_hungry_mid' where job_id = (%d)" % (job_id_list[i]))

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

    # res = []
    # res[:] = map(list, list_records)
    # job_id = [x[0] for x in res]
    # avg_cpu = [x[1] for x in res]
    # avg_mem = [x[2] for x in res]
    #
    # fig = plt.figure()
    # ax1 = fig.add_subplot(1, 1, 1)
    #
    # # 直方图
    # ax1.hist(avg_mem, normed=False, alpha=1.0, facecolor='g', bins=50)
    # ax1.set_xlabel('average memory per job')
    # ax1.set_ylabel('job number')
    # plt.savefig("../imgs_mysql/hist_of_job_memory")

    plt.show()


if __name__ == '__main__':
    graph()
