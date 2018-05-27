import MySQLdb as mdb
import numpy as np
import matplotlib.pyplot as plt

def CountAnalysis():
    # 连接数据库
    conn = mdb.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='alibaba_trace', charset='utf8')

    # 如果使用事务引擎，可以设置自动提交事务，或者在每次操作完成后手动提交事务conn.commit()
    conn.autocommit(1)  # conn.autocommit(True)

    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 因该模块底层其实是调用CAPI的，所以，需要先得到当前指向数据库的指针。

    try:
        # 查询数据条目
        batch_task = 'batch_task'
        job_num = cursor.execute('SELECT DISTINCT job_id FROM %s' % batch_task)
        task_num = cursor.execute('SELECT DISTINCT task_id FROM %s' % batch_task)
        instance_num = cursor.execute("select distinct id from batch_instance")
        failed_task = cursor.execute("select task_id from batch_task where status = 'Failed'")
        terminated_task = cursor.execute("select task_id from batch_task where status = 'Terminated'")
        waiting_task = cursor.execute("select task_id from batch_task where status = 'Waiting'")
        print('job number:' + str(job_num))
        print('task number:' + str(task_num))
        print('instance number:' + str(instance_num))
        print('failed task number:' + str(failed_task))
        print('terminated task number:' + str(terminated_task))
        print('waiting task number:' + str(waiting_task))

        instance_per_task = cursor.execute("select COUNT(*) from batch_instance where task_id > 0 group by task_id")
        instance = cursor.fetchall()
        data = list(instance)
        print('max instance number per task:' + str(max(data)))
        print('min instance number per task:' + str(min(data)))
        print('avg instance number per task:' + str(np.average(data)))

        task_per_job = cursor.execute("select COUNT(*) from batch_task group by job_id")
        task = cursor.fetchall()
        print('max task per job:' + str(max(task)))
        print('min task per job:' + str(min(task)))
        print('avg task per job:' + str(np.average(task)))

        instance_duration = cursor.execute("select end_timestamp-start_timestamp as instance_duration from batch_instance WHERE (start_timestamp != 0 or end_timestamp != 0) and (end_timestamp-start_timestamp > 0)")
        list_instance_duration = cursor.fetchall()
        # print(type(list_instance_duration))
        # instance_duration_handled = []
        # for i in range(len(list_instance_duration)):
        #     if list_instance_duration[i] >= 0:
        #         instance_duration_handled.append(list_instance_duration[i])
        print('max instance duration:' + str(max(list_instance_duration)))
        print('min instance duration:' + str(min(list_instance_duration)))
        print('avg instance duration:' + str(np.average(list_instance_duration)))

        # task_duration = cursor.execute("select max(end_timestamp)-min(start_timestamp) from batch_instance WHERE  start_timestamp != 0 and end_timestamp != 0 group by task_id ")
        task_duration = cursor.execute("select modify_timestamp-create_timestamp from batch_task WHERE (create_timestamp != 0 or "
                                       "modify_timestamp != 0) and status = 'Terminated' group by task_id ")
        task_duration = list(cursor.fetchall())
        print('max task duration:' + str(max(task_duration)))
        print('min task duration:' + str(min(task_duration)))
        print('avg task duration:' + str(np.average(task_duration)))

        capacity_server = cursor.execute("select cpu,memory,disk from server_event group by machineID")
        capacity_server = cursor.fetchall()
        capacity_server = np.array(list(capacity_server))

        util_server = cursor.execute("select cpu,memory,disk from server_usage group by machineID")
        util_server = cursor.fetchall()
        util_server = np.array(list(util_server))

        timestamp = cursor.execute("select time_stamp from server_event order by time_stamp")
        timestamp = list(cursor.fetchall())
        timestamp = np.array(timestamp)
        print(timestamp)
        # timestamp = [(x/3600 - 11) for x in timestamp]

        # 如果没有设置自动提交事务，则这里需要手动提交一次
        conn.commit()

        # 绘图
        # fig = plt.figure()
        # ax1 = fig.add_subplot(111)
        # ax1.set_xlabel("time(s)")
        # ax1.set_ylabel("cpu")
        # ax1.plot(timestamp, capacity_server[:,0],label='cpu capacity')
        # ax1.plot(timestamp, util_server[:,0],label='cpu utilization')
        # fig.savefig("../imgs_mysql/cpu.png")
        # fig.show()
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
    CountAnalysis()