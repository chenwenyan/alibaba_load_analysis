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

    plan_result, used_result = [], []
    try:
        cursor.execute("select task_id, plan_cpu, plan_mem from batch_task")
        records = cursor.fetchall()
        plan_result = list(records)
        print(plan_result)
        cursor.execute("select task_id, avg(real_cpu_avg), avg(real_mem_avg) from batch_instance where status ='Terminated'and real_mem_avg != 0 group by task_id")
        used_res = cursor.fetchall()
        used_result = list(used_res)
        print(used_result)
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
    res[:] = map(list, plan_result)
    jobs = [x[0] for x in res]
    avg_cpu = [x[2] for x in res]
    unique_avg_cpu = sorted(np.unique(avg_cpu))
    num_of_job_cpu = []
    for i in unique_avg_cpu:
        sum_job = avg_cpu.count(i)
        num_of_job_cpu.append(sum_job)
    print(max(num_of_job_cpu))
    print(min(num_of_job_cpu))

    res_used = []
    res_used[:] = map(list, used_result)
    used_jobs = [x[0] for x in used_result]
    avg_used_cpu = [x[2] for x in used_result]
    unique_avg_used_cpu = sorted(np.unique(avg_used_cpu))
    num_of_job_used_cpu = []
    for i in unique_avg_used_cpu:
        sum_job = avg_used_cpu.count(i)
        num_of_job_used_cpu.append(sum_job)
    print(max(num_of_job_used_cpu))
    print(min(num_of_job_used_cpu))

    fig = plt.figure(figsize=(10, 5))
    ax1 = fig.add_subplot(1, 2, 1)
    ax2 = fig.add_subplot(1, 2, 2)
    ax1.set_ylabel('number of tasks')
    ax1.set_xlabel('size of normalized memory request (average per instance)')
    ax2.set_ylabel('number of tasks')
    ax2.set_xlabel('size of normalized memory used (average per instance)')

    unique_avg_cpu = [float(x) for x in unique_avg_cpu]
    # ax1.set_xscale('log')
    ax2.set_xscale('log')
    # ax1.set_xlim(0.00,0.08)
    ax1.plot(unique_avg_cpu, num_of_job_cpu,width=2, color='green')
    ax2.plot(unique_avg_used_cpu, num_of_job_used_cpu, color='green')

    plt.savefig('../imgs_mysql/figure_5.png')
    plt.show()

if __name__ == '__main__':
    graph()
