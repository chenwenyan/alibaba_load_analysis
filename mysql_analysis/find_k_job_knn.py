import MySQLdb as mdb
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt

def graph():
    # 连接数据库
    conn = mdb.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='alibaba_trace', charset='utf8')

    # 如果使用事务引擎，可以设置自动提交事务，或者在每次操作完成后手动提交事务conn.commit()
    conn.autocommit(1)  # conn.autocommit(True)

    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 因该模块底层其实是调用CAPI的，所以，需要先得到当前指向数据库的指针。

    try:
        # 查询数据条目
        job_duration = cursor.execute("SELECT max(end_timestamp) - min(start_timestamp) FROM batch_instance WHERE status = 'Terminated' group by job_id")
        job_duration = cursor.fetchall()
        list_job_duration = list(job_duration)
        arr_job_duration = [x[0] for x in list_job_duration]

        cursor.execute("select t.job_id, avg(t.avg_cpu), avg(t.avg_mem) from (select job_id, task_id, avg(real_cpu_avg) as avg_cpu, avg(real_mem_avg) as avg_mem from batch_instance where status='Terminated' group by task_id )t group by job_id ")
        # cursor.execute("select t.job_id, avg(t.avg_cpu), avg(t.avg_mem) from (select job_id, task_id, avg(cpu) as avg_cpu, avg(mem) as avg_mem from test  group by task_id )t group by job_id ")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)

        job_arr = [x[0] for x in res]
        cpu_arr = [x[1] for x in res]
        mem_arr = [x[2] for x in res]

        # 如果没有设置自动提交事务，则这里需要手动提交一次
        conn.commit()

        # 将数据带入到聚类模型中
        loan = []
        loan.append(arr_job_duration)
        loan.append(cpu_arr)
        loan.append(mem_arr)
        loan = np.asarray(loan)
        loan = loan.transpose()

        K = range(1, 10)
        meandistortions = []
        for k in K:
            kmeans = KMeans(n_clusters=k)
            kmeans.fit(loan)
            # kmeans.fit(tasknum,plancpunum,planmemnum)
            # meandistortions.append(sum(np.min(cdist(X, kmeans.cluster_centers_, 'euclidean'), axis=1)) / X.shape[0])
            meandistortions.append(kmeans.inertia_)

        # 绘图
        plt.figure()
        plt.plot(K, meandistortions, 'o-')
        plt.axvline(3, ls="--", color="r")
        plt.xlabel('number of clusters')
        plt.ylabel('the average degree of distortion')
        plt.title('Use the elbow rule to determine the best K value')
        plt.savefig('../images/find_k_job.png')
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