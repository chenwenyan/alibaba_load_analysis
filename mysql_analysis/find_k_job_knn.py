import MySQLdb as mdb
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

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
        job_duration = cursor.execute('SELECT modify_timestamp - create_timestamp FROM batch_task ')
        job_duration = cursor.fetchall()
        for i in range(len(job_duration)):
            if job_duration[i] < 0 :
                print(job_duration[i])
        print('max job duration:' + str(max(job_duration)))
        print('min job duration:' + str(min(job_duration)))
        print('avg job duration:' + str(np.average(job_duration)))
        # duration_per_job = np.array(job_duration)
        duration_per_job = list(job_duration)

        instances_per_task = cursor.execute("SELECT count(*) from batch_instance group by job_id")
        instances_per_task = list(cursor.fetchall())
        cpu_avg_per_job = cursor.execute("select sum(real_cpu_avg) from batch_instance group by job_id")
        cpu_avg_per_job = list(cursor.fetchall())
        mem_avg_per_job = cursor.execute("select sum(real_mem_avg) from batch_instance group by job_id")
        mem_avg_per_job = list(cursor.fetchall())

        # 如果没有设置自动提交事务，则这里需要手动提交一次
        conn.commit()

        # 将数据带入到聚类模型中
        loan = []
        # duration_per_job =  np.append(duration_per_job, 0)
        cpu_avg_per_job.pop()
        mem_avg_per_job.pop()
        # duration_per_job = pd.DataFrame(duration_per_job)
        # cpu_avg_per_job = pd.DataFrame(cpu_avg_per_job)
        # mem_avg_per_job = pd.DataFrame(mem_avg_per_job)
        # data = pd.concat(duration_per_job,cpu_avg_per_job,mem_avg_per_job)
        loan.append(duration_per_job)
        loan.append(cpu_avg_per_job)
        loan.append(mem_avg_per_job)
        loan = np.array(loan)
        loan = loan.transpose()
        # loan = np.vstack(duration_of_job_array,cpu_of_job_array,mem_of_job_array)

        K = range(1, 10)
        meandistortions = []
        # for k in K:
        #     kmeans = KMeans(n_clusters=k)
        #     kmeans.fit(data)
            # meandistortions.append(sum(np.min(cdist(X, kmeans.cluster_centers_, 'euclidean'), axis=1)) / X.shape[0])
            # meandistortions.append(kmeans.inertia_)
        # plt.plot(K, meandistortions, 'o-')
        # plt.axvline(3, ls="--", color="r")
        # plt.xlabel('number of clusters')
        # plt.ylabel('the average degree of distortion')
        # plt.title('Use the elbow rule to determine the best K value')
        # plt.savefig('../imgs_mysql/find_k_job.png')
        duration_per_job = sorted(duration_per_job)
        duration = np.unique(duration_per_job)
        jobs = []
        for i in duration_per_job:
            job_num = duration_per_job.count(i)
            jobs.append(job_num)
        plt.plot(duration_per_job,jobs)
        plt.xlim(0,1000)
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