import MySQLdb as mdb
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_samples, silhouette_score


# 自定义归一化方法
def MaxMinNormalization(list):
    Max = np.max(list)
    Min = np.min(list)
    res = []
    for x in list:
        x = (x - Min) / (Max - Min)
        res.append(x)
    return res


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
        # cursor.execute(
        #     "SELECT max(modify_timestamp) - min(create_timestamp) FROM batch_task WHERE status = 'Terminated'  group by job_id ASC ")
        # results = cursor.fetchall()
        # result_list = list(results)
        # job_duration = [x[0] for x in result_list]
        # cursor.execute("select avg_cpu, avg_mem from batch_job_category where avg_cpu > 0 and avg_mem > 0")
        # results = cursor.fetchall()
        # result_list = list(results)
        # avg_cpu = [x[0] for x in result_list]
        # avg_mem = [x[1] for x in result_list]

        cursor.execute(
            "select instance_id, avg(cpu_util), avg(mem_util), avg(disk_util) from container_usage where mem_util > 0 and cpu_util > 0 and disk_util > 0 group by instance_id"
        )
        results = cursor.fetchall()
        result_list = list(results)
        ts = [x[0] for x in result_list]
        avg_cpu = [x[1] for x in result_list]
        avg_mem = [x[2] for x in result_list]
        avg_disk = [x[3] for x in result_list]

        # 如果没有设置自动提交事务，则这里需要手动提交一次
        conn.commit()

        # 将数据带入到聚类模型中
        loan = []
        loan.append(avg_cpu)
        loan = np.asarray(loan)
        loan = loan.transpose()

        n_cluster = range(2,11)
        sample_silhouette_job_duration_list = []
        for cluster in n_cluster:
            clf = KMeans(n_clusters=cluster)
            cluster_labels = clf.fit_predict(loan)
            silhouette_avg = silhouette_score(loan, cluster_labels)
            print("For n_clusters =", cluster,
                  "The average silhouette_score is :", silhouette_avg)
            sample_silhouette_values = silhouette_samples(loan, cluster_labels)
            print(sample_silhouette_values)
            sample_silhouette_job_duration_list.append(silhouette_avg)

        loan = []
        loan.append(avg_mem)
        loan = np.asarray(loan)
        loan = loan.transpose()
        sample_silhouette_avg_cpu_list = []
        for cluster in n_cluster:
            clf = KMeans(n_clusters=cluster)
            cluster_labels = clf.fit_predict(loan)
            silhouette_avg = silhouette_score(loan, cluster_labels)
            print("For n_clusters =", cluster,
                  "The average silhouette_score is :", silhouette_avg)
            sample_silhouette_values = silhouette_samples(loan, cluster_labels)
            print(sample_silhouette_values)
            sample_silhouette_avg_cpu_list.append(silhouette_avg)

        loan = []
        loan.append(avg_disk)
        loan = np.asarray(loan)
        loan = loan.transpose()
        sample_silhouette_avg_mem_list = []
        for cluster in n_cluster:
            clf = KMeans(n_clusters=cluster)
            cluster_labels = clf.fit_predict(loan)
            silhouette_avg = silhouette_score(loan, cluster_labels)
            print("For n_clusters =", cluster,
                  "The average silhouette_score is :", silhouette_avg)
            sample_silhouette_values = silhouette_samples(loan, cluster_labels)
            print(sample_silhouette_values)
            sample_silhouette_avg_mem_list.append(silhouette_avg)

        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        # ax1.plot(range(2,11), sample_silhouette_job_duration_list, '.-', label='job duration')
        # ax1.plot(range(2,11), sample_silhouette_avg_cpu_list, '.-', label='average cpu')
        # ax1.plot(range(2,11), sample_silhouette_avg_mem_list, '.-', label='average memory')
        # ax1.set_xlabel("cluster k")
        # ax1.set_ylabel("average silhouette score")
        # ax1.legend(loc='best')
        # plt.savefig("../imgs_mysql/batch_job_category_average_silhouette")

        ax1.plot(range(2,11), sample_silhouette_job_duration_list, '.-', label='average cpu')
        ax1.plot(range(2,11), sample_silhouette_avg_cpu_list, '.-', label='average memory')
        ax1.plot(range(2,11), sample_silhouette_avg_mem_list, '.-', label='average disk')
        ax1.set_xlabel("cluster k")
        ax1.set_ylabel("average silhouette score")
        ax1.legend(loc='best')
        plt.savefig("../imgs_mysql/container_instance_category_average_silhouette")
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
