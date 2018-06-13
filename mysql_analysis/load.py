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
        cursor.execute("select avg_cpu, avg_mem from batch_job_category where avg_cpu > 0 and avg_mem > 0")
        results = cursor.fetchall()
        result_list = list(results)
        avg_cpu = [x[0] for x in result_list]
        avg_mem = [x[1] for x in result_list]
        # cursor.execute(
        #     "select instance_id, avg(cpu_util), avg(mem_util), avg(disk_util) from container_usage where mem_util > 0 and cpu_util > 0 and disk_util > 0 group by instance_id"
        # )
        # results = cursor.fetchall()
        # result_list = list(results)
        # ts = [x[0] for x in result_list]
        # avg_cpu = [x[1] for x in result_list]
        # avg_mem = [x[2] for x in result_list]
        # avg_disk = [x[3] for x in result_list]

        # 如果没有设置自动提交事务，则这里需要手动提交一次
        conn.commit()

        # 将数据带入到聚类模型中
        loan = []
        # loan.append(avg_cpu)
        # loan.append(avg_mem)
        # loan.append(avg_disk)
        loan.append(avg_mem)
        loan = np.asarray(loan)
        loan = loan.transpose()
        # k-means聚类
        # clf = KMeans(n_clusters=3)
        # cluster_labels = clf.fit_predict(loan)
        # loan_res = np.insert(loan, 1, values=clf.labels_, axis=1)
        # class_1,class_2,class_3 = [],[],[]
        # for i in loan_res:
        #     if i[1] == 0:
        #         class_1.append(i[0])
        #     elif i[1] == 1:
        #         class_2.append(i[0])
        #     else:
        #         class_3.append(i[0])
        # print(max(class_1))
        # print(min(class_1))
        # print(max(class_2))
        # print(min(class_2))
        # print(max(class_3))
        # print(min(class_3))
        # 设置类别为3
        n_cluster = range(2,11)
        sample_silhouette_avg_list = []
        for cluster in n_cluster:
            clf = KMeans(n_clusters=cluster)
            cluster_labels = clf.fit_predict(loan)
            silhouette_avg = silhouette_score(loan, cluster_labels)
            print("For n_clusters =", cluster,
                  "The average silhouette_score is :", silhouette_avg)
            sample_silhouette_values = silhouette_samples(loan, cluster_labels)
            print(sample_silhouette_values)
            sample_silhouette_avg_list.append(silhouette_avg)



            # fig = plt.figure()
            # ax1 = fig.add_subplot(111)
            # y_lower = 10
            # for i in range(cluster):
            #     # Aggregate the silhouette scores for samples belonging to
            #     # cluster i, and sort them
            #     ith_cluster_silhouette_values = \
            #         sample_silhouette_values[cluster_labels == i]
            #
            #     ith_cluster_silhouette_values.sort()
            #
            #     size_cluster_i = ith_cluster_silhouette_values.shape[0]
            #     y_upper = y_lower + size_cluster_i
            #
            #     # color = cm.spectral(float(i) / n_clusters)
            #     ax1.fill_betweenx(np.arange(y_lower, y_upper),
            #                       0, ith_cluster_silhouette_values,
            #                       facecolor='green', edgecolor='green', alpha=0.7)
            #
            #     # Label the silhouette plots with their cluster numbers at the middle
            #     ax1.text(-0.05, y_lower + 0.5 * size_cluster_i, str(i))
            #
            #     # Compute the new y_lower for next plot
            #     y_lower = y_upper + 10  # 10 for the 0 samples

            # ax1.set_title("The silhouette plot for the various clusters.")
            # ax1.set_xlabel("The silhouette coefficient values")
            # ax1.set_ylabel("Cluster label")
            # ax1.axvline(x=silhouette_avg, color="red", linestyle="--")
            # ax1.set_yticks([])  # Clear the yaxis labels / ticks
            # ax1.set_xticks([-0.2, -0.1, 0, 0.2, 0.4, 0.6, 0.8, 1])
            # plt.savefig("../imgs_mysql/batch_job_category_silhoutte_value_" + str(cluster))
            # plt.show()
        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        ax1.plot(range(2,11), sample_silhouette_avg_list, 'cx-')
        ax1.set_xlabel("cluster k")
        ax1.set_ylabel("average silhouette score")
        plt.savefig("../imgs_mysql/batch_job_category_average_silhouette")
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
