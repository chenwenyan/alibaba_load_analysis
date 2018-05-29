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
        cursor.execute("SELECT end_timestamp - start_timestamp FROM batch_instance WHERE status = 'Terminated' and real_mem_avg != 0")
        instance_duration = cursor.fetchall()
        list_instance_duration = list(instance_duration)
        arr_instance_duration = [x[0] for x in list_instance_duration]
        print(arr_instance_duration)

        cursor.execute("select machineID, real_cpu_avg, real_mem_avg from batch_instance where status = 'Terminated' and real_mem_avg != 0")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)

        machineID_arr = [x[0] for x in res]
        print(machineID_arr)
        cpu_arr = [x[1] for x in res]
        mem_arr = [x[2] for x in res]

        # 如果没有设置自动提交事务，则这里需要手动提交一次
        conn.commit()

        # k-means聚类
        # 设置类别为3
        clf = KMeans(n_clusters=3)
        # 将数据带入到聚类模型中
        loan = []
        loan.append(arr_instance_duration)
        loan.append(cpu_arr)
        loan.append(mem_arr)
        loan = np.asarray(loan)
        loan = loan.transpose()

        K = range(1, 10)
        meandistortions = []
        for k in K:
            kmeans = KMeans(n_clusters=k)
            kmeans.fit(loan)
            # meandistortions.append(sum(np.min(cdist(X, kmeans.cluster_centers_, 'euclidean'), axis=1)) / X.shape[0])
            meandistortions.append(kmeans.inertia_)

        # 绘图
        plt.figure()
        plt.plot(K, meandistortions, 'o-')
        plt.axvline(4, ls="--", color="r")
        plt.xlabel('number of clusters')
        plt.ylabel('the average degree of distortion')
        plt.title('Use the elbow rule to determine the best K value')
        plt.savefig('../imgs_mysql/find_k_instance.png')
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