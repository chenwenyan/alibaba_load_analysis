import matplotlib.pyplot as plt
import MySQLdb as mdb
import numpy as np
from sklearn.cluster import KMeans


def graph():
    # 连接数据库
    conn = mdb.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='alibaba_trace', charset='utf8')

    # 如果使用事务引擎，可以设置自动提交事务，或者在每次操作完成后手动提交事务conn.commit()
    conn.autocommit(1)  # conn.autocommit(True)

    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 因该模块底层其实是调用CAPI的，所以，需要先得到当前指向数据库的指针。

    try:
        cursor.execute("select job_id, max(modify_timestamp)-min(create_timestamp) from batch_task where status = 'Terminated' group by job_id  ")
        records = cursor.fetchall()
        result = list(records)
        print(result)
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
    res[:] = map(list, result)
    ids = [x[0] for x in res]
    job_duration = [x[1]/3600 for x in res]
    print(max(job_duration))
    print(min(job_duration))
    ids = np.asarray(ids)
    job_duration = np.asarray(job_duration)

    data = []
    data.append(ids)
    data.append(job_duration)
    data = np.asarray(data)
    data = data.transpose()

    K = range(1, 10)
    meandistortions = []
    for k in K:
        kmeans = KMeans(n_clusters=k)
        kmeans.fit(data)
        # kmeans.fit(tasknum,plancpunum,planmemnum)
        # meandistortions.append(sum(np.min(cdist(X, kmeans.cluster_centers_, 'euclidean'), axis=1)) / X.shape[0])
        meandistortions.append(kmeans.inertia_)

    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    ax1.plot(K, meandistortions, 'o-')
    ax1.axvline(3, ls="--", color="r")
    ax1.set_xlabel('number of clusters')
    ax1.set_ylabel('the average degree of distortion')
    ax1.set_title('Use the elbow rule to determine the best K value')

    plt.savefig('../../imgs_mysql/k-means_job_duration.png')
    plt.show()

if __name__ == '__main__':
    graph()
