import MySQLdb as mdb
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from mpl_toolkits.mplot3d import Axes3D

# 自定义归一化方法
def MaxMinNormalization(list, Max, Min):
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
        job_duration = cursor.execute("SELECT max(end_timestamp) - min(start_timestamp) FROM batch_instance WHERE status = 'Terminated'  and real_mem_avg != 0  group by job_id ASC ")
        job_duration = cursor.fetchall()
        list_job_duration = list(job_duration)
        arr_job_duration = [x[0] for x in list_job_duration]
        print(arr_job_duration)
        arr_job_duration_norm = MaxMinNormalization(arr_job_duration,max(arr_job_duration),min(arr_job_duration))

        cursor.execute("select t.job_id, avg(t.avg_cpu), avg(t.avg_mem) from (select job_id, task_id, avg(real_cpu_avg) as avg_cpu, avg(real_mem_avg) as avg_mem from batch_instance where status='Terminated' and real_mem_avg != 0 group by task_id )t group by job_id ASC")
        # cursor.execute("select t.job_id, avg(t.avg_cpu), avg(t.avg_mem) from (select job_id, task_id, avg(cpu) as avg_cpu, avg(mem) as avg_mem from test  group by task_id )t group by job_id ")
        records = cursor.fetchall()
        list_records = list(records)
        res = []
        res[:] = map(list, list_records)

        job_arr = [x[0] for x in res]
        cpu_arr = [x[1] for x in res]
        print(cpu_arr)
        cpu_arr_norm = MaxMinNormalization(cpu_arr, max(cpu_arr), min(cpu_arr))
        mem_arr = [x[2] for x in res]
        print(mem_arr)
        mem_arr_norm = MaxMinNormalization(mem_arr, max(mem_arr), min(cpu_arr))

        # 如果没有设置自动提交事务，则这里需要手动提交一次
        conn.commit()

        # k-means聚类
        # 设置类别为3
        clf = KMeans(n_clusters=3)
        # 将数据带入到聚类模型中
        loan = []
        loan.append(arr_job_duration_norm)
        loan.append(cpu_arr_norm)
        loan.append(mem_arr_norm)
        loan = np.asarray(loan)
        loan = loan.transpose()

        clf = clf.fit(loan)
        loan = np.insert(loan, 3, values = clf.labels_, axis=1)

        fig = plt.figure()
        ax = Axes3D(fig)
        loan_df = pd.DataFrame(loan)
        class_1 = loan_df[loan_df.iloc[:, 3] == 0]
        print('class_1:')
        print(class_1)
        x1 = class_1.iloc[:, 0].values
        y1 = class_1.iloc[:, 1].values
        z1 = class_1.iloc[:, 2].values
        class_2 = loan_df[loan_df.iloc[:, 3] == 1]
        x2 = class_2.iloc[:, 0].values
        y2 = class_2.iloc[:, 1].values
        z2 = class_2.iloc[:, 2].values
        print("class_2:")
        print(class_2)
        class_3 = loan_df[loan_df.iloc[:, 3] == 2]
        x3 = class_3.iloc[:, 0].values
        y3 = class_3.iloc[:, 1].values
        z3 = class_3.iloc[:, 2].values
        print("class_3")
        print(class_3)
        ax.scatter(x1, y1, z1, color='red', s = 1)
        ax.scatter(x2, y2, z2, color='blue', s = 1)
        ax.scatter(x3, y3, z3, color='yellow', s= 1)

        ax.set_xlabel('job duration')
        ax.set_ylabel('average cpu(per job)')
        ax.set_zlabel('average memory(per job)')
        ax.set_xlim(0,1)
        ax.view_init(elev=20., azim=-35)
        ax.grid(False)
        plt.savefig('../imgs_mysql/knn_job.png')
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