from sklearn.cluster import KMeans
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D


# 自定义归一化方法
def MaxMinNormalization(list, Max, Min):
    res = []
    for x in list:
        x = (x - Min) / (Max - Min)
        res.append(x)
    return res

def graph_data():
    # 读取用于聚类的数据，并创建数据表
    data = pd.read_csv('../dataset/batch_task.csv', header=None, iterator=True)
    # loan_data = pd.DataFrame(data, columns=('start_timestamp', 'end_timestamp', 'job_id', 'task_id', 'machineID',
    #                                         'seq_no', 'total_seq_no', 'real_cpu_max', 'real_cpu_avg', 'real_mem_max',
    #                                         'real_mem_avg'))

    loop = True
    chunkSize = 10000
    chunks = []
    while loop:
        try:
            chunk = data.get_chunk(chunkSize)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped.")
    df = pd.concat(chunks, ignore_index=True)
    print(df.count())
    # 填充缺失值
    # df = df.fillna(0)

    jobs = df.iloc[:, 2].values
    jobs = np.unique(jobs)
    task_num, plan_cpu_num, plan_mem_num = [], [], []
    for i in jobs:
        job = df[df.iloc[:, 2] == i]
        tasks = len(job)
        a = job.iloc[:, 4].values
        b = job.iloc[:, 6].values
        c = job.iloc[:, 6].values
        a = [0 if np.isnan(x) else x for x in a]
        b = [0 if np.isnan(x) else x for x in b]
        c = [0 if np.isnan(x) else x for x in c]
        plan_cpu_total = sum(np.array(a) * np.array(b))
        plan_mem_total = sum(np.array(a) * np.array(c))
        task_num.append(tasks)
        plan_cpu_num.append(plan_cpu_total / tasks)
        plan_mem_num.append(plan_mem_total / tasks)

    # 归一化三个特征向量
    plan_cpu_num = MaxMinNormalization(plan_cpu_num, np.max(plan_cpu_num), np.min(plan_cpu_num))
    plan_mem_num = MaxMinNormalization(plan_mem_num, np.max(plan_mem_num), np.min(plan_mem_num))
    task_num = MaxMinNormalization(task_num, np.max(task_num), np.min(task_num))

    # 设置类别为3
    clf = KMeans(n_clusters=2)
    # 将数据代入到聚类模型中
    loan = []
    loan.append(task_num)
    loan.append(plan_cpu_num)
    loan.append(plan_mem_num)
    loan = np.asarray(loan)
    loan = loan.transpose()

    clf = clf.fit(loan)
    # 在原始数据表中增加聚类结果标签
    loan = np.insert(loan, 3, values=clf.labels_, axis=1)
    # np.r_(loan, clf.labels_)
    # loan['label'] = clf.labels_

    fig = plt.figure()
    ax = Axes3D(fig)
    # ax = fig.add_subplot(111, projection='3d')
    loan_df = pd.DataFrame(loan)
    class_1 = loan_df[loan_df.iloc[:, 3] == 0]
    x1 = class_1.iloc[:, 0].values
    y1 = class_1.iloc[:, 1].values
    z1 = class_1.iloc[:, 2].values
    class_2 = loan_df[loan_df.iloc[:, 3] == 1]
    x2 = class_2.iloc[:, 0].values
    y2 = class_2.iloc[:, 1].values
    z2 = class_2.iloc[:, 2].values
    # class_3 = loan_df[loan_df.iloc[:, 3] == 2]
    # x3 = class_3.iloc[:, 0].values
    # y3 = class_3.iloc[:, 1].values
    # z3 = class_3.iloc[:, 2].values
    ax.scatter(x1, y1, z1, color='red', s=1)
    ax.scatter(x2, y2, z2, color='blue', s= 1)
    # ax.scatter(x3, y3, z3, color='yellow')

    ax.set_xlabel('task num(per job)')
    ax.set_ylabel('total plan cpu(per job)')
    ax.set_zlabel('total plan memory(per job)')
    # plt.title('Use the elbow rule to determine the best K value')
    plt.savefig('../images/knn_job.png')
    plt.show()


if __name__ == '__main__':
    graph_data()
