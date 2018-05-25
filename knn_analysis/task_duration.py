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
    batch_instance = pd.read_csv('../dataset/batch_instance.csv', header=None, iterator=True)
    # task_df = pd.DataFrame(task_df,columns=('create_timestamp','modify_timestamp','job_id','task_id','instance_num','status',
    #                                                                             'plan_cpu','plan_mem'))

    # batch_instance 过大时迭代读取数据
    instance_loop = True
    instance_chunks = []
    while instance_loop:
        try:
            chunk = batch_instance.get_chunk(chunkSize)
            instance_chunks.append(chunk)
        except StopIteration:
            instance_loop = False
            print('batch_instance Iteration is stopped')
    instance_df = pd.concat(instance_chunks, ignore_index=True)
    # instance_df = pd.DataFrame(instance_df, columns=('start_timestamp','end_timestamp','job_id','task_id','machineID', 'status',
    #                                                  'seq_no','total_seq_no','real_cpu_max','real_cpu_avg','real_mem_max', 'real_mem_avg'))

    # 获取所有job
    job = instance_df.iloc[:, 2].values
    job = np.unique(job)


    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.set_xlabel('task num(per job)')
    ax.set_ylabel('total plan cpu(per job)')
    ax.set_zlabel('total plan memory(per job)')
    plt.title('clustering jobs with k-means algorithm')
    plt.savefig('../images/task_duration.png')
    plt.show()

if __name__ == '__main__':
    graph_data()
