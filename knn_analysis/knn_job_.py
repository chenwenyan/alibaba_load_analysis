from sklearn.cluster import KMeans
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

def graph3D():
    # 读取数据
    # batch_task = pd.read_csv('../dataset/batch_task.csv', header=None, usecols=10000)
    # batch_instance = pd.read_csv('../dataset/batch_instance.csv', header=None, usecols=10000)
    # task_df = pd.DataFrame(batch_task)
    # instance_df = pd.DataFrame(batch_instance)

    # 超大文件时的处理
    batch_task = pd.read_csv('../dataset/batch_task.csv', header=None, iterator=True)
    batch_instance = pd.read_csv('../dataset/batch_instance.csv', header=None, iterator=True)

    chunkSize = 10000
    # batch_task 过大时迭代读取数据
    task_loop = True
    chunks = []
    while task_loop:
        try:
            chunk = batch_task.get_chunk(chunkSize)
            chunks.append(chunk)
        except StopIteration:
            task_loop = False
            print('batch_task Iteration is stopped')
    task_df = pd.concat(chunks, ignore_index=True)
    print(task_df.count())
    print(task_df.tail(5))
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
    print(instance_df.count())
    print(instance_df.tail())
    # instance_df = pd.DataFrame(instance_df, columns=('start_timestamp','end_timestamp','job_id','task_id','machineID', 'status',
    #                                                  'seq_no','total_seq_no','real_cpu_max','real_cpu_avg','real_mem_max', 'real_mem_avg'))

    # 获取所有job
    job = task_df[task_df.iloc[:, 5] == 'Terminated']
    job = job.iloc[:, 2].values
    job = np.unique(job)

    # 获取所有正常结束的job
    print(type(task_df))
    # task_df = np.array(task_df)
    terminal_job = task_df[task_df.iloc[:, 5] == 'Terminated']
    terminal_job = np.unique(terminal_job.iloc[:, 3].values)

    duration_of_job_array, cpu_of_job_array, mem_of_job_array = [],[],[]
    for i in terminal_job:
        # 获取job_id == i的所有行（即所有task）
        job_obj = task_df[task_df.iloc[:, 2] == i]
        job_obj = job_obj[task_df.iloc[:, 5] == 'Terminated']
        print(len(job_obj))

        # job执行时间（最晚修改时间-最早开始时间?）
        if len(job_obj) > 0:
            min_creat_time = np.min(job_obj.iloc[:, 0].values)
            max_modify_time = np.max(job_obj.iloc[:, 1].values)
            duration_of_job = max_modify_time - min_creat_time

        # cpu && memory
        task = instance_df[instance_df.iloc[:, 2] == i]
        task = task.iloc[0:, 3].values
        task = np.unique(task)
        sumCPU, sumMem = [],[]
        for j in task:
            instance = instance_df[instance_df.iloc[:, 3] == j]
            cpu = instance.iloc[:, 9].values
            sum_cpu = np.sum(cpu)/len(instance)
            sumCPU.append(sum_cpu)

            memory = instance.iloc[:, 11].values
            sum_memory = np.sum(memory)/len(instance)
            sumMem.append(sum_memory)

        cpu_of_job = np.sum(sumCPU)
        mem_of_job = np.sum(sumMem)

        duration_of_job_array.append(duration_of_job)
        cpu_of_job_array.append(cpu_of_job)
        mem_of_job_array.append(mem_of_job)

    # 去除Nan值
    duration_of_job_array[np.isnan(duration_of_job_array)] = 0
    cpu_of_job_array[np.isnan(cpu_of_job_array)] = 0
    mem_of_job_array[np.isnan(mem_of_job_array)] = 0

    # k-means聚类
    # 设置类别为3
    clf = KMeans(n_clusters=3)
    # 将数据带入到聚类模型中
    loan = []
    loan.append(duration_of_job_array)
    loan.append(cpu_of_job_array)
    loan.append(mem_of_job_array)
    loan = np.asarray(loan)
    loan = loan.transpose()

    # loan = np.vstack(duration_of_job_array,cpu_of_job_array,mem_of_job_array)

    clf = clf.fit(loan)
    loan.insert(loan, 1, values = clf.labels_, axis=1)

    fig = plt.figure()
    ax = Axes3D(fig)
    loan_df = pd.DataFrame(loan)
    class_1 = loan_df[loan_df.iloc[:, 3] == 0]
    x1 = class_1.iloc[:, 0].values
    y1 = class_1.iloc[:, 1].values
    z1 = class_1.iloc[:, 2].values
    class_2 = loan_df[loan_df.iloc[:, 3] == 1]
    x2 = class_2.iloc[:, 0].values
    y2 = class_2.iloc[:, 1].values
    z2 = class_2.iloc[:, 2].values
    class_3 = loan_df[loan_df.iloc[:, 3] == 2]
    x3 = class_3.iloc[:, 0].values
    y3 = class_3.iloc[:, 1].values
    z3 = class_3.iloc[:, 2].values
    ax.scatter(x1, y1, z1, color='red')
    ax.scatter(x2, y2, z2, color='blue')
    ax.scatter(x3, y3, z3, color='yellow')

    ax.set_xlabel('task num(per job)')
    ax.set_ylabel('total plan cpu(per job)')
    ax.set_zlabel('total plan memory(per job)')
    plt.savefig('../images/knn_job_.png')
    plt.show()

if __name__ == '__main__':
    graph3D()