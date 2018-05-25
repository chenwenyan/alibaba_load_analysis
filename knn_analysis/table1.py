from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def statistics_data():
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
    job = task_df.iloc[:, 2].values
    job = np.unique(job)
    print("total job:" + str(len(job)))

    task = task_df.iloc[:, 3].values
    task = np.unique(task)
    print('total task:' + str(len(task)))

    instance = instance_df[instance_df.iloc[:, 0] > 0]
    instance = instance.iloc[:, 0].values
    print('total instance:' + str(len(instance)))

    failed_tasks_arr, terminated_task_arr, waiting_task_arr, cancelling_task_arr = [], [], [], []
    instance_of_job_arr, instance_num_of_task_arr = [], []
    task_of_job_arr = []
    instance_sum = 0
    for i in job:
        task_of_job = task_df[task_df.iloc[:, 2] == i]
        task_num_of_job = len(task_of_job.iloc[:, 0].values)
        task_of_job_arr.append(task_num_of_job)
        failed_task_of_job = task_of_job[task_of_job.iloc[:, 5] == 'Failed']
        terminated_task_of_job = task_of_job[task_of_job.iloc[:, 5] == 'Terminated']
        waiting_task_of_job = task_of_job[task_of_job.iloc[:, 5] == 'Waiting']
        cancelling_task_of_job = task_of_job[task_of_job.iloc[:, 5] == 'Cancelled']
        failed_tasks_arr.append(len(failed_task_of_job))
        terminated_task_arr.append(len(terminated_task_of_job))
        waiting_task_arr.append(len(waiting_task_of_job))
        cancelling_task_arr.append(len(cancelling_task_of_job))
        instance_sum = sum(task_df.iloc[:, 4].values)

    failed_task_sum = sum(failed_tasks_arr)
    terminated_task_num = sum(terminated_task_arr)
    waiting_task_sum = sum(waiting_task_arr)
    cancelling_task_sum = sum(cancelling_task_arr)
    instance_of_job_sum = sum(instance_of_job_arr)
    avg_task_num_of_job = sum(task_of_job_arr) / len(job)
    max_task_num_of_job = max(task_of_job_arr)
    min_task_num_of_job = min(task_of_job_arr)

    print('failed_task_sum:' + str(failed_task_sum))
    print('terminated_task_num:' + str(terminated_task_num))
    print('waiting_task_sum:' + str(waiting_task_sum))
    print('cancelling_task_sum:' + str(cancelling_task_sum))
    print('instance_of_job_sum:' + str(instance_of_job_sum))
    print('avg_task_num_of_job:' + str(avg_task_num_of_job))
    print('max_task_num_of_job:' + str(max_task_num_of_job))
    print('min_task_num_of_job:' + str(min_task_num_of_job))
    print('instance_sum:' + str(instance_sum))

    instance_arr = []
    for j in task:
        task_total = task_df[task_df.iloc[:, 3] == j]
        instance = task_total.iloc[:, 4].values
        instance_arr.append(instance)
    max_instance_num_of_task = max(instance_arr)
    min_instance_num_of_task = min(instance_arr)
    avg_instance_num_of_task = sum(instance_arr) / len(task)
    print('max_instance_num_of_task:' + str(max_instance_num_of_task))
    print('min_instance_num_of_task:' + str(min_instance_num_of_task))
    print('avg_instance_num_of_task:' + str(avg_instance_num_of_task))

    avg_instance_duration, max_instance_duration, min_instance_duration = 0, 0, 0
    avg_task_duration, max_task_duration, min_task_duration = 0, 0, 0
    print(instance_df.groupby([instance_df.iloc[:, 2], instance_df.iloc[:, 3]]))
    print(instance_df.groupby(instance_df.iloc[:, 3]).mean())
    # for indexs in instance_df.index:
    #     record = instance_df.loc[indexs].values[0:-1]

    # print('Average instance duration:' + str())
    # print('Maximum instance duration:' + str())
    # print('Minimum instance duration:' + str())
    # print('Average task duration:' + str())
    # print('Maximum task duration:' + str())
    # print('Minimum task duration:' + str())


if __name__ == '__main__':
    statistics_data()
