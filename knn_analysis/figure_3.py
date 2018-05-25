from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def count(list, key):
    res = 0
    for i in list:
        if i <= key:
            res = res + 1
    return res

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
    instance = instance_df.iloc[:, 0].values
    print('total instance:' + str(len(instance)))

    avg_cpu_request_arr, avg_mem_request_arr = [], []
    for i in job:
        task_of_job = task_df[task_df.iloc[:, 2] == i]
        cpu_of_task = sum(task_of_job.iloc[:, 4] * task_of_job.iloc[:, 6])
        mem_of_task = sum(task_of_job.iloc[:, 4] * task_of_job.iloc[:, 7])
        avg_cpu_request = cpu_of_task/len(task_of_job)
        avg_mem_request = mem_of_task/len(task_of_job)
        avg_cpu_request_arr.append(avg_cpu_request)
        avg_mem_request_arr.append(avg_mem_request)

    avg_cpu_request_arr = sorted(avg_cpu_request_arr)
    job_num = []
    for j in avg_cpu_request_arr:
        job_num_of_avg_cpu = avg_cpu_request_arr.count(j)
        job_num.append(job_num_of_avg_cpu)

    avg_mem_request_arr = sorted(avg_mem_request_arr)
    job_num_mem = []
    for k in avg_mem_request_arr:
        job_num_of_avg_mem = avg_mem_request_arr.count(k)
        job_num_mem.append(job_num_of_avg_mem)

    sum_real_cpu_avg_arr = []
    for n in task:
        instance_of_task = instance_df[instance_df.iloc[:, 3] == n]
        sum_real_cpu_avg = sum(instance_of_task.iloc[:, 9].values)
        sum_real_cpu_avg_arr.append(sum_real_cpu_avg/len(instance_of_task))
        
    print(sum_real_cpu_avg_arr)

    # 获取所有正常结束的job
    terminal_job = task_df[task_df.iloc[:, 5] == 'Terminated']
    terminal_job = np.unique(terminal_job.iloc[:, 3].values)

    avg_cpu_used_task_of_job_arr = []
    for i in terminal_job:
        print("**************")
        tasks = instance_df[instance_df.iloc[:, 2] == i]
        tasks = tasks[tasks.iloc[:, 5] == 'Terminated']
        # task_num_of_job = len(tasks.iloc[:, 0].values)
        sum_cpu_used = sum(tasks.iloc[:, 9].values)
        avg_cpu_used_task_of_job = np.average(sum_cpu_used)
        avg_cpu_used_task_of_job_arr.append(avg_cpu_used_task_of_job)

    count_job_num_cpu_used = []
    avg_cpu_used_task_of_job_unique = np.unique(avg_cpu_used_task_of_job_arr)
    for j in avg_cpu_used_task_of_job_unique:
        num_cpu_used_of_job = count(avg_cpu_used_task_of_job_arr, j)
        count_job_num_cpu_used.append(num_cpu_used_of_job)


    # 绘图
    fig = plt.figure(figsize=(10,4))
    ax1 = fig.add_subplot(1, 2, 1)
    ax2 = fig.add_subplot(1, 2, 2)

    ax1.bar(avg_cpu_request_arr, job_num, width = 50, color='green')
    ax2.bar(avg_cpu_used_task_of_job_unique, count_job_num_cpu_used, width = 50, color='green')
    ax1.set_xlabel('numbers of cpu request(average per task)')
    ax1.set_ylabel('number of jobs')
    ax2.set_xlabel('numbers of memory request(average per task)')
    ax2.set_ylabel('number of jobs')

    # 保存图片
    plt.savefig('../images/figure_3.png')
    plt.show()

if __name__ == '__main__':
    statistics_data()