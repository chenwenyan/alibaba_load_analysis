import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def count(list, key):
    res = 0
    for i in list:
        if i <= key:
            res = res + 1
    return res

def graph():
    # 读取数据
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

    # 获取所有正常结束的job
    terminal_job = task_df[task_df.iloc[:, 5] == 'Terminated']
    terminal_job = np.unique(terminal_job.iloc[:, 3].values)
    duration_of_job_array, cpu_of_job_array, mem_of_job_array = [],[],[]
    for i in terminal_job:
        print('i :' + str(i))
        # 获取job_id == i的所有行（即所有task）
        job_obj = task_df[task_df.iloc[:, 2] == i]
        job_obj = job_obj[task_df.iloc[:, 5] == 'Terminated']
        print(len(job_obj.iloc[:, 0].values))

        # job执行时间（最晚修改时间-最早开始时间?）
        if len(job_obj.iloc[:, 0].values) > 0:
            min_create_time = np.min(job_obj.iloc[:, 0].values)
            max_modify_time = np.max(job_obj.iloc[:, 1].values)
            duration_of_job = max_modify_time - min_create_time
            duration_of_job_array.append(duration_of_job)
        else:
            duration_of_job_array.append(0)

    unique_duration_of_job_array = np.unique(duration_of_job_array)
    arr = []
    for j in unique_duration_of_job_array:
        a = count(duration_of_job_array, j)
        arr.append(a)
    x = [x/3600 for x in unique_duration_of_job_array]
    y = [x/len(terminal_job) for x in arr]

    print('max excute time:' + str(np.max(duration_of_job_array)))
    plt.figure()
    plt.plot(x, y)
    plt.xlabel('job duration(hour)')
    plt.xlim(0.001,10)
    plt.ylabel('portion of jobs')
    plt.savefig('../images/figure_9.png')
    plt.show()

if __name__ == '__main__':
    graph()