from sklearn.cluster import KMeans
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


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

    duration_of_job_array, cpu_of_job_array, mem_of_job_array = [], [], []
    # terminal_job = terminal_job.slice(0, 101)
    for i in terminal_job:
        print('i :' + str(i))
        # 获取job_id == i的所有行（即job i的所有task）
        job_obj = task_df[task_df.iloc[:, 2] == i]
        # job_obj = job_obj[job_obj.iloc[:, 5] == 'Terminated']

        # job执行时间（最晚修改时间-最早开始时间?）
        min_create_time = np.min(job_obj.iloc[:, 0].values)
        max_modify_time = np.max(job_obj.iloc[:, 1].values)
        duration_of_job = max_modify_time - min_create_time

        # cpu && memory
        job_task = instance_df[instance_df.iloc[:, 2] == i]
        task = job_task.iloc[:, 3].values
        task = np.unique(task)
        sumCPU, sumMem = [], []
        if len(task) > 0:
            for j in task:
                print('j:' + str(j))
                instance = instance_df[(instance_df.iloc[:, 3] == j) & (instance_df.iloc[:, 5] == 'Terminated')]
                cpu = instance.iloc[:, 9].values
                sum_cpu = np.sum(cpu) / len(instance)
                sumCPU.append(sum_cpu)

                memory = instance.iloc[:, 11].values
                sum_memory = np.sum(memory) / len(instance)
                sumMem.append(sum_memory)
        else:
            sumCPU.append(0)
            sumMem.append(0)
        cpu_of_job = np.sum(sumCPU)
        mem_of_job = np.sum(sumMem)

        duration_of_job_array.append(duration_of_job)
        cpu_of_job_array.append(cpu_of_job)
        mem_of_job_array.append(mem_of_job)

    # 将数据带入到聚类模型中
    loan = []
    loan.append(duration_of_job_array)
    loan.append(cpu_of_job_array)
    loan.append(mem_of_job_array)
    loan = np.asarray(loan)
    loan = loan.transpose()

    # loan = np.vstack(duration_of_job_array,cpu_of_job_array,mem_of_job_array)

    K = range(1, 10)
    meandistortions = []
    for k in K:
        kmeans = KMeans(n_clusters=k)
        kmeans.fit(loan)
        # kmeans.fit(tasknum,plancpunum,planmemnum)
        # meandistortions.append(sum(np.min(cdist(X, kmeans.cluster_centers_, 'euclidean'), axis=1)) / X.shape[0])
        meandistortions.append(kmeans.inertia_)
    plt.plot(K, meandistortions, 'o-')
    plt.axvline(3, ls="--", color="r")
    plt.xlabel('number of clusters')
    plt.ylabel('the average degree of distortion')
    plt.title('Use the elbow rule to determine the best K value')
    plt.savefig('../images/find_k_job.png')
    plt.show()


if __name__ == '__main__':
    graph()
