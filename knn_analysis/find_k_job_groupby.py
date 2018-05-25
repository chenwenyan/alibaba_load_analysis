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

    grouped_by_job = instance_df.groupby(instance_df.iloc[:, 2])
    grouped_by_job_task = instance_df.groupby([instance_df.iloc[:, 2], instance_df.iloc[:, 3]])
    print(grouped_by_job.iloc[:, 3].count())
    print(grouped_by_job_task.count())
    task_num_arr = grouped_by_job
    instance_num_arr = grouped_by_job_task
    # while grouped_by_job.next():
    #     record = grouped_by_job_task
    #     print(record)

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
