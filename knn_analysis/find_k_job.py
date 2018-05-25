from sklearn.cluster import KMeans
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def graph():
    # 读取用于聚类的数据
    data = pd.read_csv('../dataset/batch_task.csv', header=None, iterator=True)
    # task_df = pd.DataFrame(task_df,columns=('create_timestamp','modify_timestamp','job_id','task_id','instance_num','status',
    #                                                                             'plan_cpu','plan_mem'))

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

    jobs = df.iloc[:, 2].values
    jobs = np.unique(jobs)
    task_num, plan_cpu_num, plan_mem_num = [], [], []
    for i in jobs:
        job = df[df.iloc[:, 2] == i]
        tasks = len(job)
        a = job.iloc[:, 4].values
        b = job.iloc[:, 6].values
        c = job.iloc[:, 7].values
        a = [0 if np.isnan(x) else x for x in a]
        b = [0 if np.isnan(x) else x for x in b]
        c = [0 if np.isnan(x) else x for x in c]
        plan_cpu_total = sum(np.array(a) * np.array(b))
        plan_mem_total = sum(np.array(a) * np.array(c))
        task_num.append(tasks)
        plan_cpu_num.append(plan_cpu_total / tasks)
        plan_mem_num.append(plan_mem_total / tasks)

    # 将数据带入到聚类模型中
    loan = []
    loan.append(task_num)
    loan.append(plan_cpu_num)
    loan.append(plan_mem_num)
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
