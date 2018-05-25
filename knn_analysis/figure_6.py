import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def graph():
    # 读取数据
    # container_event = pd.read_csv('../dataset/container_event.csv', header=None, usecols=10000)
    # container_usage = pd.read_csv('../dataset/container_usage.csv', header=None, usecols=10000)
    # container_event_df = pd.DataFrame(container_event)
    # container_usage_df = pd.DataFrame(container_usage)

    # 超大文件时的处理
    container_event_df = pd.read_csv('../dataset/container_event.csv', header=None, iterator=True)
    container_usage_df = pd.read_csv('../dataset/container_usage.csv', header=None, iterator=True)

    chunkSize = 10000
    # batch_task 过大时迭代读取数据
    task_loop = True
    chunks = []
    while task_loop:
        try:
            chunk = container_event_df.get_chunk(chunkSize)
            chunks.append(chunk)
        except StopIteration:
            task_loop = False
            print('batch_task Iteration is stopped')
    container_event_df = pd.concat(chunks, ignore_index=True)
    print(container_event_df.count())
    print(container_event_df.tail(5))
    # container_event_df = pd.DataFrame(container_event_df,columns=('ts','event','instance_id','machine_id','plan_cpu','plan_mem',
    # 'plan_disk', 'cpuset'))


    # batch_instance 过大时迭代读取数据
    instance_loop = True
    instance_chunks = []
    while instance_loop:
        try:
            chunk = container_usage_df.get_chunk(chunkSize)
            instance_chunks.append(chunk)
        except StopIteration:
            instance_loop = False
            print('batch_instance Iteration is stopped')
    container_usage_df = pd.concat(instance_chunks, ignore_index=True)
    print(container_usage_df.count())
    print(container_usage_df.tail())
    # container_usage_df = pd.DataFrame(container_usage_df, columns=('ts','instance_id','cpu_util','mem_util','disk_util','load1',
    # 'load5','load15', 'avg_cpi', 'avg_mpki', 'max_cpi', 'max_mkpi'))

    times = container_usage_df.iloc[:, 0].values
    timestamp = sorted(np.unique(times))

    max_cpu_util_arr, min_cpu_util_arr, avg_cpu_util_arr = [], [], []
    max_mem_util_arr, min_mem_util_arr, avg_mem_util_arr = [], [], []
    for i in timestamp:
        record = container_usage_df[container_usage_df.iloc[:, 0] == i]
        cpu_util_arr = record.iloc[:, 2].values
        max_cpu_util = np.max(cpu_util_arr)
        min_cpu_util = np.min(cpu_util_arr)
        avg_cpu_util = np.average(cpu_util_arr)
        max_cpu_util_arr.append(max_cpu_util)
        min_cpu_util_arr.append(min_cpu_util)
        avg_cpu_util_arr.append(avg_cpu_util)

        mem_util_arr = record.iloc[:, 3].values
        max_mem_util = np.max(mem_util_arr)
        min_mem_util = np.min(mem_util_arr)
        avg_mem_util = np.average(mem_util_arr)
        max_mem_util_arr.append(max_mem_util)
        min_mem_util_arr.append(min_mem_util)
        avg_mem_util_arr.append(avg_mem_util)

    timestamp = [(x/3600-11) for x in timestamp]
    max_cpu_util_arr = [x/100 for x in max_cpu_util_arr]
    avg_cpu_util_arr = [x/100 for x in avg_cpu_util_arr]
    min_cpu_util_arr = [x/100 for x in min_cpu_util_arr]

    max_mem_util_arr = [x/100 for x in max_mem_util_arr]
    avg_mem_util_arr = [x/100 for x in avg_mem_util_arr]
    min_mem_util_arr = [x/100 for x in min_mem_util_arr]

    fig = plt.figure(figsize=(10,4))
    ax1 = fig.add_subplot(1, 2, 1)
    ax2 = fig.add_subplot(1, 2, 2)

    ax1.plot(timestamp,max_cpu_util_arr,label='max')
    ax1.plot(timestamp,avg_cpu_util_arr,label='avg')
    ax1.plot(timestamp,min_cpu_util_arr,label='min')

    ax2.plot(timestamp,max_mem_util_arr,label='max')
    ax2.plot(timestamp,avg_mem_util_arr,label='avg')
    ax2.plot(timestamp,min_mem_util_arr,label='min')

    # 最大cpu和最小cpu之间用颜色填充
    ax1.fill_between(timestamp, min_cpu_util_arr, max_cpu_util_arr, facecolor='#FFB6C1', edgecolor='green')
    ax2.fill_between(timestamp, min_mem_util_arr, max_mem_util_arr, facecolor='#FFB6C1', edgecolor='green')
    ax1.legend(loc='best')
    ax2.legend(loc='best')
    ax1.set_xlabel('time(h)')
    ax2.set_xlabel('time(h)')
    ax1.set_ylabel('used/request(CPU)')
    ax2.set_ylabel('used/request(memory)')
    ax1.set_ylim(0, 1.4)
    ax2.set_ylim(0, 1.4)

    plt.savefig('../images/figure_6.png')
    plt.show()

if __name__ == '__main__':
    graph()