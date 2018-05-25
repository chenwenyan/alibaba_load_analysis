import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# 自定义函数 统计list数组中小于等于某个值的数目
def sum_smaller_than_num(list, num):
    count = 0
    for i in list:
        if i <= num:
            count = count + 1
    return count


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
        max_cpu_util_arr.append(np.sum(cpu_util_arr <= max_cpu_util))
        min_cpu_util_arr.append(np.sum(cpu_util_arr <= min_cpu_util))
        avg_cpu_util_arr.append(np.sum(cpu_util_arr <= avg_cpu_util))

        mem_util_arr = record.iloc[:, 3].values
        max_mem_util = np.max(mem_util_arr)
        min_mem_util = np.min(mem_util_arr)
        avg_mem_util = np.average(mem_util_arr)
        max_mem_util_arr.append(np.sum(mem_util_arr <= max_mem_util))
        min_mem_util_arr.append(np.sum(mem_util_arr <= min_mem_util))
        avg_mem_util_arr.append(np.sum(mem_util_arr <= avg_mem_util))

    max_cpu_util_arr = [x / 100 for x in max_cpu_util_arr]
    avg_cpu_util_arr = [x / 100 for x in avg_cpu_util_arr]
    min_cpu_util_arr = [x / 100 for x in min_cpu_util_arr]

    max_mem_util_arr = [x / 100 for x in max_mem_util_arr]
    avg_mem_util_arr = [x / 100 for x in avg_mem_util_arr]
    min_mem_util_arr = [x / 100 for x in min_mem_util_arr]

    max_cpu_util_arr_unique = np.unique(max_cpu_util_arr)
    min_cpu_util_arr_unique = np.unique(min_cpu_util_arr)
    avg_cpu_util_arr_unique = np.unique(avg_cpu_util_arr)
    max_mem_util_arr_unique = np.unique(max_mem_util_arr)
    min_mem_util_arr_unique = np.unique(min_mem_util_arr)
    avg_mem_util_arr_unique = np.unique(avg_mem_util_arr)
    CDF_max_cpu_usage_arr, CDF_min_cpu_usage_arr, CDF_avg_cpu_usage_arr, \
    CDF_max_mem_usage_arr, CDF_min_mem_usage_arr, CDF_avg_mem_usage_arr = [], [], [], [], [], []

    x = []
    i = 0
    while i < 1000:
        x.append(0.001 * i)
        i = i + 1

    for m in x:
        CDF_max_cpu_usage = sum_smaller_than_num(max_cpu_util_arr, m) / len(max_cpu_util_arr)
        CDF_max_cpu_usage_arr.append(CDF_max_cpu_usage)
        CDF_min_cpu_usage = sum_smaller_than_num(min_cpu_util_arr, m) / len(min_cpu_util_arr)
        CDF_min_cpu_usage_arr.append(CDF_min_cpu_usage)
        CDF_avg_cpu_usage = sum_smaller_than_num(avg_cpu_util_arr, m) / len(avg_cpu_util_arr)
        CDF_avg_cpu_usage_arr.append(CDF_avg_cpu_usage)
        CDF_max_mem_usage = sum_smaller_than_num(max_mem_util_arr, m) / len(max_mem_util_arr)
        CDF_max_mem_usage_arr.append(CDF_max_mem_usage)
        CDF_min_mem_usage = sum_smaller_than_num(min_mem_util_arr, m) / len(min_mem_util_arr)
        CDF_min_mem_usage_arr.append(CDF_min_mem_usage)
        CDF_avg_mem_usage = sum_smaller_than_num(avg_mem_util_arr, m) / len(avg_mem_util_arr)
        CDF_avg_mem_usage_arr.append(CDF_avg_mem_usage)

    # for m in max_cpu_util_arr_unique:
    #     CDF_max_cpu_usage = sum_smaller_than_num(max_cpu_util_arr, m)/len(max_cpu_util_arr)
    #     CDF_max_cpu_usage_arr.append(CDF_max_cpu_usage)
    #
    # for m in min_cpu_util_arr_unique:
    #     CDF_min_cpu_usage = sum_smaller_than_num(min_cpu_util_arr, m)/len(min_cpu_util_arr)
    #     CDF_min_cpu_usage_arr.append(CDF_min_cpu_usage)
    #
    # for m in avg_cpu_util_arr_unique:
    #     CDF_avg_cpu_usage = sum_smaller_than_num(avg_cpu_util_arr, m)/len(avg_cpu_util_arr)
    #     CDF_avg_cpu_usage_arr.append(CDF_avg_cpu_usage)
    #
    # for m in max_mem_util_arr_unique:
    #     CDF_max_mem_usage = sum_smaller_than_num(max_mem_util_arr, m)/len(max_mem_util_arr)
    #     CDF_max_mem_usage_arr.append(CDF_max_mem_usage)
    #
    # for m in min_mem_util_arr_unique:
    #     CDF_min_mem_usage = sum_smaller_than_num(min_mem_util_arr, m)/len(min_mem_util_arr)
    #     CDF_min_mem_usage_arr.append(CDF_min_mem_usage)
    #
    # for m in avg_mem_util_arr_unique:
    #     CDF_avg_mem_usage = sum_smaller_than_num(avg_mem_util_arr, m)/len(avg_mem_util_arr)
    #     CDF_avg_mem_usage_arr.append(CDF_avg_mem_usage)

    plt.xlim(0, 1.0)
    # x = range(0, 1.0, 0.001)
    plt.plot(x, CDF_max_cpu_usage_arr, 'r', label='max cpu usage')
    plt.plot(x, CDF_avg_cpu_usage_arr, 'g', label='avg cpu')
    plt.plot(x, CDF_min_cpu_usage_arr, 'b', label='min cpu')
    plt.plot(x, CDF_max_mem_usage_arr, 'r--', label='max memory')
    plt.plot(x, CDF_avg_mem_usage_arr, 'g--', label='avg memory')
    plt.plot(x, CDF_min_mem_usage_arr, 'b--', label='min memory')
    # x = timestamp
    # plt.plot(x, max_cpu_util_arr, 'r', label='max cpu usage')
    # plt.plot(x, avg_cpu_util_arr, 'g', label='avg cpu')
    # plt.plot(x, min_cpu_util_arr, 'b', label='min cpu')
    # plt.plot(x, max_mem_util_arr, 'r--', label='max memory')
    # plt.plot(x, avg_mem_util_arr, 'g--', label='avg memory')
    # plt.plot(x, min_mem_util_arr, 'b--', label='min memory')
    plt.legend(loc='best')
    plt.xlabel('average utilization of 12 hours')
    plt.ylabel('portion of instances')

    plt.savefig('../images/figure_7.png')
    plt.show()

if __name__ == '__main__':
    graph()
