import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def graph():
    # 超大文件时的处理
    server_usage_df = pd.read_csv('../dataset/server_usage.csv', header=None, iterator=True)
    container_usage_df = pd.read_csv('../dataset/container_usage.csv', header=None, iterator=True)

    chunkSize = 10000
    # batch_task 过大时迭代读取数据
    task_loop = True
    chunks = []
    while task_loop:
        try:
            chunk = server_usage_df.get_chunk(chunkSize)
            chunks.append(chunk)
        except StopIteration:
            task_loop = False
            print('server_usage Iteration is stopped')
    server_usage_df = pd.concat(chunks, ignore_index=True)
    # server_usage_df = pd.DataFrame(server_usage_df,columns=('timestamp','machineID','CPU','memory','disk','load1',
    # 'load5', 'load15'))

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
    # container_usage_df = pd.DataFrame(container_usage_df, columns=('ts','instance_id','cpu_util','mem_util','disk_util','load1',
    # 'load5','load15', 'avg_cpi', 'avg_mpki', 'max_cpi', 'max_mkpi'))

    times = container_usage_df.iloc[:, 0].values
    timestamp = sorted(np.unique(times))

    max_load1_arr, avg_load1_arr = [], []
    max_load5_arr, avg_load5_arr = [], []
    max_load15_arr, avg_load15_arr = [], []
    for i in timestamp:
        record = container_usage_df[container_usage_df.iloc[:, 0] == i]
        load1 = record.iloc[:, 5].values
        max_load1 = max(load1)
        avg_load1 = np.average(load1)
        load5 = record.iloc[:, 6].values
        max_load5 = max(load5)
        avg_load5 = np.average(load5)
        load15 = record.iloc[:, 7].values
        max_load15 = max(load15)
        avg_load15 = np.average(load15)
        max_load1_arr.append(max_load1)
        max_load5_arr.append(max_load5)
        max_load15_arr.append(max_load15)
        avg_load1_arr.append(avg_load1)
        avg_load5_arr.append(avg_load5)
        avg_load15_arr.append(avg_load15)

    server_times = server_usage_df.iloc[:, 0].values
    server_timestamp = sorted(np.unique(server_times))

    server_max_load1_arr, server_avg_load1_arr = [], []
    server_max_load5_arr, server_avg_load5_arr = [], []
    server_max_load15_arr, server_avg_load15_arr = [], []
    for i in server_timestamp:
        record = server_usage_df[server_usage_df.iloc[:, 0] == i]
        load1 = record.iloc[:, 5].values
        max_load1 = max(load1)
        avg_load1 = np.average(load1)
        load5 = record.iloc[:, 6].values
        max_load5 = max(load5)
        avg_load5 = np.average(load5)
        load15 = record.iloc[:, 7].values
        max_load15 = max(load15)
        avg_load15 = np.average(load15)
        server_max_load1_arr.append(max_load1)
        server_max_load5_arr.append(max_load5)
        server_max_load15_arr.append(max_load15)
        server_avg_load1_arr.append(avg_load1)
        server_avg_load5_arr.append(avg_load5)
        server_avg_load15_arr.append(avg_load15)

    fig = plt.figure(figsize=(15, 6))
    ax1 = fig.add_subplot(2, 3, 1)
    ax2 = fig.add_subplot(2, 3, 2)
    ax3 = fig.add_subplot(2, 3, 3)
    ax4 = fig.add_subplot(2, 3, 4)
    ax5 = fig.add_subplot(2, 3, 5)
    ax6 = fig.add_subplot(2, 3, 6)

    timestamp = [(x / 3600 - 11) for x in timestamp]
    max_load1_arr = [x / 100 for x in max_load1_arr]
    avg_load1_arr = [x / 100 for x in avg_load1_arr]

    ax1.plot(timestamp, max_load1_arr, label='max')
    ax1.plot(timestamp, avg_load1_arr, label='avg')
    # 最大cpu和最小cpu之间用颜色填充
    ax1.fill_between(timestamp, avg_load1_arr, max_load1_arr, facecolor='#FFB6C1', edgecolor='green')
    ax1.legend(loc='best')
    ax1.set_ylim(0, 1.4)
    ax1.set_xlabel('time(h)')
    ax1.set_ylabel('portion of CPU loads (container)')
    ax1.set_title('1 min')

    max_load5_arr = [x / 100 for x in max_load5_arr]
    avg_load5_arr = [x / 100 for x in avg_load5_arr]
    ax2.plot(timestamp, max_load5_arr, label='max')
    ax2.plot(timestamp, avg_load5_arr, label='avg')
    # 最大cpu和最小cpu之间用颜色填充
    ax2.fill_between(timestamp, avg_load5_arr, max_load5_arr, facecolor='#FFB6C1', edgecolor='green')
    ax2.legend(loc='best')
    ax2.set_ylim(0, 1.4)
    ax2.set_xlabel('time(h)')
    ax2.set_title('5 min')

    max_load15_arr = [x / 100 for x in max_load15_arr]
    avg_load15_arr = [x / 100 for x in avg_load15_arr]
    ax3.plot(timestamp, max_load15_arr, label='max')
    ax3.plot(timestamp, avg_load15_arr, label='avg')
    # 最大cpu和最小cpu之间用颜色填充
    ax3.fill_between(timestamp, avg_load15_arr, max_load15_arr, facecolor='#FFB6C1', edgecolor='green')
    ax3.legend(loc='best')
    ax3.set_ylim(0, 1.4)
    ax3.set_xlabel('time(h)')
    ax3.set_title('15 min')

    server_timestamp = [(x / 3600 - 11) for x in server_timestamp]
    server_max_load1_arr = [x / 100 for x in server_max_load1_arr]
    server_avg_load1_arr = [x / 100 for x in server_avg_load1_arr]

    ax4.plot(server_timestamp, server_max_load1_arr, label='max')
    ax4.plot(server_timestamp, server_avg_load1_arr, label='avg')
    # 最大cpu和最小cpu之间用颜色填充
    ax4.fill_between(server_timestamp, 0, server_max_load1_arr, facecolor='#FFB6C1', edgecolor='green')
    ax4.legend(loc='best')
    ax4.set_ylim(0, 1.4)
    ax4.set_xlabel('time(h)')
    ax4.set_ylabel('portion of CPU loads (machine)')

    server_max_load5_arr = [x / 100 for x in server_max_load5_arr]
    server_avg_load5_arr = [x / 100 for x in server_avg_load5_arr]
    ax5.plot(server_timestamp, server_max_load5_arr, label='max')
    ax5.plot(server_timestamp, server_avg_load5_arr, label='avg')
    # 最大cpu和最小cpu之间用颜色填充
    ax5.fill_between(server_timestamp, 0, server_max_load5_arr, facecolor='#FFB6C1', edgecolor='green')
    ax5.legend(loc='best')
    ax5.set_ylim(0, 1.4)
    ax5.set_xlabel('time(h)')

    server_max_load15_arr = [x / 100 for x in server_max_load15_arr]
    server_avg_load15_arr = [x / 100 for x in server_avg_load15_arr]
    ax6.plot(server_timestamp, server_max_load15_arr, label='max')
    ax6.plot(server_timestamp, server_avg_load15_arr, label='avg')
    # 最大cpu和最小cpu之间用颜色填充
    ax6.fill_between(server_timestamp, 0, server_max_load15_arr, facecolor='#FFB6C1', edgecolor='green')
    ax6.legend(loc='best')
    ax6.set_ylim(0, 1.4)
    ax6.set_xlabel('time(h)')

    plt.savefig('../images/figure_8.png')
    plt.show()


if __name__ == '__main__':
    graph()
