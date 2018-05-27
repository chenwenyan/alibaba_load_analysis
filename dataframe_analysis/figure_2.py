import matplotlib.pyplot as plt
from matplotlib import colors
import pandas as pd
import numpy as np


def graph_data():
    data = pd.read_csv('../dataset/server_usage.csv', ',')
    data = data.values
    df = pd.DataFrame(data, columns=('timestamp', 'id', 'cpu', 'memory', 'disk', 'load1', 'load5', 'load15'))
    fig = plt.figure(figsize=(10, 4))
    ax1 = fig.add_subplot(1, 2, 1)
    ax2 = fig.add_subplot(1, 2, 2)
    # 设置图表标题并给坐标轴加上标签
    # plt.title("cpu utilization")
    # plt.title("memory utilization")
    ax1.set_ylabel('machine cpu utilization')
    ax2.set_ylabel('machine memory utilization')
    ax1.set_xlabel('time(h)')
    ax2.set_xlabel('time(h)')

    timestamp = df.timestamp.values
    timestamp = list(set(timestamp))
    timestamp = sorted(timestamp)

    cpumax, cpuavg, cpumin = [], [], []
    memmax, memavg, memmin = [], [], []
    for i in timestamp:
        record = df[df.timestamp == i]
        cpu = record.cpu.values
        mem = record.memory.values
        cpumax.append(np.max(cpu))
        cpuavg.append(np.average(cpu))
        cpumin.append(np.min(cpu))
        memmax.append(np.max(mem))
        memavg.append(np.average(mem))
        memmin.append(np.min(mem))
    timestamp = [(x / 3600 - 11) for x in timestamp]

    ax1.plot(timestamp, cpumax, label='max')
    ax1.plot(timestamp, cpuavg, label='avg')
    ax1.plot(timestamp, cpumin, label='min')

    ax2.plot(timestamp, memmax, label='max')
    ax2.plot(timestamp, memavg, label='avg')
    ax2.plot(timestamp, memmin, label='min')

    # 最大cpu和最小cpu之间用颜色填充
    ax1.fill_between(timestamp, cpumin, cpumax, facecolor='#FFB6C1', edgecolor='green')
    ax2.fill_between(timestamp, memmin, memmax, facecolor='#FFB6C1', edgecolor='green')
    ax1.set_ylim(0, 140)
    ax2.set_ylim(0, 140)
    ax1.legend(loc='upper left')
    ax2.legend(loc='upper left')
    # plt.savefig('../images/server_usage_cpu_maxminavg.png')
    plt.savefig('../images/figure_2.png')
    plt.show()


if __name__ == '__main__':
    graph_data()
