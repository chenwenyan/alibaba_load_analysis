import matplotlib.pyplot as plt
from matplotlib import colors
import pandas as pd
import numpy as np

def graph_data():
    data = pd.read_csv('../dataset/server_usage.csv', ',')
    data = data.values
    df = pd.DataFrame(data, columns=('timestamp', 'id', 'cpu', 'memory', 'disk', 'load1', 'load5', 'load15'))

    fig = plt.figure(figsize=(11,4))
    ax1 = fig.add_subplot(1, 2, 1)
    ax2 = fig.add_subplot(1, 2, 2)
    # 设置图表标题并给坐标轴加上标签
    ax1.set_title("CPU utilization")
    ax2.set_title("memory utilization")
    ax1.set_ylabel('machineID')
    ax2.set_ylabel('machineID')
    ax1.set_xlabel('time(hour)')
    ax2.set_xlabel('time(hour)')
    norm = colors.Normalize(vmin=0, vmax=100)
    id_list = df.id.values
    id_list = np.unique(sorted(id_list))
    for i in id_list:
        df_id = df[df.id == i]
        ids = df_id.iloc[:, 1].values
        times = [(x / 3600 - 11) for x in df_id.iloc[:, 0].values]
        cpu = df_id.iloc[:, 2].values
        mem = df_id.iloc[:, 3].values
        ax1.scatter(times, ids, c=cpu, norm=norm, alpha=0.5, s=2.0)
        ax2.scatter(times, ids, c=mem, norm=norm, alpha=0.5, s=2.0)
    # 绘制渐变色标注
    gci = plt.scatter(times, ids, c=cpu, norm=norm, alpha=0.5, s=2.0)
    cbar = plt.colorbar(gci)
    cbar.set_label('used')
    cbar.set_ticks(np.linspace(0, 100, 6))
    # cbar.set_ticklabels(('0','20','40','60','80','100'))
    # 保存图片
    # plt.savefig('../images/server_usage_cpu.png')
    plt.savefig('../images/figure_1.png')
    plt.show()


if __name__ == '__main__':
    graph_data()
