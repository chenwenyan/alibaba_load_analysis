import MySQLdb as mdb
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from mpl_toolkits.mplot3d import Axes3D


# 自定义归一化方法
def MaxMinNormalization(list, Max, Min):
    res = []
    for x in list:
        x = (x - Min) / (Max - Min)
        res.append(x)
    return res


def graph():
    # 连接数据库
    conn = mdb.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='alibaba_trace', charset='utf8')

    # 如果使用事务引擎，可以设置自动提交事务，或者在每次操作完成后手动提交事务conn.commit()
    conn.autocommit(1)  # conn.autocommit(True)

    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 因该模块底层其实是调用CAPI的，所以，需要先得到当前指向数据库的指针。

    try:
        # 查询数据条目
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='short_less_less'")
        short_less_less = cursor.fetchall()
        short_less_less = list(short_less_less)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='short_less_mid'")
        short_less_mid = cursor.fetchall()
        short_less_mid = list(short_less_mid)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='short_mid_less'")
        short_mid_less = cursor.fetchall()
        short_mid_less = list(short_mid_less)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='short_mid_mid'")
        short_mid_mid = cursor.fetchall()
        short_mid_mid = list(short_mid_mid)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='short_hungry_less'")
        short_hungry_less = cursor.fetchall()
        short_hungry_less = list(short_hungry_less)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='short_hungry_mid'")
        short_hungry_mid = cursor.fetchall()
        short_hungry_mid = list(short_hungry_mid)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='medium_less_less'")
        medium_less_less = cursor.fetchall()
        medium_less_less = list(medium_less_less)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='medium_less_mid'")
        medium_less_mid = cursor.fetchall()
        medium_less_mid = list(medium_less_mid)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='medium_mid_less'")
        medium_mid_less = cursor.fetchall()
        medium_mid_less = list(medium_mid_less)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='medium_mid_mid'")
        medium_mid_mid = cursor.fetchall()
        medium_mid_mid = list(medium_mid_mid)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='medium_hungry_less'")
        medium_hungry_less = cursor.fetchall()
        medium_hungry_less = list(medium_hungry_less)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='medium_hungry_mid'")
        medium_hungry_mid = cursor.fetchall()
        medium_hungry_mid = list(medium_hungry_mid)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='long_less_less'")
        long_less_less = cursor.fetchall()
        long_less_less = list(long_less_less)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='long_less_mid'")
        long_less_mid = cursor.fetchall()
        long_less_mid = list(long_less_mid)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='long_mid_less'")
        long_mid_less = cursor.fetchall()
        long_mid_less = list(long_mid_less)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='long_mid_mid'")
        long_mid_mid = cursor.fetchall()
        long_mid_mid = list(long_mid_mid)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='long_hungry_less'")
        long_hungry_less = cursor.fetchall()
        long_hungry_less = list(long_hungry_less)
        cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='long_hungry_mid'")
        long_hungry_mid = cursor.fetchall()
        long_hungry_mid = list(long_hungry_mid)

    except:
        import traceback
        traceback.print_exc()
        # 发生错误时回滚
        conn.rollback()
    finally:
        # 关闭游标连接
        cursor.close()
        # 关闭数据库连接
        conn.close()

    fig = plt.figure(figsize=(6,5))
    # ax1 = fig.add_subplot(111)
    # x = ['sll', 'slm', 'sml', 'smm', 'shl', 'shm', 'mll',
    #      'mlm', 'mml', 'mmm', 'mhl', 'mhm', 'lll', 'llm',
    #      'lml', 'lmm', 'lhl', 'lhm']
    # x = np.arange(18)
    # y = [len(short_less_less), len(short_less_mid), len(short_mid_less), len(short_mid_mid), len(short_hungry_less),
    #      len(short_hungry_mid), len(medium_less_less),
    #      len(medium_less_mid), len(medium_mid_less), len(medium_mid_mid), len(medium_hungry_less),
    #      len(medium_hungry_mid), len(long_less_less), len(long_less_mid),
    #      len(long_mid_less), len(long_mid_mid), len(long_hungry_less), len(long_hungry_mid)]
    # y = [i/100 for i in y]
    # ax1.bar(x, y)
    # for a,b in zip(x,y):
    #     ax1.text(a, b+0.05, '%d' % b , ha='center', va= 'bottom',fontsize=11)
    # ax1.set_xlabel('category')
    # ax1.set_ylabel('job number')
    # ax1.set_ylim(0, 3000)
    # plt.savefig('../imgs_mysql/hist_category_job.png')

    # 3d图形绘制
    res = []
    res[:] = map(list, short_less_less)
    short_less_less_duration = [x[1] for x in res]
    short_less_less_cpu = [x[2] for x in res]
    short_less_less_mem = [x[3] for x in res]

    res = []
    res[:] = map(list, short_mid_less)
    short_mid_less_duration = [x[1] for x in res]
    short_mid_less_cpu = [x[2] for x in res]
    short_mid_less_mem = [x[3] for x in res]

    res = []
    res[:] = map(list, short_mid_mid)
    short_mid_mid_duration = [x[1] for x in res]
    short_mid_mid_cpu = [x[2] for x in res]
    short_mid_mid_mem = [x[3] for x in res]

    res = []
    res[:] = map(list, short_hungry_less)
    short_hungry_less_duration = [x[1] for x in res]
    short_hungry_less_cpu = [x[2] for x in res]
    short_hungry_less_mem = [x[3] for x in res]

    res = []
    res[:] = map(list, medium_less_less)
    medium_less_less_duration = [x[1] for x in res]
    medium_less_less_cpu = [x[2] for x in res]
    medium_less_less_mem = [x[3] for x in res]

    res = []
    res[:] = map(list, medium_mid_less)
    medium_mid_less_duration = [x[1] for x in res]
    medium_mid_less_cpu = [x[2] for x in res]
    medium_mid_less_mem = [x[3] for x in res]

    res = []
    res[:] = map(list, long_mid_less)
    long_mid_less_duration = [x[1] for x in res]
    long_mid_less_cpu = [x[2] for x in res]
    long_mid_less_mem = [x[3] for x in res]


    ax = Axes3D(fig)

    short_less_less = ax.scatter(short_less_less_duration, short_less_less_cpu, short_less_less_mem, color='red', s = 1)
    short_mid_less = ax.scatter(short_mid_less_duration, short_mid_less_cpu, short_mid_less_mem, color='yellow', s = 1)
    short_mid_mid = ax.scatter(short_mid_mid_duration, short_mid_mid_cpu, short_mid_mid_mem, color='blue', s = 1)
    short_hungry_less = ax.scatter(short_hungry_less_duration, short_hungry_less_cpu, short_hungry_less_mem, color='green', s = 1)
    medium_less_less = ax.scatter(medium_less_less_duration, medium_less_less_cpu, medium_less_less_mem, color='pink', s = 1)
    medium_mid_less = ax.scatter(medium_mid_less_duration, medium_mid_less_cpu, medium_mid_less_mem, color='purple', s = 1)
    long_mid_less = ax.scatter(long_mid_less_duration, long_mid_less_cpu, long_mid_less_mem, color='black', s = 1)
    ax.legend((short_less_less,short_mid_less,short_mid_mid,short_hungry_less,medium_less_less,medium_mid_less,long_mid_less),
              (u'sll',u'sml',u'smm',u'shl',u'mll',u'mml','lml'), loc=2)
    # ax.scatter(x3, y3, z3, color='yellow', s= 1)
    ax.set_xlabel('job duration(hour)')
    ax.set_ylabel('average cpu(per job)')
    ax.set_zlabel('average memory(per job)')
    ax.set_xlim(0,1)
    # ax.view_init(elev=20., azim=-35)
    ax.grid(False)
    plt.savefig('../imgs_mysql/3d_category_job.png')
    plt.show()

if __name__ == '__main__':
    graph()
