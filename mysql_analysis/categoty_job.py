import MySQLdb as mdb
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from mpl_toolkits.mplot3d import Axes3D
from matplotlib import colors


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
        # cursor.execute(
        #     "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='long_less_less'")
        # long_less_less = cursor.fetchall()
        # long_less_less = list(long_less_less)
        # cursor.execute(
        #     "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='long_less_mid'")
        # long_less_mid = cursor.fetchall()
        # long_less_mid = list(long_less_mid)
        # cursor.execute(
        #     "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='long_mid_less'")
        # long_mid_less = cursor.fetchall()
        # long_mid_less = list(long_mid_less)
        # cursor.execute(
        #     "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='long_mid_mid'")
        # long_mid_mid = cursor.fetchall()
        # long_mid_mid = list(long_mid_mid)
        # cursor.execute(
        #     "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='long_hungry_less'")
        # long_hungry_less = cursor.fetchall()
        # long_hungry_less = list(long_hungry_less)
        # cursor.execute(
        #     "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where category='long_hungry_mid'")
        # long_hungry_mid = cursor.fetchall()
        # long_hungry_mid = list(long_hungry_mid)



        # fig = plt.figure(figsize=(13,4))
        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        # x = ['sll', 'slm', 'sml', 'smm', 'shl', 'shm', 'mll',
        #      'mlm', 'mml', 'mmm', 'mhl', 'mhm', 'lll', 'llm',
        #      'lml', 'lmm', 'lhl', 'lhm']
        # x = np.arange(18)
        # y = [len(short_less_less), len(short_less_mid), len(short_mid_less), len(short_mid_mid), len(short_hungry_less),
        #      len(short_hungry_mid), len(medium_less_less),
        #      len(medium_less_mid), len(medium_mid_less), len(medium_mid_mid), len(medium_hungry_less),
        #      len(medium_hungry_mid), len(long_less_less), len(long_less_mid),
        #      len(long_mid_less), len(long_mid_mid), len(long_hungry_less), len(long_hungry_mid)]
        # x = ['sll', 'slm', 'sml', 'smm', 'shl', 'shm', 'mll', 'mlm', 'mml', 'mmm', 'mhl', 'mhm']
        # y = [len(short_less_less), len(short_less_mid), len(short_mid_less), len(short_mid_mid), len(short_hungry_less),
        #  len(short_hungry_mid), len(medium_less_less),
        #  len(medium_less_mid), len(medium_mid_less), len(medium_mid_mid), len(medium_hungry_less),
        #  len(medium_hungry_mid)]
        # # y = [i/100 for i in y]
        # ax1.bar(x, y)
        # for a,b in zip(x,y):
        #     ax1.text(a, b+0.05, '%d' % b , ha='center', va= 'bottom',fontsize=11)
        # ax1.set_xlabel('category')
        # ax1.set_ylabel('job number')
        # ax1.set_ylim(0, 3000)
        # plt.savefig('../imgs_mysql/hist_category_job.png')

        # 3d图形绘制
        # res = []
        # res[:] = map(list, short_less_less)
        # short_less_less_job = [x[0] for x in res]
        # short_less_less_duration = [x[1] for x in res]
        # short_less_less_cpu = [x[2] for x in res]
        # short_less_less_mem = [x[3] for x in res]
        #
        # res = []
        # res[:] = map(list, short_mid_less)
        # short_mid_less_duration = [x[1] for x in res]
        # short_mid_less_cpu = [x[2] for x in res]
        # short_mid_less_mem = [x[3] for x in res]
        #
        # res = []
        # res[:] = map(list, short_mid_mid)
        # short_mid_mid_duration = [x[1] for x in res]
        # short_mid_mid_cpu = [x[2] for x in res]
        # short_mid_mid_mem = [x[3] for x in res]
        #
        # res = []
        # res[:] = map(list, short_hungry_less)
        # short_hungry_less_duration = [x[1] for x in res]
        # short_hungry_less_cpu = [x[2] for x in res]
        # short_hungry_less_mem = [x[3] for x in res]
        #
        # res = []
        # res[:] = map(list, medium_less_less)
        # medium_less_less_duration = [x[1] for x in res]
        # medium_less_less_cpu = [x[2] for x in res]
        # medium_less_less_mem = [x[3] for x in res]
        #
        # res = []
        # res[:] = map(list, medium_mid_less)
        # medium_mid_less_duration = [x[1] for x in res]
        # medium_mid_less_cpu = [x[2] for x in res]
        # medium_mid_less_mem = [x[3] for x in res]

        # res = []
        # res[:] = map(list, long_mid_less)
        # long_mid_less_duration = [x[1] for x in res]
        # long_mid_less_cpu = [x[2] for x in res]
        # long_mid_less_mem = [x[3] for x in res]


        # ax = Axes3D(fig)
        # short_less_less = ax.scatter(short_less_less_duration, short_less_less_cpu, short_less_less_mem, color='red', s = 1)
        # short_mid_less = ax.scatter(short_mid_less_duration, short_mid_less_cpu, short_mid_less_mem, color='yellow', s = 1)
        # short_mid_mid = ax.scatter(short_mid_mid_duration, short_mid_mid_cpu, short_mid_mid_mem, color='blue', s = 1)
        # short_hungry_less = ax.scatter(short_hungry_less_duration, short_hungry_less_cpu, short_hungry_less_mem, color='green', s = 1)
        # medium_less_less = ax.scatter(medium_less_less_duration, medium_less_less_cpu, medium_less_less_mem, color='pink', s = 1)
        # medium_mid_less = ax.scatter(medium_mid_less_duration, medium_mid_less_cpu, medium_mid_less_mem, color='purple', s = 1)
        # # long_mid_less = ax.scatter(long_mid_less_duration, long_mid_less_cpu, long_mid_less_mem, color='black', s = 1)
        # ax.legend((short_less_less,short_mid_less,short_mid_mid,short_hungry_less,medium_less_less,medium_mid_less),
        #           (u'sll',u'sml',u'smm',u'shl',u'mll',u'mml'), loc=2)
        # # ax.scatter(x3, y3, z3, color='yellow', s= 1)
        # ax.set_xlabel('job duration(hour)')
        # ax.set_ylabel('average cpu(per job)')
        # ax.set_zlabel('average memory(per job)')
        # ax.set_xlim(0,1)
        # # ax.view_init(elev=20., azim=-35)
        # ax.grid(False)
        # plt.savefig('../imgs_mysql/3d_category_job.png')
        # plt.show()


        cursor.execute("select DISTINCT machineID from batch_instance where job_id in (select job_id from batch_job_category where category='short_less_less') group by machineID")
        sll_machineId = cursor.fetchall()
        print(sll_machineId)
        res = map(list, sll_machineId)
        sll_machineId = [x[0] for x in res]

        # cursor.execute("select DISTINCT machineID from batch_instance where job_id in (select job_id from batch_job_category where category='short_mid_less') group by machineID")
        # sml_machineId = cursor.fetchall()
        # print(sml_machineId)
        # res = map(list, sml_machineId)
        # sml_machineId = [x[0] for x in res]

        # cursor.execute("select machineID from batch_instance where job_id in (select job_id from batch_job_category where category='short_mid_mid') group by machineID")
        # smm_machineId = cursor.fetchall()
        # print(smm_machineId)
        # res = map(list, smm_machineId)
        # smm_machineId = [x[0] for x in res]

        # cursor.execute("select  machineID from batch_instance where job_id in (select job_id from batch_job_category where category='short_hungry_less') group by machineID")
        # shl_machineId = cursor.fetchall()
        # print(len(shl_machineId))
        # res = map(list, shl_machineId)
        # shl_machineId = [x[0] for x in res]

        # cursor.execute("select DISTINCT machineID from batch_instance where job_id in (select job_id from batch_job_category where category='medium_less_less') group by machineID")
        # mll_machineId = cursor.fetchall()
        # print(mll_machineId)
        # res = map(list, mll_machineId)
        # mll_machineId = [x[0] for x in res]

        # cursor.execute("select DISTINCT machineID from batch_instance where job_id in (select job_id from batch_job_category where category='medium_mid_less') group by machineID")
        # mml_machineId = cursor.fetchall()
        # print(mml_machineId)
        # res = map(list, mml_machineId)
        # mml_machineId = [x[0] for x in res]

        # cursor.execute("select DISTINCT machineID from batch_instance where job_id = (select job_id from batch_job_category where category='long_mid_less')")
        # lml_records = cursor.fetchall()
        # print(lml_records)
        # res[:] = map(list, lml_records)
        # lml_machineID = [x[0] for x in res]
        #
        # ax1 = fig.add_subplot(131)
        # ax2 = fig.add_subplot(132)
        # ax3 = fig.add_subplot(133)
        #
        # # 设置图表标题并给坐标轴加上标签
        # ax1.set_title("CPU utilization")
        # ax2.set_title("memory utilization")
        # ax3.set_title("disk utilization")
        # ax1.set_ylabel('machineID')
        # ax1.set_xlabel('time(hour)')
        # ax2.set_xlabel('time(hour)')
        # ax3.set_xlabel('time(hour)')
        # norm = colors.Normalize(vmin=0, vmax=100)
        # id_list = sorted(sll_machineId)
        # for i in id_list:
        #     cursor.execute("select machineID, time_stamp, cpu, memory, disk from server_usage where machineID = (%d)" % (i))
        #     records = cursor.fetchall()
        #     res = []
        #     res[:] = map(list, list(records))
        #     machineID = [x[0] for x in res]
        #     time_stamp = [x[1] for x in res]
        #     cpu = [x[2] for x in res]
        #     mem = [x[3] for x in res]
        #     disk = [x[4] for x in res]
        #     times = [(x / 3600 - 11) for x in time_stamp]
        #     ax1.scatter(times, machineID, c=cpu, norm=norm, alpha=0.5, s=2.0)
        #     ax2.scatter(times, machineID, c=mem, norm=norm, alpha=0.5, s=2.0)
        #     ax3.scatter(times, machineID, c=disk, norm=norm, alpha=0.5, s=2.0)
        # # 绘制渐变色标注
        # gci = plt.scatter(times, machineID, c=cpu, norm=norm, alpha=0.5, s=2.0)
        # cbar = plt.colorbar(gci)
        # cbar.set_label('used')
        # cbar.set_ticks(np.linspace(0, 100, 6))
        # ax1.set_xlim(0, max(times))
        # ax1.set_ylim(0, max(id_list))
        # ax2.set_xlim(0, max(times))
        # ax2.set_ylim(0, max(id_list))
        # ax3.set_xlim(0, max(times))
        # ax3.set_ylim(0, max(id_list))
        # 保存图片
        # plt.savefig('../imgs_mysql/lml_resource.png')
        # plt.savefig('../imgs_mysql/mml_resource.png')
        # plt.savefig('../imgs_mysql/mll_resource.png')
        # plt.savefig('../imgs_mysql/smm_resource.png')
        # plt.savefig('../imgs_mysql/sml_resource.png')
        # plt.savefig('../imgs_mysql/sll_resource.png')

        plt.show()

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

if __name__ == '__main__':
    graph()
