import MySQLdb as mdb
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from mpl_toolkits.mplot3d import Axes3D


# 自定义归一化方法
def MaxMinNormalization(list):
    Max = np.max(list)
    Min = np.min(list)
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
        sss_job_res = cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where job_duration <= 1.1534 and avg_cpu <= 0.6238 and avg_mem > 0 and avg_mem <= 0.0621")
        sss_job_res = cursor.fetchall()
        sss_job_list = list(sss_job_res)
        sss_job = [x[0] for x in sss_job_list]
        sss_job_duration = [x[1] for x in sss_job_list]
        sss_avg_cpu = [x[2] for x in sss_job_list]
        sss_avg_mem = [x[3] for x in sss_job_list]

        # for i in range(len(sss_job_id)):
        #     cursor.execute("update batch_job_category set category_1 = 'sss' where job_id = (%d)" % (sss_job_id[i]))

        ssm_job = cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where job_duration <= 1.1534 and avg_cpu <= 0.6238 and avg_mem > 0.0621")
        ssm_job = cursor.fetchall()
        ssm_job_list = list(ssm_job)
        ssm_job_id = [x[0] for x in ssm_job_list]
        ssm_job_duration = [x[1] for x in ssm_job_list]
        ssm_avg_cpu = [x[2] for x in ssm_job_list]
        ssm_avg_mem = [x[3] for x in ssm_job_list]


        sms_job = cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where job_duration <= 1.1534 and avg_cpu > 0.6238 and avg_mem > 0 and avg_mem <= 0.0621")
        sms_job = cursor.fetchall()
        sms_job_list = list(sms_job)
        sms_job_id = [x[0] for x in sms_job_list]
        sms_job_duration = [x[1] for x in sms_job_list]
        sms_avg_cpu = [x[2] for x in sms_job_list]
        sms_avg_mem = [x[3] for x in sms_job_list]

        # for i in range(len(sms_job_id)):
        #     cursor.execute("update batch_job_category set category_1 = 'sms' where job_id = (%d)" % (sms_job_id[i]))

        smm_job = cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where job_duration <= 1.1534 and avg_cpu > 0.6238 and avg_mem > 0.0621")
        smm_job = cursor.fetchall()
        smm_job = list(smm_job)
        smm_job_duration = [x[0] for x in smm_job]
        smm_avg_cpu = [x[1] for x in smm_job]
        smm_avg_mem = [x[2] for x in smm_job]

        mss_job = cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where job_duration > 1.1534 and avg_cpu <= 0.6238 and avg_mem > 0 and avg_mem <= 0.0621")
        mss_job = cursor.fetchall()
        mss_job_list = list(mss_job)
        mss_job_id = [x[0] for x in mss_job_list]
        mss_job_duration = [x[1] for x in mss_job_list]
        mss_avg_cpu = [x[2] for x in mss_job_list]
        mss_avg_mem = [x[3] for x in mss_job_list]

        # for i in range(len(mss_job_id)):
        #     cursor.execute("update batch_job_category set category_1 = 'mss' where job_id = (%d)" % (mss_job_id[i]))

        msm_job = cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where job_duration > 1.1534 and avg_cpu <= 0.6238 and avg_mem > 0.0621")
        msm_job = cursor.fetchall()
        msm_job = list(msm_job)
        msm_job_duration = [x[0] for x in msm_job]
        msm_avg_cpu = [x[1] for x in msm_job]
        msm_avg_mem = [x[2] for x in msm_job]

        mms_job = cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where job_duration > 1.1534 and avg_cpu > 0.6238 and avg_mem > 0 and avg_mem <= 0.0621")
        mms_job = cursor.fetchall()
        mms_job_list = list(mms_job)
        mms_job_id = [x[0] for x in mms_job_list]
        mms_job_duration = [x[1] for x in mms_job_list]
        mms_avg_cpu = [x[2] for x in mms_job_list]
        mms_avg_mem = [x[3] for x in mms_job_list]

        # for i in range(len(mms_job_id)):
        #     cursor.execute("update batch_job_category set category_1 = 'mms' where job_id = (%d)" % (mms_job_id[i]))

        mmm_job = cursor.execute(
            "select job_id, job_duration, avg_cpu, avg_mem from batch_job_category where job_duration > 1.1534 and avg_cpu > 0.6238 and avg_mem > 0.0621")
        mmm_job = cursor.fetchall()
        mmm_job = list(mmm_job)
        mmm_job_duration = [x[0] for x in mmm_job]
        mmm_avg_cpu = [x[1] for x in mmm_job]
        mmm_avg_mem = [x[2] for x in mmm_job]

        # 如果没有设置自动提交事务，则这里需要手动提交一次
        conn.commit()

        job_num = [len(sss_job), len(ssm_job), len(sms_job), len(smm_job),
                   len(mss_job), len(msm_job), len(mms_job), len(mmm_job)]
        job_class = ['sss','ssm','sms','smm','mss','msm','mms','mmm']
        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        ax1.bar(job_class, job_num, width=0.3)
        for a,b in zip(job_class,job_num):
            ax1.text(a, b+0.05, '%d' % b , ha='center', va= 'bottom',fontsize=11)
        ax1.set_xlabel('group')
        ax1.set_ylabel('job number')
        # plt.savefig('../../imgs_mysql/bar_of_batch_job')
        plt.savefig('../../paper_img/batch_job_group', dpi=2000)
        plt.show()

        # fig = plt.figure()
        # ax = Axes3D(fig)
        # x1 = MaxMinNormalization(sss_job_duration)
        # x2 = MaxMinNormalization(sms_job_duration)
        # x3 = MaxMinNormalization(mss_job_duration)
        # x4 = MaxMinNormalization(mms_job_duration)
        # y1 = MaxMinNormalization(sss_avg_cpu)
        # y2 = MaxMinNormalization(sms_avg_cpu)
        # y3 = MaxMinNormalization(mss_avg_cpu)
        # y4 = MaxMinNormalization(mms_avg_cpu)
        # z1 = sss_avg_mem
        # z2 = sms_avg_mem
        # z3 = mss_avg_mem
        # z4 = mms_avg_cpu
        # sss = ax.scatter(x1, y1, z1, color='red', s = 1)
        # sms = ax.scatter(x2, y2, z2, color='blue', s = 1)
        # mss = ax.scatter(x3, y3, z3, color='black', s= 1)
        # mms = ax.scatter(x4, y4, z4, color='green', s= 1)
        # ax.legend((sss, sms, mss, mms),(u'sss',u'sms',u'mss',u'mms'), loc='upper left')
        #
        # ax.set_xlabel('norm job duration')
        # ax.set_ylabel('average cpu(per job)')
        # ax.set_zlabel('average memory(per job)')
        # # ax.set_xlim(0,1)
        # # ax.view_init(elev=20., azim=-35)
        # ax.grid(False)
        # plt.savefig('../../imgs_mysql/3d_kmeans_job_category.png')
        # plt.show()

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
