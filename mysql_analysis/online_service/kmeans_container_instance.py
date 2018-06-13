import MySQLdb as mdb
import numpy as np
import matplotlib.pyplot as plt
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
        cursor.execute(
            "select t.instance_id, t.avg_cpu, t.avg_mem, t.avg_disk from (select instance_id, avg(cpu_util) as avg_cpu, avg(mem_util) as avg_mem, avg(disk_util) as avg_disk from container_usage where mem_util > 0 and cpu_util > 0 and disk_util > 0 group by instance_id) t where t.avg_cpu <= 17.0986 and avg_mem <= 34.2797 and avg_disk <= 22.8332")
        sss_instance = cursor.fetchall()
        sss_instance_list = list(sss_instance)
        sss_instance_id = [x[0] for x in sss_instance_list]
        sss_avg_cpu = [x[1] for x in sss_instance_list]
        sss_avg_mem = [x[2] for x in sss_instance_list]
        sss_avg_disk = [x[3] for x in sss_instance_list]

        cursor.execute(
            "select t.instance_id, t.avg_cpu, t.avg_mem, t.avg_disk from (select instance_id, avg(cpu_util) as avg_cpu, avg(mem_util) as avg_mem, avg(disk_util) as avg_disk from container_usage where mem_util > 0 and cpu_util > 0 and disk_util > 0 group by instance_id) t where t.avg_cpu <= 17.0986 and avg_mem <= 34.2797 and avg_disk > 22.8332")
        ssm_instance = cursor.fetchall()
        ssm_instance_list = list(ssm_instance)
        ssm_instance_id = [x[0] for x in ssm_instance_list]
        ssm_avg_cpu = [x[1] for x in ssm_instance_list]
        ssm_avg_mem = [x[2] for x in ssm_instance_list]
        ssm_avg_disk = [x[3] for x in ssm_instance_list]

        cursor.execute(
            "select t.instance_id, t.avg_cpu, t.avg_mem, t.avg_disk from (select instance_id, avg(cpu_util) as avg_cpu, avg(mem_util) as avg_mem, avg(disk_util) as avg_disk from container_usage where mem_util > 0 and cpu_util > 0 and disk_util > 0 group by instance_id) t where t.avg_cpu <= 17.0986 and avg_mem > 34.2797 and avg_mem <= 52.0417 and avg_disk <= 22.8332")
        sms_instance = cursor.fetchall()
        sms_instance_list = list(sms_instance)
        sms_instance_id = [x[0] for x in sms_instance_list]
        sms_avg_cpu = [x[1] for x in sms_instance_list]
        sms_avg_mem = [x[2] for x in sms_instance_list]
        sms_avg_disk = [x[3] for x in sms_instance_list]

        cursor.execute(
            "select t.instance_id, t.avg_cpu, t.avg_mem, t.avg_disk from (select instance_id, avg(cpu_util) as avg_cpu, avg(mem_util) as avg_mem, avg(disk_util) as avg_disk from container_usage where mem_util > 0 and cpu_util > 0 and disk_util > 0 group by instance_id) t where t.avg_cpu <= 17.0986 and avg_mem > 34.2797 and avg_mem <= 52.0417 and avg_disk > 22.8332")
        smm_instance = cursor.fetchall()
        smm_instance_list = list(smm_instance)
        smm_instance_id = [x[0] for x in smm_instance_list]
        smm_avg_cpu = [x[1] for x in smm_instance_list]
        smm_avg_mem = [x[2] for x in smm_instance_list]
        smm_avg_disk = [x[3] for x in smm_instance_list]

        cursor.execute(
            "select t.instance_id, t.avg_cpu, t.avg_mem, t.avg_disk from (select instance_id, avg(cpu_util) as avg_cpu, avg(mem_util) as avg_mem, avg(disk_util) as avg_disk from container_usage where mem_util > 0 and cpu_util > 0 and disk_util > 0 group by instance_id) t where t.avg_cpu <= 17.0986 and avg_mem > 52.0417 and avg_disk <= 22.8332")
        sls_instance = cursor.fetchall()
        sls_instance_list = list(sls_instance)
        sls_instance_id = [x[0] for x in sls_instance_list]
        sls_avg_cpu = [x[1] for x in sls_instance_list]
        sls_avg_mem = [x[2] for x in sls_instance_list]
        sls_avg_disk = [x[3] for x in sls_instance_list]

        cursor.execute(
            "select t.instance_id, t.avg_cpu, t.avg_mem, t.avg_disk from (select instance_id, avg(cpu_util) as avg_cpu, avg(mem_util) as avg_mem, avg(disk_util) as avg_disk from container_usage where mem_util > 0 and cpu_util > 0 and disk_util > 0 group by instance_id) t where t.avg_cpu <= 17.0986 and avg_mem > 52.0417 and avg_disk > 22.8332")
        slm_instance = cursor.fetchall()
        slm_instance_list = list(slm_instance)
        slm_instance_id = [x[0] for x in slm_instance_list]
        slm_avg_cpu = [x[1] for x in slm_instance_list]
        slm_avg_mem = [x[2] for x in slm_instance_list]
        slm_avg_disk = [x[3] for x in slm_instance_list]

        cursor.execute(
            "select t.instance_id, t.avg_cpu, t.avg_mem, t.avg_disk from (select instance_id, avg(cpu_util) as avg_cpu, avg(mem_util) as avg_mem, avg(disk_util) as avg_disk from container_usage where mem_util > 0 and cpu_util > 0 and disk_util > 0 group by instance_id) t where t.avg_cpu <= 17.0986 and avg_mem <= 34.2797 and avg_disk <= 22.8332")
        mss_instance = cursor.fetchall()
        mss_instance_list = list(mss_instance)
        mss_instance_id = [x[0] for x in mss_instance_list]
        mss_avg_cpu = [x[1] for x in mss_instance_list]
        mss_avg_mem = [x[2] for x in mss_instance_list]
        mss_avg_disk = [x[3] for x in mss_instance_list]

        cursor.execute(
            "select t.instance_id, t.avg_cpu, t.avg_mem, t.avg_disk from (select instance_id, avg(cpu_util) as avg_cpu, avg(mem_util) as avg_mem, avg(disk_util) as avg_disk from container_usage where mem_util > 0 and cpu_util > 0 and disk_util > 0 group by instance_id) t where t.avg_cpu > 17.0986 and avg_mem <= 34.2797 and avg_disk > 22.8332")
        msm_instance = cursor.fetchall()
        msm_instance_list = list(msm_instance)
        msm_instance_id = [x[0] for x in msm_instance_list]
        msm_avg_cpu = [x[1] for x in msm_instance_list]
        msm_avg_mem = [x[2] for x in msm_instance_list]
        msm_avg_disk = [x[3] for x in msm_instance_list]

        cursor.execute(
            "select t.instance_id, t.avg_cpu, t.avg_mem, t.avg_disk from (select instance_id, avg(cpu_util) as avg_cpu, avg(mem_util) as avg_mem, avg(disk_util) as avg_disk from container_usage where mem_util > 0 and cpu_util > 0 and disk_util > 0 group by instance_id) t where t.avg_cpu > 17.0986 and avg_mem > 34.2797 and avg_mem <= 52.0417 and avg_disk <= 22.8332")
        mms_instance = cursor.fetchall()
        mms_instance_list = list(mms_instance)
        mms_instance_id = [x[0] for x in mms_instance_list]
        mms_avg_cpu = [x[1] for x in mms_instance_list]
        mms_avg_mem = [x[2] for x in mms_instance_list]
        mms_avg_disk = [x[3] for x in mms_instance_list]

        cursor.execute(
            "select t.instance_id, t.avg_cpu, t.avg_mem, t.avg_disk from (select instance_id, avg(cpu_util) as avg_cpu, avg(mem_util) as avg_mem, avg(disk_util) as avg_disk from container_usage where mem_util > 0 and cpu_util > 0 and disk_util > 0 group by instance_id) t where t.avg_cpu > 17.0986 and avg_mem > 34.2797 and avg_mem <= 52.0417 and avg_disk > 22.8332")
        mmm_instance = cursor.fetchall()
        mmm_instance_list = list(mmm_instance)
        mmm_instance_id = [x[0] for x in mmm_instance_list]
        mmm_avg_cpu = [x[1] for x in mmm_instance_list]
        mmm_avg_mem = [x[2] for x in mmm_instance_list]
        mmm_avg_disk = [x[3] for x in mmm_instance_list]

        cursor.execute(
            "select t.instance_id, t.avg_cpu, t.avg_mem, t.avg_disk from (select instance_id, avg(cpu_util) as avg_cpu, avg(mem_util) as avg_mem, avg(disk_util) as avg_disk from container_usage where mem_util > 0 and cpu_util > 0 and disk_util > 0 group by instance_id) t where t.avg_cpu > 17.0986 and avg_mem > 52.0417 and avg_disk <= 22.8332")
        mls_instance = cursor.fetchall()
        mls_instance_list = list(mls_instance)
        mls_instance_id = [x[0] for x in mls_instance_list]
        mls_avg_cpu = [x[1] for x in mls_instance_list]
        mls_avg_mem = [x[2] for x in mls_instance_list]
        mls_avg_disk = [x[3] for x in mls_instance_list]

        cursor.execute(
            "select t.instance_id, t.avg_cpu, t.avg_mem, t.avg_disk from (select instance_id, avg(cpu_util) as avg_cpu, avg(mem_util) as avg_mem, avg(disk_util) as avg_disk from container_usage where mem_util > 0 and cpu_util > 0 and disk_util > 0 group by instance_id) t where t.avg_cpu > 17.0986 and avg_mem > 52.0417 and avg_disk > 22.8332")
        mlm_instance = cursor.fetchall()
        mlm_instance_list = list(mlm_instance)
        mlm_instance_id = [x[0] for x in mlm_instance_list]
        mlm_avg_cpu = [x[1] for x in mlm_instance_list]
        mlm_avg_mem = [x[2] for x in mlm_instance_list]
        mlm_avg_disk = [x[3] for x in mlm_instance_list]

        # 如果没有设置自动提交事务，则这里需要手动提交一次
        conn.commit()

        # instance_num = [len(sss_instance_id), len(ssm_instance_id), len(sms_instance_id), len(smm_instance_id),len(sls_instance_id), len(slm_instance_id),
        #                 len(mss_instance_id), len(msm_instance_id), len(mms_instance_id), len(mmm_instance_id), len(mls_instance_id), len(mlm_instance_id)]
        # instance_class = ['sss', 'ssm', 'sms', 'smm','sls','slm', 'mss', 'msm', 'mms', 'mmm','mls', 'mlm']
        # fig = plt.figure()
        # ax1 = fig.add_subplot(111)
        # ax1.bar( instance_class, instance_num, width=0.3)
        # for a, b in zip(instance_class, instance_num):
        #     ax1.text(a, b + 0.05, '%d' % b, ha='center', va='bottom', fontsize=11)
        # ax1.set_xlabel('category')
        # ax1.set_ylabel('instance number')
        # plt.savefig('../../imgs_mysql/bar_of_container_instance_1')
        # plt.show()

        fig = plt.figure()
        ax = Axes3D(fig)
        sss = ax.scatter(sss_avg_cpu, sss_avg_mem, sss_avg_disk, color='red', s = 1)
        ssm = ax.scatter(ssm_avg_cpu, ssm_avg_mem, ssm_avg_disk, color='blue', s = 1)
        sms = ax.scatter(sms_avg_cpu, sms_avg_mem, sms_avg_disk, color='#663300', s= 1)
        sls = ax.scatter(smm_avg_cpu, smm_avg_mem, smm_avg_disk, color='green', s= 1)
        slm = ax.scatter(sls_avg_cpu, sls_avg_mem, sls_avg_disk, color='#99ff66', s= 1)
        smm = ax.scatter(slm_avg_cpu, slm_avg_mem, slm_avg_disk, color='#663366', s= 1)
        mss = ax.scatter(mss_avg_cpu, mss_avg_mem, mss_avg_disk, color='yellow', s= 1)
        # msm = ax.scatter(msm_avg_cpu, msm_avg_mem, msm_avg_disk, color='pink', s= 1)
        mms = ax.scatter(mms_avg_cpu, mms_avg_mem, mms_avg_disk, color='orange', s= 1)
        mmm = ax.scatter(mmm_avg_cpu, mmm_avg_mem, mmm_avg_disk, color='purple', s= 1)
        mls = ax.scatter(mls_avg_cpu, mls_avg_mem, mls_avg_disk, color='#00cc66', s= 1)
        mlm = ax.scatter(mlm_avg_cpu, mlm_avg_mem, mlm_avg_disk, color='#DEB887', s= 1)
        ax.legend((sss, ssm, sms, smm, sls, slm, mss, mms, mmm, mls, mlm),(u'sss',u'ssm',u'sms',u'smm',u'sls',u'slm',u'mss',u'mms',u'mmm', u'mls',u'mlm'), loc='upper left')

        ax.set_xlabel('average cpu(%)')
        ax.set_ylabel('average memory(%)')
        ax.set_zlabel('average disk(%)')
        # ax.set_xlim(0,1)
        # ax.view_init(elev=20., azim=-35)
        ax.grid(False)
        plt.savefig('../../imgs_mysql/3d_kmeans_container_instance_category_1.png')
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
