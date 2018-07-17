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
        cursor.execute(
            "SELECT instance_id, avg(avg_cpi), avg(avg_mpki), avg(cpu_util), avg(mem_util), avg(disk_util) FROM container_usage WHERE instance_id IN ( SELECT t.instance_id FROM ( SELECT instance_id, avg(cpu_util) AS avg_cpu, avg(mem_util) AS avg_mem, avg(disk_util) AS avg_disk FROM container_usage WHERE mem_util > 0 AND cpu_util > 0 AND disk_util > 0 GROUP BY instance_id ) t WHERE t.avg_cpu <= 17.0986 AND t.avg_mem <= 45.5775 AND t.avg_disk <= 22.8332 ) GROUP BY instance_id")
        sss_instance = cursor.fetchall()
        sss_instance_list = list(sss_instance)
        sss_instance_id = [x[0] for x in sss_instance_list]
        sss_avg_cpi = [x[1] for x in sss_instance_list]
        sss_avg_mpki = [x[2] for x in sss_instance_list]
        sss_avg_cpu = [x[3] for x in sss_instance_list]
        sss_avg_mem = [x[4] for x in sss_instance_list]
        sss_avg_disk = [x[5] for x in sss_instance_list]

        cursor.execute(
            "SELECT instance_id, avg(avg_cpi), avg(avg_mpki), avg(cpu_util), avg(mem_util), avg(disk_util) FROM container_usage WHERE instance_id IN ( SELECT t.instance_id FROM ( SELECT instance_id, avg(cpu_util) AS avg_cpu, avg(mem_util) AS avg_mem, avg(disk_util) AS avg_disk FROM container_usage WHERE mem_util > 0 AND cpu_util > 0 AND disk_util > 0 GROUP BY instance_id ) t WHERE t.avg_cpu <= 17.0986 AND t.avg_mem <= 45.5775  AND t.avg_disk > 22.8332 ) group by instance_id")
        ssm_instance = cursor.fetchall()
        ssm_instance_list = list(ssm_instance)
        ssm_instance_id = [x[0] for x in ssm_instance_list]
        ssm_avg_cpi = [x[1] for x in ssm_instance_list]
        ssm_avg_mpki = [x[2] for x in ssm_instance_list]
        ssm_avg_cpu = [x[3] for x in ssm_instance_list]
        ssm_avg_mem = [x[4] for x in ssm_instance_list]
        ssm_avg_disk = [x[5] for x in ssm_instance_list]

        cursor.execute(
            "SELECT instance_id, avg(avg_cpi), avg(avg_mpki), avg(cpu_util), avg(mem_util), avg(disk_util) FROM container_usage WHERE instance_id IN ( SELECT t.instance_id FROM ( SELECT instance_id, avg(cpu_util) AS avg_cpu, avg(mem_util) AS avg_mem, avg(disk_util) AS avg_disk FROM container_usage WHERE mem_util > 0 AND cpu_util > 0 AND disk_util > 0 GROUP BY instance_id ) t WHERE t.avg_cpu <= 17.0986 AND t.avg_mem > 45.5775 AND t.avg_disk < 22.8332 ) group by instance_id")
        sms_instance = cursor.fetchall()
        sms_instance_list = list(sms_instance)
        sms_instance_id = [x[0] for x in sms_instance_list]
        sms_avg_cpi = [x[1] for x in sms_instance_list]
        sms_avg_mpki = [x[2] for x in sms_instance_list]
        sms_avg_cpu = [x[3] for x in sms_instance_list]
        sms_avg_mem = [x[4] for x in sms_instance_list]
        sms_avg_disk = [x[5] for x in sms_instance_list]

        cursor.execute(
            "SELECT instance_id, avg(avg_cpi), avg(avg_mpki), avg(cpu_util), avg(mem_util), avg(disk_util) FROM container_usage WHERE instance_id IN ( SELECT t.instance_id FROM ( SELECT instance_id, avg(cpu_util) AS avg_cpu, avg(mem_util) AS avg_mem, avg(disk_util) AS avg_disk FROM container_usage WHERE mem_util > 0 AND cpu_util > 0 AND disk_util > 0 GROUP BY instance_id ) t WHERE t.avg_cpu <= 17.0986 AND t.avg_mem > 45.5775 AND t.avg_disk > 22.8332 ) group by instance_id")
        smm_instance = cursor.fetchall()
        smm_instance_list = list(smm_instance)
        smm_instance_id = [x[0] for x in smm_instance_list]
        smm_avg_cpi = [x[1] for x in smm_instance_list]
        smm_avg_mpki = [x[2] for x in smm_instance_list]
        smm_avg_cpu = [x[3] for x in smm_instance_list]
        smm_avg_mem = [x[4] for x in smm_instance_list]
        smm_avg_disk = [x[5] for x in smm_instance_list]

        cursor.execute(
            "SELECT instance_id, avg(avg_cpi), avg(avg_mpki), avg(cpu_util), avg(mem_util), avg(disk_util) FROM container_usage WHERE instance_id IN ( SELECT t.instance_id FROM ( SELECT instance_id, avg(cpu_util) AS avg_cpu, avg(mem_util) AS avg_mem, avg(disk_util) AS avg_disk FROM container_usage WHERE mem_util > 0 AND cpu_util > 0 AND disk_util > 0 GROUP BY instance_id ) t WHERE t.avg_cpu > 17.0986 AND t.avg_mem <= 45.5775 AND t.avg_disk <= 22.8332 ) group by instance_id")
        mss_instance = cursor.fetchall()
        mss_instance_list = list(mss_instance)
        mss_instance_id = [x[0] for x in mss_instance_list]
        mss_avg_cpi = [x[1] for x in mss_instance_list]
        mss_avg_mpki = [x[2] for x in mss_instance_list]
        mss_avg_cpu = [x[3] for x in mss_instance_list]
        mss_avg_mem = [x[4] for x in mss_instance_list]
        mss_avg_disk = [x[5] for x in mss_instance_list]

        cursor.execute(
            "SELECT instance_id, avg(avg_cpi), avg(avg_mpki), avg(cpu_util), avg(mem_util), avg(disk_util) FROM container_usage WHERE instance_id IN ( SELECT t.instance_id FROM ( SELECT instance_id, avg(cpu_util) AS avg_cpu, avg(mem_util) AS avg_mem, avg(disk_util) AS avg_disk FROM container_usage WHERE mem_util > 0 AND cpu_util > 0 AND disk_util > 0 GROUP BY instance_id ) t WHERE t.avg_cpu > 17.0986 AND t.avg_mem <= 45.5775 AND t.avg_disk > 22.8332 ) group by instance_id")
        msm_instance = cursor.fetchall()
        msm_instance_list = list(msm_instance)
        msm_instance_id = [x[0] for x in msm_instance_list]
        msm_avg_cpi = [x[1] for x in msm_instance_list]
        msm_avg_mpki = [x[2] for x in msm_instance_list]
        msm_avg_cpu = [x[3] for x in msm_instance_list]
        msm_avg_mem = [x[4] for x in msm_instance_list]
        msm_avg_disk = [x[5] for x in msm_instance_list]

        cursor.execute(
            "SELECT instance_id, avg(avg_cpi), avg(avg_mpki), avg(cpu_util), avg(mem_util), avg(disk_util) FROM container_usage WHERE instance_id IN ( SELECT t.instance_id FROM ( SELECT instance_id, avg(cpu_util) AS avg_cpu, avg(mem_util) AS avg_mem, avg(disk_util) AS avg_disk FROM container_usage WHERE mem_util > 0 AND cpu_util > 0 AND disk_util > 0 GROUP BY instance_id ) t WHERE t.avg_cpu > 17.0986 AND t.avg_mem > 45.5775 AND t.avg_disk < 22.8332 ) group by instance_id")
        mms_instance = cursor.fetchall()
        mms_instance_list = list(mms_instance)
        mms_instance_id = [x[0] for x in mms_instance_list]
        mms_avg_cpi = [x[1] for x in mms_instance_list]
        mms_avg_mpki = [x[2] for x in mms_instance_list]
        mms_avg_cpu = [x[3] for x in mms_instance_list]
        mms_avg_mem = [x[4] for x in mms_instance_list]
        mms_avg_disk = [x[5] for x in mms_instance_list]

        cursor.execute(
            "SELECT instance_id, avg(avg_cpi), avg(avg_mpki), avg(cpu_util), avg(mem_util), avg(disk_util) FROM container_usage WHERE instance_id IN ( SELECT t.instance_id FROM ( SELECT instance_id, avg(cpu_util) AS avg_cpu, avg(mem_util) AS avg_mem, avg(disk_util) AS avg_disk FROM container_usage WHERE mem_util > 0 AND cpu_util > 0 AND disk_util > 0 GROUP BY instance_id ) t WHERE t.avg_cpu > 17.0986 AND t.avg_mem > 45.5775 AND t.avg_disk > 22.8332 ) group by instance_id")
        mmm_instance = cursor.fetchall()
        mmm_instance_list = list(mmm_instance)
        mmm_instance_id = [x[0] for x in mmm_instance_list]
        mmm_avg_cpi = [x[1] for x in mmm_instance_list]
        mmm_avg_mpki = [x[2] for x in mmm_instance_list]
        mmm_avg_cpu = [x[3] for x in mmm_instance_list]
        mmm_avg_mem = [x[4] for x in mmm_instance_list]
        mmm_avg_disk = [x[5] for x in mmm_instance_list]

        # 如果没有设置自动提交事务，则这里需要手动提交一次
        conn.commit()

        fig = plt.figure(figsize=(11, 6))
        ax1 = fig.add_subplot(241)
        ax2 = fig.add_subplot(242, sharex=ax1, sharey=ax1)
        ax3 = fig.add_subplot(243, sharex=ax1, sharey=ax1)
        ax4 = fig.add_subplot(244, sharex=ax1, sharey=ax1)
        ax5 = fig.add_subplot(245, sharex=ax1, sharey=ax1)
        ax6 = fig.add_subplot(246, sharex=ax1, sharey=ax1)
        ax7 = fig.add_subplot(247, sharex=ax1, sharey=ax1)
        ax8 = fig.add_subplot(248, sharex=ax1, sharey=ax1)
        ax1.set_xlim(0, 1.0)
        ax1.set_yscale('log')

        # ax1.hist(sss_avg_cpi, density=False, alpha=1.0, bins=100)
        # ax2.hist(ssm_avg_cpi, density=False, alpha=1.0, bins=100)
        # ax3.hist(sms_avg_cpi, density=False, alpha=1.0, bins=100)
        # ax4.hist(smm_avg_cpi, density=False, alpha=1.0, bins=100)
        # ax5.hist(mss_avg_cpi, density=False, alpha=1.0, bins=100)
        # ax6.hist(msm_avg_cpi, density=False, alpha=1.0, bins=100)
        # ax7.hist(mms_avg_cpi, density=False, alpha=1.0, bins=100)
        # ax8.hist(mmm_avg_cpi, density=False, alpha=1.0, bins=100)

        ax1.hist(sss_avg_mpki, density=False, alpha=1.0, bins=100)
        ax2.hist(ssm_avg_mpki, density=False, alpha=1.0, bins=100)
        ax3.hist(sms_avg_mpki, density=False, alpha=1.0, bins=100)
        ax4.hist(smm_avg_mpki, density=False, alpha=1.0, bins=100)
        ax5.hist(mss_avg_mpki, density=False, alpha=1.0, bins=100)
        ax6.hist(msm_avg_mpki, density=False, alpha=1.0)
        ax7.hist(mms_avg_mpki, density=False, alpha=1.0, bins=100)
        ax8.hist(mmm_avg_mpki, density=False, alpha=1.0, bins=100)

        ax1.set_title('sss')
        ax2.set_title('ssm')
        ax3.set_title('sms')
        ax4.set_title('smm')
        ax5.set_title('mss')
        ax6.set_title('msm')
        ax7.set_title('mms')
        ax8.set_title('mmm')
        ax1.set_ylabel('instance number')
        ax5.set_ylabel('instance number')
        # ax6.set_xlabel("average cpi for container instance category")
        ax6.set_xlabel("average mpki for container instance group")
        # plt.savefig('../../imgs_mysql/container_instance_category_cpi')
        # plt.savefig('../../imgs_mysql/online_service_group_cpi.pdf')
        # plt.savefig('../../imgs_mysql/container_instance_category_mpki')
        plt.savefig('../../imgs_mysql/online_service_group_mpki.pdf')
        plt.show()

        # CDF
        # fig = plt.figure()
        # ax1 = fig.add_subplot(111)
        # hist, bin_edges = np.histogram(sss_avg_cpi, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='sss')
        # hist, bin_edges = np.histogram(ssm_avg_cpi, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='ssm')
        # hist, bin_edges = np.histogram(sms_avg_cpi, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='sms')
        # hist, bin_edges = np.histogram(smm_avg_cpi, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='smm')
        # hist, bin_edges = np.histogram(mss_avg_cpi, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='mss')
        # hist, bin_edges = np.histogram(msm_avg_cpi, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='msm')
        # hist, bin_edges = np.histogram(mms_avg_cpi, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='mms')
        # hist, bin_edges = np.histogram(mmm_avg_cpi, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='mmm')
        # ax1.legend(loc='best')
        # ax1.set_xlabel("average cpi")
        # ax1.set_ylabel("portion of instance")
        # plt.savefig("../../imgs_mysql/cdf_container_instance_category_cpi")
        # plt.show()

        # hist, bin_edges = np.histogram(sss_avg_mpki, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='sss')
        # hist, bin_edges = np.histogram(ssm_avg_mpki, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='ssm')
        # hist, bin_edges = np.histogram(sms_avg_mpki, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='sms')
        # hist, bin_edges = np.histogram(smm_avg_mpki, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='smm')
        # hist, bin_edges = np.histogram(mss_avg_mpki, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='mss')
        # hist, bin_edges = np.histogram(msm_avg_mpki, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='msm')
        # hist, bin_edges = np.histogram(mms_avg_mpki, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='mms')
        # hist, bin_edges = np.histogram(mmm_avg_mpki, bins=100)
        # cdf = np.cumsum(hist / sum(hist))
        # ax1.plot(bin_edges[1:], cdf, label='mmm')
        # ax1.legend(loc='best')
        # ax1.set_xlabel("average mpki")
        # ax1.set_ylabel("portion of instance")
        # plt.savefig("../../imgs_mysql/cdf_container_instance_category_mpki")
        # plt.show()
        #
        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        hist, bin_edges = np.histogram(sss_avg_mpki, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='sss')
        hist, bin_edges = np.histogram(ssm_avg_cpu, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='ssm')
        hist, bin_edges = np.histogram(sms_avg_cpu, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='sms')
        hist, bin_edges = np.histogram(smm_avg_cpu, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='smm')
        hist, bin_edges = np.histogram(mss_avg_cpu, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='mss')
        hist, bin_edges = np.histogram(msm_avg_cpu, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='msm')
        hist, bin_edges = np.histogram(mms_avg_cpu, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='mms')
        hist, bin_edges = np.histogram(mmm_avg_cpu, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='mmm')
        ax1.legend(loc='best')
        ax1.set_xlabel("cpu utilization(%)")
        ax1.set_ylabel("portion of instance")
        # plt.savefig("../../imgs_mysql/cdf_container_instance_category_cpu")
        plt.savefig("../../paper_img/cdf_avg_cpu_online_service.pdf")
        plt.show()

        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        hist, bin_edges = np.histogram(sss_avg_mem, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='sss')
        hist, bin_edges = np.histogram(ssm_avg_mem, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='ssm')
        hist, bin_edges = np.histogram(sms_avg_mem, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='sms')
        hist, bin_edges = np.histogram(smm_avg_mem, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='smm')
        hist, bin_edges = np.histogram(mss_avg_mem, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='mss')
        hist, bin_edges = np.histogram(msm_avg_mem, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='msm')
        hist, bin_edges = np.histogram(mms_avg_mem, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='mms')
        hist, bin_edges = np.histogram(mmm_avg_mem, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='mmm')
        ax1.legend(loc='best')
        ax1.set_xlabel("memory utilization(%)")
        ax1.set_ylabel("portion of instance")
        # plt.savefig("../../imgs_mysql/cdf_container_instance_category_mem")
        plt.savefig("../../paper_img/cdf_avg_memory_online_service.pdf")
        plt.show()

        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        hist, bin_edges = np.histogram(sss_avg_disk, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='sss')
        hist, bin_edges = np.histogram(ssm_avg_disk, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='ssm')
        hist, bin_edges = np.histogram(sms_avg_disk, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='sms')
        hist, bin_edges = np.histogram(smm_avg_disk, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='smm')
        hist, bin_edges = np.histogram(mss_avg_disk, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='mss')
        hist, bin_edges = np.histogram(msm_avg_disk, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='msm')
        hist, bin_edges = np.histogram(mms_avg_disk, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='mms')
        hist, bin_edges = np.histogram(mmm_avg_disk, bins=100)
        cdf = np.cumsum(hist / sum(hist))
        ax1.plot(bin_edges[1:], cdf, label='mmm')
        ax1.legend(loc='best')
        ax1.set_xlabel("disk utilization(%)")
        ax1.set_ylabel("portion of instance")
        # plt.savefig("../../imgs_mysql/cdf_container_instance_category_disk")
        plt.savefig("../../paper_img/cdf_avg_disk_online_service.pdf")
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
