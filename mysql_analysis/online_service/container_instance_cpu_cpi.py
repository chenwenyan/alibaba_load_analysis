import matplotlib.pyplot as plt
import MySQLdb as mdb
import numpy as np


def graph():
    # 连接数据库
    conn = mdb.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='alibaba_trace', charset='utf8')

    # 如果使用事务引擎，可以设置自动提交事务，或者在每次操作完成后手动提交事务conn.commit()
    conn.autocommit(1)  # conn.autocommit(True)

    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 因该模块底层其实是调用CAPI的，所以，需要先得到当前指向数据库的指针。

    try:
        cursor.execute(
            "SELECT instance_id, avg(avg_mpki) FROM container_usage WHERE instance_id IN ( SELECT instance_id FROM container_usage GROUP BY instance_id HAVING avg(cpu_util) <= 20 ) GROUP BY instance_id")
        records = cursor.fetchall()
        result = list(records)
        print(result)

        res = []
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        avg_cpi_less_20 = [x[1] for x in res]

        cursor.execute(
            "SELECT instance_id, avg(avg_mpki) FROM container_usage WHERE instance_id IN ( SELECT instance_id FROM container_usage GROUP BY instance_id HAVING avg(cpu_util) > 20 ) GROUP BY instance_id")
        records = cursor.fetchall()
        result = list(records)

        res = []
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        avg_cpi_more_20 = [x[1] for x in res]

        # 绘图
        fig = plt.figure(figsize=(7, 4))
        ax1 = fig.add_subplot(1, 2, 1)
        ax2 = fig.add_subplot(1, 2, 2)
        # ax1.set_xlabel("average cpi(avg cpu less 20)")
        ax1.set_xlabel("average mpki(avg cpu less 20)")
        # ax2.set_xlabel("average cpi(avg cpu more 20)")
        ax2.set_xlabel("average mpki(avg cpu more 20)")
        ax1.set_ylabel("portion of instance")
        # ax1.set_ylim(0, 1800)
        # ax2.set_ylim(0, 1800)
        ax1.hist(avg_cpi_less_20, bins=100, density=False, alpha=1.0, facecolor='g')
        ax2.hist(avg_cpi_more_20, bins=100, density=False, alpha=1.0, facecolor='g')

        # plt.savefig('../imgs_mysql/container_instance_cpi_portion.png')
        plt.savefig('../imgs_mysql/container_instance_mpki_portion.png')
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
