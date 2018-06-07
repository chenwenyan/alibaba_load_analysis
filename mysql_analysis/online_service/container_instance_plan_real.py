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
            "select instance_id, plan_cpu, plan_mem, plan_disk from container_event WHERE instance_id in (select instance_id from container_event GROUP BY instance_id HAVING count(instance_id) = 1) and instance_id in (select instance_id from container_usage) order by instance_id ")
        records = cursor.fetchall()
        result = list(records)
        print(result)

        res = []
        res[:] = map(list, result)
        plan_instance_ids = [x[0] for x in res]
        plan_cpu = [x[1] / 64 * 100 for x in res]
        plan_mem = [x[2]*100 for x in res]
        plan_disk = [x[3]*100 for x in res]

        cursor.execute(
            "select instance_id, avg(cpu_util),avg(mem_util), avg(disk_util) from container_usage group by instance_id HAVING instance_id in (select instance_id from container_event GROUP BY instance_id HAVING count(instance_id) = 1)  order by instance_id ")
        records = cursor.fetchall()
        result = list(records)

        res = []
        res[:] = map(list, result)
        real_instance_ids = [x[0] for x in res]
        avg_cpu = [x[1] for x in res]
        avg_mem = [x[2] for x in res]
        avg_disk = [x[3] for x in res]

        # 绘图
        fig = plt.figure(figsize=(15, 4))
        ax1 = fig.add_subplot(1, 3, 1)
        ax2 = fig.add_subplot(1, 3, 2)
        ax3 = fig.add_subplot(1, 3, 3)
        ax1.set_ylabel("cpu utilization(%)")
        ax2.set_ylabel("memory utilization(%)")
        ax3.set_ylabel("disk utilization(%)")
        ax1.set_xlabel("instance id")
        ax2.set_xlabel("instance id")
        ax2.set_xlabel("instance id")
        ax1.set_ylim(0, 110)
        ax2.set_ylim(0, 110)
        ax3.set_ylim(0, 110)
        ax1.plot(real_instance_ids, plan_cpu, label="plan")
        ax1.plot(real_instance_ids, avg_cpu, label="real", alpha=0.5)
        ax2.plot(real_instance_ids, plan_mem, label="plan")
        ax2.plot(real_instance_ids, avg_mem, label="real", alpha=0.5)
        ax3.plot(real_instance_ids, plan_disk, label="plan")
        ax3.plot(real_instance_ids, avg_disk, label="real", alpha=0.5)
        ax1.legend(loc="best")
        ax2.legend(loc="best")
        ax3.legend(loc="best")

        plt.savefig('../imgs_mysql/container_instance_plan_real.png')
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
