import matplotlib.pyplot as plt
import MySQLdb as mdb
from matplotlib import colors


def graph():
    # 连接数据库
    conn = mdb.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='alibaba_trace', charset='utf8')

    # 如果使用事务引擎，可以设置自动提交事务，或者在每次操作完成后手动提交事务conn.commit()
    conn.autocommit(1)  # conn.autocommit(True)

    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 因该模块底层其实是调用CAPI的，所以，需要先得到当前指向数据库的指针。

    try:
        cursor.execute("select DISTINCT instance_id from container_usage")
        records = cursor.fetchall()
        result = list(records)
        print(result)

        # 绘图
        fig = plt.figure(figsize=(20, 4))
        ax1 = fig.add_subplot(1, 3, 1)
        ax2 = fig.add_subplot(1, 3, 2)
        ax3 = fig.add_subplot(1, 3, 3)
        ax1.set_title('cpu utilization')
        ax2.set_title('memory utilization')
        ax3.set_title('disk utilization')
        ax1.set_xlabel("time(hour)")
        ax2.set_xlabel("time(hour)")
        ax3.set_xlabel("time(hour)")
        ax1.set_ylabel('instance id')
        norm = colors.Normalize(vmin=0, vmax=50)

        res = []
        res[:] = map(list, result)
        instance_ids = [x[0] for x in res]
        instance_ids = sorted(instance_ids)
        ax1.set_xlim(0, 12)
        ax2.set_xlim(0, 12)
        ax3.set_xlim(0, 12)
        ax1.set_ylim(0, max(instance_ids))
        ax2.set_ylim(0, max(instance_ids))
        ax3.set_ylim(0, max(instance_ids))
        for i in instance_ids:
            # if i > 10000 and i <= 12000:
            # if i <= 2000:
                print(i)
                cursor.execute(
                    "select instance_id, ts, cpu_util, mem_util, disk_util from container_usage where instance_id = (%d)" % (
                        i))
                result = cursor.fetchall()
                result = list(result)
                res[:] = map(list, result)
                instance_id = [x[0] for x in res]
                time_stamp = [x[1] for x in res]
                cpu_util = [x[2] for x in res]
                # print(cpu_util)
                mem_util = [x[3] for x in res]
                # print(mem_util)
                disk_util = [x[4] for x in res]
                # print(disk_util)
                timestamp = [(x / 3600 - 11) for x in time_stamp]
                timestamp = sorted(timestamp)
                # ax1.plot(timestamp, cpu_util)
                # ax2.plot(timestamp, mem_util)
                # ax3.plot(timestamp, disk_util)
                ax1.scatter(timestamp, instance_id, c=cpu_util, norm=norm, alpha=0.5, s=0.1)
                ax2.scatter(timestamp, instance_id, c=mem_util, norm=norm, alpha=0.5, s=0.1)
                ax3.scatter(timestamp, instance_id, c=disk_util, norm=norm, alpha=0.5, s=0.1)
        # 绘制渐变色标注
        gci = plt.scatter(timestamp, instance_id, c=cpu_util, norm=norm, alpha=0.5, s=0.001)
        cbar = plt.colorbar(gci)
        cbar.set_label('used')
        plt.savefig('../imgs_mysql/container_usage.png')
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
