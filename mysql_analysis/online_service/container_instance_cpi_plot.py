import matplotlib
import matplotlib.pyplot as plt
import MySQLdb as mdb

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
            "select ts, avg(avg_cpi), avg(max_cpi), min(avg_cpi), avg(avg_mpki), avg(max_mpki), min(avg_mpki) from container_usage group by instance_id, ts ASC")
        # cursor.execute("SELECT t.ts, avg(t.avg_cpi), avg(t.max_cpi), avg(t.min_cpi), avg(t.avg_mpki), avg(t.max_mpki), avg(t.min_mpki) FROM ( SELECT ts, avg(avg_cpi) AS avg_cpi, avg(max_cpi) AS max_cpi, min(avg_cpi) AS min_cpi, avg(avg_mpki) AS avg_mpki, avg(max_mpki) AS max_mpki, min(avg_mpki) AS min_mpki FROM container_usage GROUP BY instance_id ) t GROUP BY t.ts")
        records = cursor.fetchall()
        result = list(records)
        # print(result)

        res = []
        res[:] = map(list, result)
        timestamp = [x[0] for x in res]
        avg_cpi = [x[1] for x in res]
        print(avg_cpi)
        print(max(avg_cpi))
        print(min(avg_cpi))
        max_cpi = [x[2] for x in res]
        print(max_cpi)
        print(max(max_cpi))
        print(min(max_cpi))
        min_cpi = [x[3] for x in res]
        print(min_cpi)
        print(max(min_cpi))
        print(min(min_cpi))
        avg_mpki = [x[4] for x in res]
        print(avg_mpki)
        print(max(avg_mpki))
        print(min(avg_mpki))
        max_mpki = [x[5] for x in res]
        print(max_mpki)
        print(max(max_mpki))
        print(min(max_mpki))
        min_mpki = [x[6] for x in res]
        print(min_mpki)
        print(max(min_mpki))
        print(min(min_mpki))
        timestamp = [x/3600 - 11 for x in timestamp]

        # 绘图
        fig = plt.figure(figsize=(9, 4))
        ax1 = fig.add_subplot(1, 2, 1)
        ax2 = fig.add_subplot(1, 2, 2)
        ax1.set_xlim(0, max(timestamp))
        ax2.set_xlim(0, max(timestamp))
        ax1.set_ylim(0, max(max_cpi) + 0.2)
        ax2.set_ylim(0, max(max_mpki) + 0.2)
        ax1.plot(timestamp, min_cpi, label="min")
        ax1.plot(timestamp, avg_cpi, label="avg")
        ax1.plot(timestamp, max_cpi, label="max")
        ax2.plot(timestamp, min_mpki, label="min")
        ax2.plot(timestamp, avg_mpki, label="avg")
        ax2.plot(timestamp, max_mpki, label="max")
        ax1.fill_between(timestamp, min_cpi, max_cpi, facecolor='#FFB6C1')
        ax2.fill_between(timestamp, min_mpki, max_mpki, facecolor='#FFB6C1')
        ax1.set_xlabel("time(hour)")
        ax2.set_xlabel("time(hour)")
        ax1.set_ylabel("average cpi of all instance")
        ax2.set_ylabel("average mpki of all instance")
        # matplotlib.rcParams['xtick.direction'] = 'in'
        ax1.legend(loc="best")
        ax2.legend(loc="best")

        # plt.savefig('../../imgs_mysql/container_instance_cpi_plot.png')
        plt.savefig('../../paper_img/avg_cpi_mpki.pdf')
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
