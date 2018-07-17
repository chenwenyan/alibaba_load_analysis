import matplotlib.pyplot as plt
import MySQLdb as mdb
import numpy as np
from mpl_toolkits.axes_grid1.inset_locator import inset_axes

def graph():
    # 连接数据库
    conn = mdb.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='alibaba_trace', charset='utf8')

    # 如果使用事务引擎，可以设置自动提交事务，或者在每次操作完成后手动提交事务conn.commit()
    conn.autocommit(1)  # conn.autocommit(True)

    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 因该模块底层其实是调用CAPI的，所以，需要先得到当前指向数据库的指针。
    list_records = []
    try:
        cursor.execute("select pattern, count(machineID) from workload_pattern group by pattern")
        records = cursor.fetchall()
        list_records = list(records)
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

    res = []
    res[:] = map(list, list_records)
    pattern = [x[0] for x in res]
    machine_number = [x[1] for x in res]

    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    # # 直方图
    ax1.bar(pattern, machine_number, width=0.2)
    ax1.set_xlabel('workload pattern')
    ax1.set_ylabel('machine number')
    for a,b in zip(pattern, machine_number):
        ax1.text(a, b+0.05, '%d' % b , ha='center', va= 'bottom',fontsize=8)
    # plt.setp(ax1.xaxis.get_majorticklabels(), rotation=90)
    plt.tight_layout()

    # plt.savefig("../../imgs_mysql/job_category_at_time")
    plt.savefig("../../paper_img/batch_job_workload_pattern_1.pdf")
    plt.show()

if __name__ == '__main__':
    graph()
