import os
import time
import pandas as pd
import data_read as rd
import main as mi

time0 = time.asctime()

max_day, max_hour, max_minute = 32, 24, 60  # max_day从1开始.

# =========================初始化文件及日志目录==============================
"""
记录异常数据目录
"""
path_data = './error_data/'
if not os.path.exists(path_data):
    os.mkdir(path_data)
mi.init_log()


# ===================================================================


def _demand_judge(df):
    """

    :param df:
    :return:
    """
    if len(df) > 1:
        pass


def _demand_core(data):
    """

    :param data:
    :return:
    """
    time_1 = time.time()
    data.drop_duplicates(['id', 'gps_time'], keep='first', inplace=True)
    data.groupby('id').apply(_demand_judge)
    time_2 = time.time()
    print('用时:%s' % (time_2 - time_1))


def _demand(day, hour, minute, step_size):
    """

    :param day:
    :param hour:
    :param minute:
    :param step_size:
    :return:
    """
    count = 0
    data = pd.DataFrame()
    for x in range(day, max_day):
        if x == 17:
            continue
        for y in range(hour, max_hour):
            for z in range(minute, max_minute):
                try:
                    data_0 = rd.read_txt(x, y, z, 1)
                    data_1 = data_0.drop(['empty', 'state', 'receipt_time', 'speed'], axis=1, inplace=True)
                    del data_0
                    data_1['index_0'] = data_1.index
                    data_1['txt_time'] = rd.txt_name(x, y, z)
                    count = count + 1
                    if count == 1:
                        data = data_1
                    else:
                        data = data.append(data_1, ignore_index=True)
                    if count == step_size:
                        data['gps_time'] = pd.to_datetime(data['gps_time'], format='%Y-%m-%d %H:%M:%S')
                        print(data)
                        _demand_core(data)
                        count = 0
                        del data
                        exit()

                except Exception as result:
                    file = open('./log/demand_log.txt', 'a')
                    file.write('%s,%s\n' % (time0, result))
                    file.close()


def demand(day, hour, minute, step_size):
    """

    :param day:
    :param hour:
    :param minute:
    :param step_size:
    :return:
    """
    _demand(day, hour, minute, step_size)
    print("demand处理已完成")


if __name__ == '__main__':
    pass
    demand(1, 0, 0, 1)
