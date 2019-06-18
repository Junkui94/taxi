import os
import time
import pandas as pd
import data_read as rd
import main as mi


time0 = time.asctime()

max_day, max_hour, max_minute = 32, 24, 60  # max_day从1开始.

# =========================初始化文件目录==============================
"""
记录异常数据目录
"""
path_error_data = './error_data/'
if not os.path.exists(path_error_data):
    os.mkdir(path_error_data)
mi.init_log()

# ===================================================================


def _demand_judge(df):
    """

    :param df:
    :return:
    """
    index_list = df.index.tolist()
    for x in range(len(index_list) - 1):
        if df[df.index[x]] == 0 and df[df.index[x+1]] == 1:
            pass
        elif df[df.index[x]] == 1 and df[df.index[x+1]] == 0:
            pass


def _demand_core(data):
    """

    :param data:
    :return:
    """
    data.sort_values(['id', 'gps_time'], inplace=True)
    data.drop_duplicates(['id', 'gps_time'], keep='first', inplace=True)
    for ids, group in data.groupby('id'):
        _demand_judge(group)


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
                    data_0 = rd.read_txt(x, y, z)
                    data_1 = data_0.drop(['control', 'police', 'state', 'viaduct', 'brake', 'P1', 'lon', 'lat'
                                          'receipt_time', 'speed', 'direction', 'numS', 'P2'], axis=1).copy()
                    del data_0
                    data_1['index'] = data_1.index
                    data_1['txt_time'] = rd.txt_name(x, y, z)
                    count = count + 1
                    if count == 1:
                        data = data_1
                    else:
                        data = data.append(data_1, ignore_index=True)
                    if count == step_size:
                        _demand_core(data)
                        del data
                        count = 0
                except Exception as result:
                    file = open('./log/drift_log.txt', 'a')
                    file.write('%s,%s\n' % (time0, result))
                    file.close()


def demand(day, hour, minute, size=None):
    """

    :param day:
    :param hour:
    :param minute:
    :param size:
    :return:
    """
    _demand(day, hour, minute, size)
    print("demand处理已完成")
