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
path_data = './data/demand_data'
if not os.path.exists(path_data):
    os.mkdir(path_data)
mi.init_log()


# ===================================================================


def _demand_judge(df):
    """

    :param df:
    :return:
    """
    x = list(pd.Series(df.day).astype('int'))
    df.drop(['day'], axis=1, inplace=True)
    empty = df['empty']-df['empty'].shift(1)
    empty = list(pd.Series(empty).astype('bool'))
    empty[0] = False
    # print(empty)
    # print(df[empty])
    df[empty].to_csv('%s/%02d.txt' % (path_data, x[0]), mode='a', header=0, index=0, sep="|")
    del x
    del df


def _demand_core(data):
    """

    :param data:
    :return:
    """
    time_1 = time.time()
    data.drop_duplicates(['id', 'gps_time'], keep='first', inplace=True)
    data.groupby('id').apply(_demand_judge)
    del data
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
                    data_1 = data_0.drop(['receipt_time', 'speed'], axis=1).copy()
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
                        data['day'] = x
                        _demand_core(data)
                        count = 0
                        del data
                        # exit()

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


# ===================================================================


def demand_clean(day):
    data = pd.read_csv('%s/%02d.txt' % (path_data, day),
                       names=['id', 'empty', 'state', 'gps_time', 'lon', 'lat', 'index_1', 'txt_name'],
                       sep='|')
    print(data)


if __name__ == '__main__':
    pass
    demand_clean(1)
