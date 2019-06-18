import os
import time
import pandas as pd
from geopy.distance import distance
import data_read as rd
import main as mi
import file_operate as fo

time0 = time.asctime()

# earliest_time = '2016-02-29 23:55:00'
# latest_time = '2016-03-31 23:59:59'

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


types_search = {'missing': 'data[data.isnull().values].drop_duplicates()',
                'duplication': 'data[data.duplicated()].copy()',
                'id': 'data[data[types] > 99999]',
                'control': 'data[data[types] != \'A\'].copy()',
                'police': 'data[~data[types].isin([\'0\', \'1\'])].copy()',
                'empty': 'data[~data[types].isin([\'0\', \'1\'])].copy()',
                'state': 'data[~data[types].isin([\'0\', \'1\', \'2\', \'3\', \'4\', \'5\', \'A\', \'V\'])].copy()',
                'viaduct': 'data[~data[types].isin([\'0\', \'1\'])].copy()',
                'brake': 'data[~data[types].isin([\'0\', \'1\'])].copy()',
                # 'gps_time': 'data[data[types] > \'2016-03-31 23:59:59\'].copy()',
                'gps_time': 'data[data[types] < \'2016-02-29 23:55:00\'].copy()',
                'speed': 'data[data[types] > 180].copy()',
                }
types_drop = {'missing': 'data.dropna(inplace=True)',
              'duplication': 'data.drop_duplicates(inplace=True)',
              'id': 'data.drop(index=types_error.index, inplace=True)',
              'control': 'data.drop(index=types_error.index, inplace=True)',
              'police': 'data.drop(index=types_error.index, inplace=True)',
              'empty': 'data.drop(index=types_error.index, inplace=True)',
              'state': 'data.drop(index=types_error.index, inplace=True)',
              'viaduct': 'data.drop(index=types_error.index, inplace=True)',
              'brake': 'data.drop(index=types_error.index, inplace=True)',
              'gps_time': 'data.drop(index=types_error.index, inplace=True)',
              'speed': 'data.drop(index=types_error.index, inplace=True)',
              }


# ====================================================================


def _data_clean(types, size=None, day=None, hour=None, minute=None):
    """

    :param types:
    :param size:
    :param day:
    :param hour:
    :param minute:
    :return:
    """
    types_choice = {'search': types_search[types], 'drop': types_drop[types]}
    if size != 'all':
        _data_clean_core(types, day, hour, minute, types_choice)
    else:
        for x in range(1, max_day):
            if x == 17:
                continue
            for y in range(0, max_hour):
                for z in range(0, max_minute):
                    _data_clean_core(types, day=x, hour=y, minute=z, types_choice=types_choice)


def _data_clean_core(types, day, hour, minute, types_choice):
    """

    :param types:
    :param day:
    :param hour:
    :param minute:
    :param types_choice:
    :return:
    """
    try:
        data = rd.read_txt(day, hour, minute)
        # print(data)
        types_error = eval(types_choice['search'])
        # print(types_error)
        if not types_error.empty:
            types_error['txt_name'] = rd.txt_name(day, hour, minute)
            types_error.to_csv('%s/%s_data.txt' % (path_error_data, types), mode='a', header=0, index=0, sep="|")
            eval(types_choice['drop'])
            data.to_csv(rd.path_name(day, hour, minute), header=0, index=0, sep="|")
            del data

    except Exception as result:
        file = open('./log/%s_log.txt' % types, 'a')
        file.write('%s,%s,%s\n' % (time0, rd.txt_name(day, hour, minute), result))
        file.close()


def data_clean(types, size=None, day=None, hour=None, minute=None):
    """

    :param types:
    :param size:
    :param day:
    :param hour:
    :param minute:
    :return:
    """

    _data_clean(types=types, size=size, day=day, hour=hour, minute=minute)
    print('%s已完成！' % types)


def data_error_delete(types):
    """

    :param types:
    :return:
    """
    types_choice = {'search': types_search[types], 'drop': types_drop[types]}
    df1 = fo.error_data_path(types)
    # print(df1)
    for y in df1.index:
        x = df1.loc[y]
        data = rd.read_txt(day=x.day, hour=x.hour, minute=x.minute)
        types_error = eval(types_choice['search'])
        print(types_error)
        if not types_error.empty:
            eval(types_choice['drop'])
            data.to_csv(rd.path_name(day=x.day, hour=x.hour, minute=x.minute), header=0, index=0, sep="|")
            del data


# =========================================================================

def _drift_judge(df):
    """

    :param df:
    :return:
    """
    index_list = df.index.tolist()
    for x in range(len(index_list)-1):
        point_one = (float(df.loc[index_list[x], ['lat']].copy()), float(df.loc[index_list[x], ['lon']].copy()))
        point_two = (float(df.loc[index_list[x+1], ['lat']].copy()), float(df.loc[index_list[x+1], ['lon']].copy()))
        dis = distance(point_one, point_two).meters
        time_gap = (pd.to_datetime(df.loc[index_list[x + 1], ['gps_time']]) -
                    pd.to_datetime(df.loc[index_list[x], ['gps_time']])).dt.seconds
        speed = dis/float(time_gap)
        if speed > 180:
            # print(speed)
            df.loc[index_list[x]].to_csv('%s/drift_data.txt' % path_error_data, mode='a', header=0, index=0, sep="|")


def _drift_error_core(data):
    """

    :param day:
    :param hour:
    :param minute:
    :return:
    """
    try:

        data.sort_values(['id', 'gps_time'], inplace=True)
        data.drop_duplicates(['id', 'gps_time'], keep='first', inplace=True)
        for ids, group in data.groupby('id'):
            _drift_judge(group)

    except Exception as result:
        file = open('./log/drift_log.txt', 'a')
        file.write('%s,%s\n' % (time0, result))
        file.close()


def _drift_error(day, hour, minute, step_size):
    """

    :param day:
    :param hour:
    :param minute:
    :param size:
    :return:
    """
    count = 0
    data = pd.DataFrame()
    for x in range(day, max_day):
        if x == 17:
            continue
        for y in range(hour, max_hour):
                for z in range(minute, max_minute):
                    data_0 = rd.read_txt(x, y, z)
                    data_1 = data_0.drop(['control', 'police', 'empty', 'state', 'viaduct', 'brake', 'P1',
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
                        _drift_error_core(data)
                        del data
                        count = 0


def drift_error(day, hour, minute, size=None):
        """

        :param day:
        :param hour:
        :param minute:
        :param size:
        :return:
        """
        _drift_error(day, hour, minute, size)
        print("drift_error处理已完成")


# =========================================================================


def _data_reduce_core(day, hour, minute, pt_name02):
    """
    数据精简，去掉无用字段
    :param day:
    :param hour:
    :param minute:
    :param pt_name02:
    :return:
    """
    try:
        data = rd.read_txt(day, hour, minute)
        data.drop(['police', 'viaduct', 'brake', 'P1', 'direction', 'numS', 'P2'], axis=1,
                  inplace=True, errors='ignore')
        data.to_csv(pt_name02, index=False)

    except Exception as result:
        file = open('./log/reduce_log.txt', 'a')
        file.write('%s ：%s\n' % (rd.txt_name(day, hour, minute), result))
        file.close()
        print('%s ：%s' % (rd.txt_name(day, hour, minute), result))


def data_reduce():
    """
    所有数据精简
    :return:
    """
    pt_name02 = ''
    day, hour, minute = 1, 0, 0
    for x in range(1, max_day):
        if x == 17:
            continue
        for y in range(0, max_hour):
            for z in range(0, max_minute):
                pt_name02 = rd.path_name(x, y, z, types=1)
                if not os.path.exists(pt_name02):
                    day, hour, minute = x, y, z
                    break
    path02 = rd.txt_path(day, hour, 1)
    for x in range(day, max_day):
        if x == 17:
            continue
        for y in range(0, max_hour):
            if not os.path.exists(path02):
                os.makedirs(path02)
            for z in range(0, max_minute):
                _data_reduce_core(x, y, z, pt_name02)

    print('已有数据精简完成！')


# =============================================================================

if __name__ == '__main__':
    pass
    drift_error(1, 0, 0, 60)
