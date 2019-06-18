import os
import time
import subprocess as sp
import pandas as pd
import datetime
from geopy.distance import distance
import data_read as rd
import main as mi
import file_operate as fo

time0 = time.asctime()

max_day, max_hour, max_minute = 32, 24, 60  # max_day从1开始.

# =========================初始化文件及日志目录==============================
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
    if len(df) > 1:
        gps_time = pd.Series(df['gps_time'])
        point = pd.Series(df['point'])
        """
        比较（该法效率不高）
        time_gap = (df['gps_time']-df['gps_time'].shift(1))
        print(time_gap)
        """
        time_gap = map(lambda x, y: datetime.timedelta.total_seconds(y - x), gps_time[:-1:], gps_time[1::])
        dis = map(lambda x, y: distance(y, x).meters, point[:-1:], point[1::])
        speed = list(map(lambda x, y: (y / x) > 180, time_gap, dis)) + [False]
        w = 0
        for z in range(len(speed)):
            if speed[z] is True and w == 0:
                w = 1
                speed[z] = False
            elif speed[z] is True and w == 1:
                speed[z] = True
            elif speed[z] is False and w == 1:
                w = 0
        df[speed].to_csv('%s/drift_data.txt' % path_error_data, mode='a', header=0, index=0, sep="|")


def _drift_error_core(data):
    """

    :param data:
    :return:
    """
    time_1 = time.time()
    data.drop_duplicates(['id', 'gps_time'], keep='first', inplace=True)
    data['point'] = data.apply(lambda x: (float(x['lat']), float(x['lon'])), axis=1)
    data.groupby('id').apply(_drift_judge)
    time_2 = time.time()
    print('用时:%s' % (time_2 - time_1))


def _drift_error(day, hour, minute, step_size):
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
                    data_1 = data_0.drop(['control', 'police', 'empty', 'state', 'viaduct', 'brake', 'P1',
                                          'receipt_time', 'speed', 'direction', 'numS', 'P2'], axis=1, inplace=True)
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
                        _drift_error_core(data)
                        count = 0
                        del data
                        # exit()

                except Exception as result:
                    file = open('./log/drift_log.txt', 'a')
                    file.write('%s,%s\n' % (time0, result))
                    file.close()


def drift_error(day, hour, minute, step_size):
    """

    :param day:
    :param hour:
    :param minute:
    :param step_size:
    :return:
    """
    _drift_error(day, hour, minute, step_size)
    print("drift_error处理已完成")


def data_drift_delete():
    """

    :return:
    """
    df0 = pd.read_csv('./error_data/drift_data.txt',
                      names=['id', 'gps_time', 'lon', 'lat', 'index_1', 'txt_name', 'point'], sep='|')
    df0.groupby('txt_name').apply(_drift_delete)


def _drift_delete(df0):
    """

    :return:
    """
    df1 = fo.error_data_path(df0['txt_name'])
    df2 = rd.read_txt(df1.day, df1.hour, df1.minute)
    df2['index_1'] = df2.index
    df3 = df2[~df2.index_1.isin(df0['index_1'])]
    df3.drop(['index_1'], axis=1, inplace=True)
    df3.to_csv(rd.path_name(df1.day, df1.hour, df1.minute), header=0, index=0, sep="|")


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
        data.drop(['control', 'police', 'viaduct', 'brake', 'P1', 'direction', 'numS', 'P2'], axis=1,
                  inplace=True, errors='ignore')
        data.to_csv(pt_name02, header=0, index=0, sep="|")

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
    for x in range(1, max_day):
        if x == 17:
            continue
        for y in range(0, max_hour):
            path02 = rd.txt_path(x, y, 1)
            if not os.path.exists(path02):
                os.makedirs(path02)
            for z in range(0, max_minute):
                pt_name02 = rd.path_name(x, y, z, types=1)
                if not os.path.exists(pt_name02):
                    _data_reduce_core(x, y, z, pt_name02)
    print('已有数据精简完成！')


# =============================================================================


def data_count():
    """
    所有数据精简
    :return:
    """
    for x in range(25, max_day):
        if x == 17:
            continue
        # _unzip_file(x)
        for y in range(0, max_hour):
            for z in range(0, max_minute):
                _data_count(x, y, z)
    print('已有数据统计完成！')


def _data_count(x, y, z):
    """

    :param x:
    :param y:
    :param z:
    :return:
    """
    pn = rd.path_name(x, y, z)
    count = sp.getoutput('wc -l %s' % pn)
    file = open('./log/count.txt', 'a')
    file.write('%s\n' % count)
    file.close()


def _unzip_file(x):
    """

    :param x:
    :return:
    """
    data = pd.read_csv('/media/wjk/wjkfiles/data/password.txt', names='d')
    if x > 17:
        ps = data.loc[x - 2, 'd']
    else:
        ps = data.loc[x - 1, 'd']
    print(str(ps))
    pn = '/media/wjk/wjkfiles/data/HT1603%02d.rar' % x
    sp.call('unrar x -p%s %s' % (ps, pn))

    pass


# =============================================================================

if __name__ == '__main__':
    pass
    # data = pd.read_csv('./log/count.txt', names=['1', '2'], sep=' ')
    # data = data[data['1'].str.len() != 3]
    # data['1'] = data['1'].astype('int')
    # print(data['1'].sum())
