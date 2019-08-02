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


def demand(day=1, hour=0, minute=0):
    for x in range(day, max_day):
        if x == 17:
            continue
        for y in range(hour, max_hour):
            print('完成：%02d' % y)
            data = pd.DataFrame()
            for z in range(minute, max_minute):
                try:

                    da0 = rd.read_txt(x, y, z, types=1)
                    data = data.append(da0, ignore_index=True)
                except Exception as result:
                    print(result)
                    continue
            data.drop(['state', 'receipt_time', 'speed'], axis=1, inplace=True)
            data.drop_duplicates(['id', 'gps_time'], inplace=True)
            data.sort_values('gps_time', inplace=True)
            da = data.groupby('id').apply(_demand_state)
            # print(da)
            da.to_csv('./data/demand_data/%02d.csv' % x, mode='a', index=0, header=0)
            del da


def _demand_state(data):
    if data['empty'].mean() != 1.0 and data['empty'].mean() != 0.0:
        state_i = data['empty'] - data['empty'].shift(1)
        # print(state_i)
        da1 = data[state_i == -1]
        da2 = data[state_i == 1]
        return pd.concat([da1, da2], ignore_index=True)
        # pass
    else:
        da3 = data.head(1)
        da4 = data.tail(1)
        return pd.concat([da3, da4], ignore_index=True)


def demand_reduct_1():
    for x in range(1, max_day):
        try:
            data = pd.read_csv('./data/demand_data/%02d.csv' % x,
                               names=['id', 'empty', 'gps_time', 'lon', 'lat'])
        except Exception as result:
            print(result)
            continue
        data.drop_duplicates(['id', 'gps_time'], inplace=True)
        data.sort_values('gps_time', inplace=True)
        da = data.groupby('id').apply(_demand_reduct_1)
        da.to_csv('./data/demand_data_1/%02d.csv' % x, mode='a', index=0, header=0)


def _demand_reduct_1(data):
    if data['empty'].mean() != 1.0 and data['empty'].mean() != 0.0:
        state_i = data['empty'] - data['empty'].shift(1)
        da0 = data[state_i == -1]
        da1 = data[state_i == 1]
        da = da0.append(da1, ignore_index=True)
        da.sort_values('gps_time', inplace=True)
        return da


def demand_reduct_2():
    for x in range(1, max_day):
        try:
            data = pd.read_csv('./data/demand_data_1/%02d.csv' % x,
                               names=['id', 'empty', 'gps_time', 'lon', 'lat'])
        except Exception as result:
            print(result)
            continue
        data['gps_time'] = pd.to_datetime(data['gps_time'], format='%Y-%m-%d %H:%M:%S')
        data['od'] = None
        da = data.groupby('id').apply(_demand_reduct_2)
        da.to_csv('./data/demand_data_2/%02d.csv' % x, mode='a', index=0, header=0)


def _demand_reduct_2(da):
    # print(da)
    if da.head(1)['empty'].any() == 1:
        da[:1]['od'] = 'qd'
        # print(data)
        data = da[1:]
    else:
        data = da
    if len(data) % 2 == 1:
        da[-1:]['od'] = 'qo'
        # print(data)
        data = data[:-1]
    da0 = data[data['empty'] == 0].reset_index()
    da1 = data[data['empty'] == 1].reset_index()
    gap_time = da1['gps_time'] - da0['gps_time']
    x = gap_time < '00:02:00'
    y = gap_time > '06:00:00'
    da3 = pd.concat([da0[x], da1[x]], ignore_index=True)
    # print(da3)
    da4 = da3.append([da0[y], da1[y]], ignore_index=True)
    # print(da4)
    da.drop(index=da4['index'], inplace=True)
    return da


def demand_reduct_3():
    for x in range(1, max_day):
        data = pd.DataFrame()
        try:
            da0 = pd.read_csv('./data/demand_data_2/%02d.csv' % x,
                              names=['id', 'empty', 'gps_time', 'lon', 'lat', 'od'])
            data = data.append(da0[da0['od'] == 'qo'], ignore_index=True)

            # da1 = pd.read_csv('./data/demand_data_2/%02d.csv' % (x + 1),
            #                   names=['id', 'empty', 'gps_time', 'lon', 'lat', 'od'])
            # data = data.append(da1[da1['od'] == 'qd'], ignore_index=True)
            # data['gps_time'] = pd.to_datetime(data['gps_time'], format='%Y-%m-%d %H:%M:%S')
            da = data.groupby('id').apply(_demand_reduct_3)
        except Exception as result:
            print(result)
            continue
        da = da.append(da0[da0['od'].isnull()], ignore_index=True)
        da.to_csv('./data/demand_data_3/%02d.csv' % x, mode='a', index=0, header=0)


def _demand_reduct_3(da):
    if len(da) == 2:
        gap_time = da['gps_time'] - da['gps_time'].shift(1)
        x = gap_time[-1:] > '00:02:00'
        y = gap_time[-1:] < '06:00:00'
        if x is True and y is True:
            print(da)
            return da


def demand_reduct_4():
    for x in range(1, max_day):
        try:
            data = pd.read_csv('./data/demand_data_3/%02d.csv' % x,
                               names=['id', 'empty', 'gps_time', 'lon', 'lat', 'od'])
        except Exception as result:
            print(result)
            continue
        del data['od']
        data.sort_values('gps_time', inplace=True)
        da = data.groupby('id').apply(_demand_reduct_4)
        da.to_csv('./data/demand_data_4/%02d.csv' % x, mode='a', index=0, header=0)


def _demand_reduct_4(data):
    # print(data)
    da0 = data[data['empty'] == 0]
    da1 = data[data['empty'] == 1]
    da0 = da0.reset_index(drop=True)
    da1 = da1.reset_index(drop=True)
    da0.rename(columns={'empty': 'empty_0', 'gps_time': 'gps_time_0', 'lon': 'lon_0', 'lat': 'lat_0'}, inplace=True)
    da1.rename(columns={'empty': 'empty_1', 'gps_time': 'gps_time_1', 'lon': 'lon_1', 'lat': 'lat_1'}, inplace=True)
    da = pd.concat([da0, da1], axis=1, ignore_index=True, sort=False)
    return da


def demand_reduct_5():
    for x in range(1, max_day):
        try:
            data = pd.read_csv('./data/demand_data_4/%02d.csv' % x,
                               names=['id', 'empty_0', 'gps_time_0', 'lon_0', 'lat_0',
                                      'id_1', 'empty_1', 'gps_time_1', 'lon_1', 'lat_1'])
        except Exception as result:
            print(result)
            continue
        del data['id_1']
        data.sort_values('gps_time_0', inplace=True)
        data.to_csv('./data/demand_data_5/%02d.csv' % x, index=0)
        

if __name__ == '__main__':
    pass
    demand_reduct_5()
    # demand()
