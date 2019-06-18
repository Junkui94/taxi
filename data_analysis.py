# -*- coding:utf-8 -*-

import sys
import os
import pandas as pd
from decimal import *
from pandas import Series, DataFrame
import read_data as rd


# =====================文件路径创建=========================================

path = '.\\data'
isExists = os.path.exists(path)
if not isExists:
    for i in range(1, 32):
        for j in range(24):
            os.makedirs(path + '\\loc\\%02d\\%02d' % (i, j))
            os.makedirs(path + '\\demand\\%02d\\%02d' % (i, j))

'''常量定义'''
Area = 300
N = 32
S = 30.7
W = 122.2
E = 120.8
x1 = (Decimal(W) - Decimal(E)) / Area
y1 = (Decimal(N) - Decimal(S)) / Area


# =====================分表数据处理=========================================

def loc_state(day, hour, minute):
    """
    Current taxi location
    :param day: someday
    :param hour, minute: sometable
    :return:loc dataframe
    """

    da = rd.data(day, hour, minute, sort='id, gpstime', columns='id,lon,lat')
    da.drop_duplicates(['id'], inplace=True)
    da.reindex()
    return da


def area(Area=Area, E=E, S=S, x1=x1, y1=y1):
    """
    Rectangular partition
    :param Area:numbers
    :param E:
    :param S:
    :param x1:
    :param y1:
    :return:
    """
    if os.path.exists('.\\data\\area.txt'):
        da = pd.read_csv('.\\data\\area.txt')

        return da

    else:
        # 1. 上海市分区：根据上海市东经120°52′至122°12′,北纬30°40′至31°53′之间

        getcontext().prec = 6

        da = DataFrame(columns=['id1', 'id2', 'lon', 'lat'])

        for x in range(Area):
            for y in range(Area):
                di = {'id1': x, 'id2': y,
                      'lon': E + (x + 0.5) * float(x1),
                      'lat': S + (y + 0.5) * float(y1)}

                xy = DataFrame(di, index=[0])
                da = da.append(xy, ignore_index=True)

        da.to_csv('.\\data\\area.txt', index=False)
        return da


def count(data, E=E, S=S, x1=x1, y1=y1):
    """
    Statistical quantity
    :param data:
    :param E:
    :param S:
    :param x1:
    :param y1:
    :return:
    """

    da = area()
    # 2. 统计
    da['num'] = 0

    a = (data['lon'] - E) // float(x1)
    b = (data['lat'] - S) // float(y1)

    for i in data.index:
        c = da[(da.id1 == a[i]) & (da.id2 == b[i])].index.tolist()
        da.loc[c, 'num'] = da.loc[c, 'num'] + 1
    list1 = da[da['num'] == 0].index.tolist()
    da.drop(list1, inplace=True)
    da.reindex()
    return da


def _pretable_lastempty(day, hour, minute):
    """
    找出上一张表的最后一个车辆是否空车的状态
    :param day:
    :param hour:
    :param minute:
    :return:
    """
    if minute > 0:
        data = rd.data(day, hour, minute - 1, 'id, gpstime', 'id, empty')
        data.drop_duplicates('id', keep='last', inplace=True)
        return data
    elif hour > 0 & minute == 0:
        data = rd.data(day, hour - 1, 59, 'id, gpstime', 'id, empty')
        data.drop_duplicates('id', keep='last', inplace=True)
        return data
    elif day > 1 & hour == 0 & minute == 0:
        data = rd.data(day - 1, 23, 59, 'id, gpstime', 'id, empty')
        data.drop_duplicates('id', keep='last', inplace=True)
        return data
    else:
        print ('这已经是第一张表了')


def _next_lastempty(day, hour, minute):
    """
    找出下一张表的最后一个车辆是否空车的状态
    :param day:
    :param hour:
    :param minute:
    :return:
    """
    if minute == 59:
        data = rd.data(day, hour + 1, minute, 'id, gpstime', 'id, empty')
        data.drop_duplicates('id', keep='last', inplace=True)
        return data
    elif hour == 23 & minute == 59:
        data = rd.data(day + 1, 0, 0, 'id, gpstime', 'id, empty')
        data.drop_duplicates('id', keep='last', inplace=True)
        return data
    elif day == 31 & hour == 23 & minute == 59:
        print ('这已经是最后一张表了')
    else:
        data = rd.data(day, hour, minute + 1, 'id, gpstime', 'id, empty')
        data.drop_duplicates('id', keep='last', inplace=True)
        return data


def _demand(day, hour, minute):
    """
    分析单表\一分钟的交通需求
    :param day:some day
    :param hour, minute:some table
    :return:
    """
    if os.path.exists('.\\data\\demand\\%02d\\%02d\\demand_%02d.txt' % (day, hour, minute)):
        da = pd.read_csv('.\\data\\demand\\%02d\\%02d\\demand_%02d.txt' % (day, hour, minute))
        return da
    else:

        da = rd.data(day, hour, minute, sort='id, gpstime', columns='id, lon, lat, gpstime, empty, speed',
                     where='state=0')
        dad = da.groupby('id')['empty'].agg(['first', 'last', 'max', 'min'])
        '''
        共2**4=16种情况：0表示重车，1表示空车
            first last max min
        1.    1   1   1   1       （1）空车途中（2）第一个点下客                                                      
        2.    1   0   1   0       （1）中间上客     
        3.    0   1   1   0       （1）中间下客    
        4.    0   0   1   0       （1）中间下客，再上客
        5.    0   0   0   0       （1）重车途中（2）第一个点上客
        其中现实中不存在的情况：
        6.    1   1   1   0       （1）中间上客，再下客
        其中数据上不存在的情况：
        7.    1   1   0   0
        8.    1   1   0   1
        9.    0   0   0   1
        10.    0   1   0   1
        11.    0   0   1   1
        12.    1   0   0   1
        13.    0   1   0   0
        14.    0   1   1   1
        15.    1   0   1   1 
        16.    1   0   0   0  
        '''
        # 找出符合这几种条件的车辆id
        dad1 = dad[(dad['first'] == 1) & (dad['last'] == 1) & (dad['min'] == 1)].index.tolist()
        dad2 = dad[(dad['first'] == 1) & (dad['last'] == 0)].index.tolist()
        dad3 = dad[(dad['first'] == 0) & (dad['last'] == 1)].index.tolist()
        dad4 = dad[(dad['first'] == 0) & (dad['last'] == 0) & (dad['max'] == 1)].index.tolist()
        dad5 = dad[(dad['first'] == 0) & (dad['last'] == 0) & (dad['max'] == 0)].index.tolist()

        # 验证每种状态，并将详细信息记录
        if day == 1 & hour == 0 & minute == 0:
            pass
        else:
            # 找出上一张表的最后一个状态
            data = _pretable_lastempty(day, hour, minute)
            # 1
            da1 = da[da['id'].isin(dad1)].drop_duplicates('id')
            dat1 = data[(data['id'].isin(dad1)) & (data['empty'] == 0)]
            da1 = da1[da1['id'].isin(dat1['id'])]

            # 2
            da2 = da[(da['id'].isin(dad2)) & (da['empty'] == 0)].drop_duplicates('id')
            dd = DataFrame(pd.concat([da1, da2], axis=0, ignore_index=True, copy=False))

            # 3
            da3 = da[(da['id'].isin(dad3)) & (da['empty'] == 1)].drop_duplicates('id')
            dd = DataFrame(pd.concat([dd, da3], axis=0, ignore_index=True, copy=False))

            # 4:有O有D
            da4O = da[da['id'].isin(dad4)]
            print da4O
            da4D = da[(da['id'].isin(dad4)) & (da['empty'] == 1)].drop_duplicates('id')
            print da4D
            # da4O = da[(da['id'].isin(dad4)) & (da.index > da4D.index) & (da['empty'] == 0)].drop_duplicates('id')
            # print da4O
            dd = DataFrame(pd.concat([dd, da4D, da4O], axis=0, ignore_index=True, copy=False))

            # 5
            da5 = da[da['id'].isin(dad5)].drop_duplicates('id')
            dat5 = data[(data['id'].isin(dad5)) & (data['empty'] == 1)]
            dd5 = da5[da5['id'].isin(dat5['id'])]
            dd = DataFrame(pd.concat([dd, dd5], axis=0, ignore_index=True, copy=False))
            # print dd

            # dd.to_csv('.\\data\\demand\\%02d\\%02d\\demand_%02d.txt' % (day, hour, minute), index=False)


def demand(st_d=1, ed_d=None):
    """
    某几天的交通需求
    :param st_d:
    :param ed_d:
    :return:
    """
    # (_demand(i, j, k) for i in range(st_d, ed_d+1) for j in range(24) for k in range(60))

    for i in range(st_d, ed_d + 1):
        for j in range(24):
            for k in range(60):
                _demand(i, j, k)


def hot_deal(day, hour, minute, type):
    """
    热度分析
    :param day:
    :param hour, minute:
    :param type:
    :return:
    """
    if type == 'loc':

        if os.path.exists('.\\data\\loc\\%02d\\%02d\\hot_%02d.txt' % (day, hour, minute)):
            da = pd.read_csv('.\\data\\loc\\%02d\\%02d\\hot_%02d.txt' % (day, hour, minute))

            return da

        else:

            data = loc_state(day, hour, minute)
            da = count(data)
            da.to_csv('.\\data\\loc\\%02d\\%02d\\hot_%02d.txt' % (day, hour, minute), index=False)

            return da
    elif type == 'demand':

        if os.path.exists('.\\data\\demand\\%02d\\%02d\\hot_%02d.txt' % (day, hour, minute)):
            da = pd.read_csv('.\\data\\demand\\%02d\\%02d\\hot_%02d.txt' % (day, hour, minute))

            return da

        else:

            data = _demand(day, hour, minute)
            da = count(data)

            da.to_csv('.\\data\\demand\\%02d\\%02d\\hot_%02d.txt' % (day, hour, minute), index=False)
            return da


if __name__ == '__main__':
    pass
    _demand(1, 0, 3)
