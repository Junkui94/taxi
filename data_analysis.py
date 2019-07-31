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

# def demand(day, hour, minute, step_size):
#
#     _demand(day, hour, minute, step_size)
#     print("demand处理已完成")
#
#
# def _demand(day, hour, minute, step_size):
#
#     count = 0
#     data = pd.DataFrame()
#     for x in range(day, max_day):
#         if x == 17:
#             continue
#         for y in range(hour, max_hour):
#             for z in range(minute, max_minute):
#                 try:
#                     data_0 = rd.read_txt(x, y, z, 1)
#                     data_1 = data_0.drop(['receipt_time', 'speed'], axis=1).copy()
#                     del data_0
#                     data_1['index_0'] = data_1.index
#                     data_1['txt_time'] = rd.txt_name(x, y, z)
#                     count = count + 1
#                     if count == 1:
#                         data = data_1
#                     else:
#                         data = data.append(data_1, ignore_index=True)
#                     if count == step_size:
#                         data['gps_time'] = pd.to_datetime(data['gps_time'], format='%Y-%m-%d %H:%M:%S')
#                         data['day'] = x
#                         _demand_core(data)
#                         count = 0
#                         del data
#                         # exit()
#
#                 except Exception as result:
#                     file = open('./log/demand_log.txt', 'a')
#                     file.write('%s,%s\n' % (time0, result))
#                     file.close()
#
#
# def _demand_core(data):
#
#     time_1 = time.time()
#     data.drop_duplicates(['id', 'gps_time'], keep='first', inplace=True)
#     data.groupby('id').apply(_demand_judge)
#     del data
#     time_2 = time.time()
#     print('用时:%s' % (time_2 - time_1))
#
#
# def _demand_judge(df):
#
#     print(df['gps_time'])
#     exit()
#     x = list(pd.Series(df.day).astype('int'))
#     df.drop(['day'], axis=1, inplace=True)
#     empty = df['empty'] - df['empty'].shift(1)
#     empty = list(pd.Series(empty).astype('bool'))
#     empty[0] = False
#     # print(empty)
#     # print(df[empty])
#     # df[empty].to_csv('%s/%02d.txt' % (path_data, x[0]), mode='a', index=0, sep="|")
#     del x
#     del df
#
#
# # ===================================================================
#
#
# def _add_header(day):
#     data = pd.read_csv('%s/%02d.txt' % (path_data, day),
#                        names=['id', 'empty', 'state', 'gps_time', 'lon', 'lat', 'index_1', 'txt_name'],
#                        sep='|')
#     data.to_csv('./test/%02d.csv' % day, index=0)
#
#
# def add_header_all():
#     for x in range(1, max_day):
#         if x == 17:
#             continue
#         _add_header(x)
#
#
# # ===================================================================
#
#
# def demand_clean(day):
#     data = pd.read_csv('%s/%02d.csv' % (path_data, day))
#     data['gps_time'] = pd.to_datetime(data['gps_time'], format='%Y-%m-%d %H:%M:%S')
#
#     data.groupby('id').apply(_demand_clean)
#
#
# def _demand_clean(data):
#     empty = data['empty'] - data['empty'].shift(1)
#     times = data['gps_time'] - data['gps_time'].shift(1)
#     print(data)
#     print(empty)
#     print(times)
#     exit()


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
        # print(data)
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
    # exit()


def demand_reduct(day):
    data = pd.DataFrame()
    for i in range(day, day + 1):
        da0 = pd.read_csv('./data/demand_data/%02d.csv' % day,
                          names=['id', 'empty', 'gps_time', 'lon', 'lat'])
        data = data.append(da0, ignore_index=True)
    data.drop_duplicates(['id', 'gps_time'], inplace=True)
    data.sort_values('gps_time', inplace=True)
    data.groupby('id').apply(_demand_reduct)


def _demand_reduct(data):
    if data['empty'].mean() != 1.0 and data['empty'].mean() != 0.0:
        state_i = data['empty'] - data['empty'].shift(1)
        # print(state_i)
        da1 = data[state_i == -1]
        da2 = data[state_i == 1]
        da = pd.concat([da1, da2], ignore_index=True, sort=['gps_time'])
        da.sort_values('gps_time', inplace=True)
        print(da)
        # return pd.concat([da1, da2], ignore_index=True)





if __name__ == '__main__':
    pass
    demand_reduct(1)
    # demand()
