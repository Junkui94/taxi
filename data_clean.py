import os
import time
import read_data as rd
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


def all_data(types, column='other'):
    """

    :param types:
    :return:
    """
    for day in range(1, max_day):
        if day == 17:
            continue
        for hour in range(0, max_hour):
            for minute in range(0, max_minute):
                if types == 'clean1':
                    _data_clean1(day, hour, minute)
                elif types == 'clean2':
                    _data_clean2(day, hour, minute)
                elif types == 'clean3':
                    _data_clean3(day, hour, minute, column)


# ========================================================================


def _data_clean1(day, hour, minute):
    """
    删除缺失数据
    :param day:
    :param hour:
    :param minute:
    :return:
    """
    try:
        data = rd.read_txt(day, hour, minute)
        # 1.判断缺失数据进行并记录
        missing_data = data[data.isnull().values].drop_duplicates()
        if not missing_data.empty:
            missing_data['txt_name'] = rd.txt_name(day, hour, minute)
            missing_data.to_csv('%s/missing_data.txt' % path_error_data, mode='a', header=0, index=0, sep="|")
            # 2.对缺失数据进行处理
            data.dropna(inplace=True)
            data.to_csv(rd.path_name(day, hour, minute), header=0, index=0, sep="|")
        del data
    except Exception as result:
        file = open('%s/data_clean1.txt' % path_log, 'a')
        file.write('%s,%s,%s\n' % (time0, rd.txt_name(day, hour, minute), result))
        file.close()


def all_data_clean1():
    """

    :return:
    """
    all_data('clean1')
    print('clean１已完成！')


def _data_clean2(day, hour, minute):
    """
    对数据进行去重
    :param day:
    :param hour:
    :param minute:
    :return:
    """
    try:
        data = rd.read_txt(day, hour, minute)
        # 判断重复数据并进行记录
        duplication_data = data[data.duplicated()].copy()
        if not duplication_data.empty:
            duplication_data['txt_name'] = rd.txt_name(day, hour, minute)
            duplication_data.to_csv('%s/duplication_data.txt' % path_error_data, mode='a', header=0, index=0, sep="|")
            # 对重复数据进行处理
            data.drop_duplicates(inplace=True)
            data.to_csv(rd.path_name(day, hour, minute), header=0, index=0, sep="|")
        del data
    except Exception as result:
        file = open('%s/data_clean2.txt' % path_log, 'a')
        file.write('%s,%s,%s\n' % (time0, rd.txt_name(day, hour, minute), result))
        file.close()


def all_data_clean2():
    """

    :return:
    """
    all_data('clean2')
    print('clean２已完成！')


def _data_clean3(day, hour, minute):
    """
    对格式不正确的数据进行清洗
    :param day:
    :param hour:
    :param minute:
    :return:
              'id', 'control', 'police', 'empty',
              'state', 'Viaduct', 'brake', 'P1',
              'receipt_time', 'gps_time', 'lon', 'lat',
              'speed', 'direction', 'numS', 'P2'
    """
    try:
        data = rd.read_txt(day, hour, minute)
        # 判断错误数据并进行记录
        # ====id=========================================================================================
        # id_error = data[data.id > 99999]
        # if not id_error.empty:
        #     id_error['txt_name'] = rd.txt_name(day, hour, minute)
        #     id_error.to_csv('%s/type_id_data.txt' % path_error_data, mode='a', header=0, index=0, sep="|")
        #     # 2.对缺失数据进行处理
        #     data.drop(index=id_error.index, inplace=True)
        #     data.to_csv(rd.path_name(day, hour, minute), header=0, index=0, sep="|")
        # ====control=========================================================================================
        # control_error = data[data.control != 'A']
        # if not control_error.empty:
        #     control_error['txt_name'] = rd.txt_name(day, hour, minute)
        #     control_error.to_csv('%s/type_control_data.txt' % path_error_data, mode='a', header=0, index=0, sep="|")
        #     # 2.对缺失数据进行处理
        #     data.drop(index=control_error.index, inplace=True)
        #     data.to_csv(rd.path_name(day, hour, minute), header=0, index=0, sep="|")
        # ====police=========================================================================================
        police_error = data[~data.police.isin(['0', '1'])]
        if not police_error.empty:
            police_error['txt_name'] = rd.txt_name(day, hour, minute)
            # print(police_error)
            police_error.to_csv('%s/type_police_data.txt' % path_error_data, mode='a', header=0, index=0, sep="|")
            # 2.对缺失数据进行处理
            # data.drop(index=police_error.index, inplace=True)
            # data.to_csv(rd.path_name(day, hour, minute), header=0, index=0, sep="|")
        # ====empty=========================================================================================

        del data
    except Exception as result:
        file = open('%s/data_clean3.txt' % path_log, 'a')
        file.write('%s,%s,%s\n' % (time0, rd.txt_name(day, hour, minute), result))
        file.close()


def all_data_clean3():
    """
    对数据库所有表的速度异常数据进行处理
    :return:
    """
    all_data('clean3')
    print('clean3已完成！')


def _data_reduction(day, hour, minute):
    """
    数据精简，去掉无用字段
    :param day:
    :param hour:
    :param minute:
    :return:
    """
    pt_name02 = rd.path_name(day, hour, minute, 1)
    if not os.path.exists(pt_name02):
        path02 = rd.txt_path(day, hour, 1)
        if not os.path.exists(path02):
            os.makedirs(path02)
        try:
            data = rd.read_txt(day, hour, minute)
            data.drop(['police', 'Viaduct', 'brake', 'P1', 'direction', 'numS', 'P2'], axis=1,
                      inplace=True, errors='ignore')
            data.to_csv(pt_name02, index=False)

        except Exception as result:
            file = open('./log_reductions.txt', 'a')
            file.write('%s ：%s\n' % (rd.txt_name(day, hour, minute), result))
            file.close()
            print('%s ：%s' % (rd.txt_name(day, hour, minute), result))
    else:
        print('该文件已经存在')


def all_data_reduction():
    """
    所有数据精简
    :return:
    """
    file = open('./log_reductions.txt', 'w')
    file.write('%s: %s\n' % ('开始时间', time0))
    file.close()
    i, j, k = 32, 24, 60
    for x in range(1, i):
        if x == 17:
            continue
        for y in range(0, j):
            for z in range(0, k):
                _data_reduction(x, y, z)

    print('已有数据精简完成！')


if __name__ == '__main__':
    pass
    all_data_clean3()
