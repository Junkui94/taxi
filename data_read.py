import pandas as pd
import main as mi
import numpy as np
import re

# ==========================判断数据文件路径是否存在================

"""
linux下的路径
精简数据路径：path1 
源数据路径：path0 
windows下的路径
精简数据路径：path3 
源数据路径：path2 
"""

path_a = mi.path0  # 选择源数据路径
# path_a = mi.path5  # 统计
path_b = mi.path1  # 选择精简数据路径


# import os
# isExists = os.path.exists(path_b)
# if not isExists:
#     print("源数据文件不存在")
#     exit()


# ================================================================


def txt_name(day, hour, minute):
    """
    根据表的时间生成表名
    :param day:
    :param hour:
    :param minute:
    :return:
    """
    name = '1603%02d%02d%02d.txt' % (day, hour, minute)
    return name


def txt_path(day, hour, types):
    """
    根据要求生成数据文件路径
    :param day:
    :param hour:
    :param types: 0表示源文件路径，其他表示精简的文件路径
    :return:
    """
    if types == 0:
        path = '%s/%02d/%02d/' % (path_a, day, hour)
    else:
        path = '%s/%02d/%02d/' % (path_b, day, hour)
    return path


def path_name(day, hour, minute, types=0):
    """
    生成完整的文件路径与名称
    :param day:
    :param hour:
    :param minute:
    :param types:
    :return:
    """
    name = txt_name(day, hour, minute)
    path = txt_path(day, hour, types)
    pt_name = '%s%s' % (path, name)
    return pt_name


# ================================================================


def view_one_txt(day, hour, minute, types=0):
    """
    查看单个txt文件的原数据
    :param day:
    :param hour:
    :param minute:
    :param types:
    :return:
    """
    path = path_name(day, hour, minute, types)
    data = pd.read_csv(path, delimiter='|', names=mi.columns, encoding='iso-8859-1')
    print(data)


def read_txt(day, hour, minute, types=0):
    """
    读取单个文件数据
    :param day:
    :param hour:
    :param minute:
    :param types:
    :return:
    """
    pt_name0 = path_name(day, hour, minute, types)
    columns = mi.columns
    if types != 0:
        columns = mi.reduction_columns
    try:
        data = pd.read_csv(pt_name0, delimiter='|', names=columns,
                           encoding='iso-8859-1', low_memory=False)
        return data
    except Exception as result:
        print(result)


# ================================================================

def read_demand(day, hour, minute):

    da = pd.read_csv('./data/demand_data_fin/%02d%02d%02d.csv' % (day, hour, minute),
                     names=['area_id_0', 'area_id_1', 'num'])
    da2 = np.zeros(shape=[201, 201])
    for x in da.index:
        da2[da['area_id_0'][x]-1][da['area_id_1'][x]-1] = da['num'][x]
        # print(da2[da['area_id_0'][x]][da['area_id_1'][x]-1])
        # print(da['num'][x])
    # print(da2)
    return da2


# ================================================================

def _weather_type(df):
    weather_type = ['晴', '多云', '阴', '阵雨', '雷阵雨', '冰雹', '雨夹雪', '小雨', '中雨', '大雨', '暴雨',
                    '大暴雨', '特大暴雨', '阵雪', '小雪', '中雪', '大雪', '暴雪', '雾', '冻雨', '沙尘暴', '小到中雨',
                    '中到大雨', '大到暴雨', '暴雨到大暴雨', '大暴雨到特大暴雨', '小到中雪', '中到大雪', '大到暴雪', '浮尘', '扬沙', '强沙尘暴', '霾', ]

    data = pd.DataFrame(weather_type, columns=['W'])
    data['M'] = [x for x in range(len(data))]
    labels = data['W'].apply(lambda x: x.strip() == df.strip())
    return data['M'][labels].item()


def _wind_type(df):
    wind_type = ['东', '东南', '南', '西南',
                 '西', '西北', '北', '东北', ]

    data = pd.DataFrame(wind_type, columns=['W'])
    data['M'] = [x for x in range(len(data))]

    labels = data['W'].apply(lambda x: x.strip() == df.strip())
    return data['M'][labels].item()


def _wind_num(df):
    df1 = re.split(r'≤|-', df)
    if df1[0].strip() == '':
        df1[0] = 0
    return df1


def _weather_data():
    df = pd.read_csv('./data/weather.csv')
    date = df['日期'].str.extract('(?P<year>\d{4})年(?P<month>\d{2})月(?P<day>\d{2})日', expand=True)
    weather = df['天气状况'].str.extract('(?P<weather0>.*)/(?P<weather1>.*)', expand=True)
    temp = df['气温'].str.extract('(?P<temp0>.*)℃ /(?P<temp1>.*)℃', expand=True)
    wind = df['风力风向'].str.extract('(?P<wind0>.*)风(?P<wind1>.*)级 /(?P<wind2>.*)风(?P<wind3>.*)级', expand=True)
    df = pd.concat([date, weather, temp, wind], axis=1)

    weather0 = df['weather0'].apply(_weather_type)
    weather1 = df['weather1'].apply(_weather_type)
    df['weather0'] = weather0
    df['weather1'] = weather1

    wind0 = df['wind0'].apply(_wind_type)
    wind1 = df['wind1'].apply(_wind_type)
    df['wind0'] = wind0
    df['wind1'] = wind1

    wind2 = df['wind2'].apply(_wind_num)
    wind3 = df['wind3'].apply(_wind_num)
    wind2_0 = wind2.apply(lambda x: x[0])
    wind2_1 = wind2.apply(lambda x: x[1])
    wind3_0 = wind3.apply(lambda x: x[0])
    wind3_1 = wind3.apply(lambda x: x[1])
    df['wind2_0'] = wind2_0
    df['wind2_1'] = wind2_1
    df['wind3_0'] = wind3_0
    df['wind3_1'] = wind3_1
    df.drop(['wind2', 'wind3'], axis=1, inplace=True)

    print(df)

    df.to_csv('./data/weather01.csv', index=0)


def read_weather():
    data = pd.read_csv('./data/weather02.csv')
    return data

# ================================================================

if __name__ == '__main__':
    pass
    da = read_demand(1, 7, 50)

