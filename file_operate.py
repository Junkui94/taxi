import os
import shutil
import pandas as pd
import main as mi
import data_read as rd
# =========================================================
file_dir = "./error_data"
path_a = '/media/wjk/wjkfiles/data-0'
path_b = '/media/wjk/taxi/data-0'


# =========================================================


def file_name(path=file_dir):
    """
    查看当前目录下的文件列表
    :param path:
    :return:
    """
    lists = os.listdir(path)
    return lists


def unit_txt():
    """
    将文件下的所有文件数据读出，写入一个文件
    :return:
    """
    lists = file_name()
    for x in lists:
        df1 = pd.read_csv('%s/%s' % (file_dir, x), names=[mi.columns],
                          encoding='iso-8859-1', low_memory=False)
        df1.to_csv("./missing_data.txt", header=0, index=0, sep='|', mode='a')


def error_data_path(types):
    """

    :param types:
    :return:
    """
    column1 = mi.columns.copy()
    column1.append('txt_name')
    df0 = pd.read_csv('%s/%s_data.txt' % (file_dir, types), names=column1, sep='|',
                      encoding='iso-8859-1', low_memory=False)
    df1 = pd.DataFrame()
    df1['txt_name'] = df0['txt_name'].copy()
    del df0
    df1.drop_duplicates(['txt_name'], inplace=True)
    df2 = df1.txt_name.str.extract('1603(?P<day>\\d{2})(?P<hour>\\d{2})(?P<minute>\\d{2}).txt', expand=True)
    df1['day'] = pd.DataFrame(df2.day, dtype=int)
    df1['hour'] = pd.DataFrame(df2.hour, dtype=int)
    df1['minute'] = pd.DataFrame(df2.minute, dtype=int)
    return df1


def copy_file(path_from, path_to, types_error=None, day=None, hour=None, minute=None):
    """
    恢复处理错误的数据文件或备份处理好的数据文件
    :param path_from:
    :param path_to:
    :param types_error:
    :param day:
    :param hour:
    :param minute:
    :return:
    """
    if types_error is None:
        name = rd.txt_name(day=day, hour=hour, minute=minute)
        shutil.copyfile('%s/%02d/%02d/%s' % (path_from, day, hour, name),
                        '%s/%02d/%02d/%s' % (path_to, day, hour, name))
    else:
        df1 = error_data_path(types_error)
        for y in df1.index:
            x = df1.loc[y]
            shutil.copyfile('%s/%02d/%02d/%s' % (path_from, x.day, x.hour, x.txt_name),
                            '%s/%02d/%02d/%s' % (path_to, x.day, x.hour, x.txt_name))
    print('文件快速备份已完成！')


if __name__ == '__main__':
    a = path_a
    b = path_b
    copy_file(path_from=b, path_to=a, types_error='speed')
