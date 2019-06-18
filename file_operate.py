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


def error_data_path(df):
    """

    :param df1:
    :return:
    """
    df0 = pd.DataFrame(df)
    df0.drop_duplicates(['txt_name'], inplace=True)
    df1 = df0.txt_name.str.extract('1603(?P<day>\\d{2})(?P<hour>\\d{2})(?P<minute>\\d{2}).txt', expand=True)
    df0['day'] = pd.DataFrame(df1.day, dtype=int)
    df0['hour'] = pd.DataFrame(df1.hour, dtype=int)
    df0['minute'] = pd.DataFrame(df1.minute, dtype=int)
    return df0


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
        column1 = mi.columns.copy()
        column1.append('txt_name')
        df0 = pd.read_csv('%s/%s_data.txt' % (file_dir, types), names=column1, sep='|',
                          encoding='iso-8859-1', low_memory=False)
        df1 = error_data_path(df0['txt_name'])
        for y in df1.index:
            x = df1.loc[y]
            shutil.copyfile('%s/%02d/%02d/%s' % (path_from, x.day, x.hour, x.txt_name),
                            '%s/%02d/%02d/%s' % (path_to, x.day, x.hour, x.txt_name))
    print('文件快速备份已完成！')


if __name__ == '__main__':
    a = path_a
    b = path_b
    copy_file(path_from=b, path_to=a, types_error='speed')
