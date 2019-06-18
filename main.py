# -*- coding:utf-8 -*-
import os

# ==========================数据路径======================================
"""
linux下的路径
精简数据路径：path1 = '/media/wjk/taxi/data-1'
源数据路径：path0 = '/media/wjk/taxi/data-0'
windows下的路径
精简数据路径：path3 = 'F:/test/data-1'
源数据路径：path2 = 'F:/taxi/data-0'
"""

path1 = '/media/wjk/taxi/data-1'
path0 = '/media/wjk/taxi/data-0'
# path0 = '/media/wjk/wjkfiles/data-0'
path3 = 'F:/test/data-1'
path2 = 'F:/taxi/data-0'
# ==========================数据字段名=====================================
columns = ['id', 'control', 'police', 'empty',
           'state', 'viaduct', 'brake', 'P1',
           'receipt_time', 'gps_time', 'lon', 'lat',
           'speed', 'direction', 'numS', 'P2']
reduction_columns = ['id', 'control', 'empty', 'state',
                     'receipt_time', 'gps_time', 'lon', 'lat', 'speed']


# ===========================日志记录目录=================================


def init_log():
    path_log = './log/'
    if not os.path.exists(path_log):
        os.mkdir(path_log)


if __name__ == '__main__':
    pass

