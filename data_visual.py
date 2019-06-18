# -*-coding:utf-8 -*-

import matplotlib.pyplot as plt
import geoplotlib as gp
from geoplotlib.layers import BaseLayer
from geoplotlib.core import BatchPainter
from geoplotlib.utils import BoundingBox
from mpl_toolkits.basemap import Basemap
import pyecharts as pe
from pyecharts import Geo
import data_analysis as da


# ==================================================================
# 上海市的地图路网背景

fig = plt.figure()
m = Basemap(width=160000, height=160000,
            resolution='h', projection='eqdc',
            lat_1=90, lat_2=90, lat_0=31.23, lon_0=121.47)


# ==================================================================

def draw_bg():
    """
    basemap画地图背景
    :return:
    """
    global fig
    global m
    # m.drawmapboundary(color='k')
    m.drawmapboundary(color='k', fill_color='w')

    # m.readshapefile('F:\\road_datas\\ShangHai\\ShangHai', '', drawbounds=True,color='gray')
    m.readshapefile('F:\\road_data\\ShangHai\\boundary', '', drawbounds=True, color='gray', zorder=0)
    m.readshapefile('F:\\road_data\\ShangHai\\road_ShangHai', '', drawbounds=True, color='k', zorder=1)


def draw_map():
    """
    显示地图背景
    :return:
    """
    draw_bg()

    plt.title("Shanghai")
    plt.show()


def draw_taxi(day, hour, minute):
    """
    显示某一时刻的出租车位置
    :param day:
    :param hour, minute:
    :return:
    """

    global fig
    global m
    draw_bg()

    data = da.loc_state(day, hour, minute)
    lon = data['lon'].values
    lat = data['lat'].values
    # x, y = map(lon, lat)

    # 2. 绘制出租车
    fig.set_zorder(2)
    m.scatter(lon, lat, s=1, latlon=True, )

    plt.title("taxis distribution in Shanghai")
    plt.show()


def draw_scatter(day, hour, minute, tp):
    """
    绘制散点热力图
    :param day:
    :param hour, minute:
    :param tp: type：neither loc or demand
    :return:
    """

    global fig
    global m
    draw_bg()

    data = da.hot_deal(day, hour, minute, tp)
    lon = data['lon'].values
    lat = data['lat'].values
    num = data['num'].values
    size = num

    fig.set_zorder(2)
    m.scatter(lon, lat, s=size, c=size, latlon=True)

    plt.title("taxis hot in Shanghai")
    plt.savefig('hot1%s.png' % hour, minute, dpi=500)
    plt.show()


class AnimatedLayer(BaseLayer):
    """
    geoplotlib 动画类图层
    """

    def __init__(self, data):
        self.data = data
        self.frame_counter = 0

    def invalidate(self, proj):
        self.x, self.y = proj.lonlat_to_screen(
            self.data['lon'], self.data['lat'])

    def draw(self, proj, mouse_x, mouse_y, ui_manager):
        self.painter = BatchPainter()
        self.painter.points(self.x[self.frame_counter],
                            self.y[self.frame_counter]
                            )
        self.painter.batch_draw()
        self.frame_counter += 1


def _draw_hot(day, hour, minute):
    """
    geoplotlib热力图
    :param day:
    :param hour, minute:
    :param tp:
    :return:
    """

    data = da.loc_state(day, hour, minute)

    bbox1 = BoundingBox(north=32, south=30.7, west=122.2, east=120.8)

    gp.set_bbox(bbox1)

    # gp.shapefiles('F:\\road_datas\\ShangHai\\boundary', shape_type='empty')
    gp.kde(data, bw=[0.5, 0.5], cmap='jet', scaling='wjk')
    gp.savefig('001')
    gp.show()


def darw_dayhot(sd):
    """

    :param sd:
    :param tp:
    :return:
    """

    for i in range(24):
        for j in range(60):
            data = da.loc_state(sd, i, j)


def _draw_hot1(day, hour, minute, tp):
    """
    pycharts热力图
    :param day:
    :param hour, minute:
    :param tp:
    :return:
    """
    geo = Geo("上海市出租车",
              "data from 强生出租车",
              title_color="#fff",
              title_pos="center",
              width=500,
              height=400,
              background_color="#404a59", )

    data = da.hot_deal(day, hour, minute, tp)
    for i in data.index:
        geo.add_coordinate('%s' % i, data.loc[i, 'lon'], data.loc[i, 'lat'])
    list1 = [('%s' % i, data.loc[i, 'num']) for i in data.index]
    attr, value = geo.cast(list1)

    geo.add(
        "",
        attr,
        value,
        maptype=u'上海',
        coordinate_region=u'上海',
        # type='heatmap',
        is_visualmap=True,
        visual_text_color="#fff",
        visual_range=[1, 10],
        symbol_size=2,
        is_roam=False)

    return geo


def draw_onehot1(day, hour, minute, tp):
    """
    单表
    :param day:
    :param hour, minute:
    :param tp:
    :return:
    """

    x = _draw_hot1(day, hour, minute, tp)
    x.render()


def draw_dayhot1(sd, tp):
    """
    显示一天热力图变化
    :param sd: 某天
    :param tp: loc or demand
    :return:
    """

    timeline = pe.Timeline()
    for i in range(24):
        for j in range(60):
            geo = _draw_hot1(sd, i, j, tp)
            timeline.add(geo, time_point=0.06)
    timeline.render()


if __name__ == '__main__':
    pass
    _draw_hot(1, 12, 0)
