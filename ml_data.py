import torch
from torchvision import transforms
import numpy as np
import pandas as pd
import data_read as dr

# =================================数据=======================================
MONTH_TYPE = 12
DAY_TYPE = 31
WEEK_TYPE = 7
HOUR_TYPE = 24
WEATHER_TYPE = 33
WIND_TYPE = 8
batch_size = 31

# ===============================input_features===============================================


# 天气
da1 = dr.read_weather()
weather0 = torch.LongTensor(da1['weather0']).view(-1, 1)
weather0_onehot = torch.zeros(DAY_TYPE, WEATHER_TYPE).scatter_(1, weather0, 1)
weather1 = torch.LongTensor(da1['weather1']).view(-1, 1)
weather1_onehot = torch.zeros(DAY_TYPE, WEATHER_TYPE).scatter_(1, weather1, 1)
wind0 = torch.LongTensor(da1['wind0']).view(-1, 1)
wind0_onehot = torch.zeros(DAY_TYPE, WIND_TYPE).scatter_(1, wind0, 1)
wind1 = torch.LongTensor(da1['wind1']).view(-1, 1)
wind1_onehot = torch.zeros(DAY_TYPE, WIND_TYPE).scatter_(1, wind1, 1)
# 时间
day_onehot = torch.eye(31)
da2 = pd.read_csv('./data/week.csv')
festival = torch.LongTensor(da2['节日']).view(-1, 1)
holiday = torch.LongTensor(da2['假日']).view(-1, 1)
week = torch.LongTensor(da2['星期']).view(-1, 1)
week_onehot = torch.zeros(DAY_TYPE, WEEK_TYPE).scatter_(1, week, 1)
hour_onehot = torch.eye(24)
table_onehot = torch.eye(12)
# 数据归一化
temp0 = da1['temp0'] / 40
temp1 = da1['temp1'] / 40
wind2_0 = da1['wind2_0'] / 12
wind2_1 = da1['wind2_1'] / 12
wind3_0 = da1['wind3_0'] / 12
wind3_1 = da1['wind3_1'] / 12

# print(da1)


for x in range(0, 31):
    if x == 16:
        continue
    for y in range(0, 24):
        for z in range(0, 12):
            pass
            tables = list(week_onehot[x:x + 1].numpy().reshape(-1))
            tables.extend(list(festival[x:x + 1].numpy().reshape(-1)))
            tables.extend(list(holiday[x:x + 1].numpy().reshape(-1)))
            tables.extend(list(day_onehot[x:x + 1].numpy().reshape(-1)))
            tables.extend(list(hour_onehot[y:y + 1].numpy().reshape(-1)))
            tables.extend(list(table_onehot[z:z + 1].numpy().reshape(-1)))
            tables.extend(list(weather0_onehot[x:x + 1].numpy().reshape(-1)))
            tables.extend(list(weather1_onehot[x:x + 1].numpy().reshape(-1)))
            tables.extend(list(wind0_onehot[x:x + 1].numpy().reshape(-1)))
            tables.extend(list(wind1_onehot[x:x + 1].numpy().reshape(-1)))
            tables.extend(list(wind2_0[x:x + 1]))
            tables.extend(list(wind2_1[x:x + 1]))
            tables.extend(list(wind3_0[x:x + 1]))
            tables.extend(list(wind3_1[x:x + 1]))
            tables.extend(list(temp0[x:x + 1]))
            tables.extend(list(temp1[x:x + 1]))
            with open('./data/input.csv', 'a') as file:
                file.write('%s\n' % str(tables))



# ===================================output======================================
# for x in range(1, 32):
#     if x == 17:
#         continue
#     for y in range(0, 24):
#         for z in range(0, 60, 5):
#             da3 = list(dr.read_demand(x, y, z).reshape(-1))
#             print(len(da3))
            # with open('./data/label.csv', 'a') as file:
            #     file.write('%s\n' % str(da3))
