import torch
import torch.nn as nn
from torch.utils import data
import matplotlib.pyplot as plt
import numpy as np
import time
import os


# ===================================================================================
input_file = './data/input.csv'
output_file = './data/label.csv'
net_file = 'net.pkl'


# ====================================================================================

class Taxi_Predict_Dataset(data.Dataset):
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        with open(input_file, 'r') as file:
            index = [idx + 1 for idx, line in enumerate(file)]
            self.index = index[0: int(len(index) / 4 * 3)]
        self.fopen_in = open(input_file, 'r')
        self.fopen_out = open(output_file, 'r')

    def __getitem__(self, idx):
        input_one = self.fopen_in.__next__()
        input_two = input_one[1:-2]
        input_three = input_two.split(',')
        input_fin = torch.Tensor([float(i) for i in input_three]).unsqueeze(0)

        output_one = self.fopen_out.__next__()
        output_two = output_one[1:-2]
        output_three = output_two.split(',')
        output_fin = torch.Tensor([float(i) for i in output_three]).unsqueeze(0)
        return input_fin, output_fin

    def __len__(self):
        return len(self.index)


# ===================================================================================
class Rnn_Net(nn.Module):
    def __init__(self, in_dim, hidden_dim, n_layer, out_dim):
        super(Rnn_Net, self).__init__()
        self.lstm = nn.LSTM(in_dim, hidden_dim, n_layer, batch_first=True)
        self.linear_1 = nn.Linear(hidden_dim, hidden_dim)
        self.linear_2 = nn.Linear(hidden_dim, out_dim)
        self.relu = nn.ReLU()

    def forward(self, x):
        pred, h_state = self.lstm(x)
        y = self.linear_1(pred)
        z = self.relu(y)
        predict = self.linear_2(z)
        return predict


# ==================================================================================

if __name__ == '__main__':
    pass
    time1 = time.time()
    dataset = Taxi_Predict_Dataset(input_file, output_file)
    data_loader = data.DataLoader(dataset, batch_size=1, num_workers=1, shuffle=False, pin_memory=True)
    net = Rnn_Net(164, 288, 1, 40401).cuda()
    optimizer = torch.optim.Adam(net.parameters(), lr=0.00001)
    loss_func = torch.nn.MSELoss()
    if os.path.exists(net_file):
        net.load_state_dict(torch.load(net_file))
    for epoch in range(2000):
        for step, input_data in enumerate(data_loader):
            x, y = input_data
            x = x.cuda()
            y = y.cuda()
            predict = net(x)
            loss = loss_func(predict, y)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step
            print(step, loss)
    torch.save(net.state_dict(), net_file)
    time2 = time.time()
    print(time2 - time1)
