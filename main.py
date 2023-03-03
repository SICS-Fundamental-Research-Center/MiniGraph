import torch
from torch.autograd import Variable
from torch import nn
from preprocessing import getWebsk
from torch.utils.data import Dataset, DataLoader
from load import getDataLoader
import argparse
import numpy as np
import random

parser = argparse.ArgumentParser(description='cost model')
parser.add_argument('-m', '--mode', type=int, default=1)
parser.add_argument('-n', '--epoch', type=int, default=1000)
parser.add_argument('-s', '--seed', type=int, default=100)

args = parser.parse_args()

def setup_seed(seed):
     torch.manual_seed(seed)
     torch.cuda.manual_seed_all(seed)
     np.random.seed(seed)
     random.seed(seed)
     torch.backends.cudnn.deterministic = True
# 设置随机数种子
setup_seed(args.seed)

# class TimeSeriesDataSet(Dataset):
#   """
#   This is a custom dataset class. It can get more complex than this, but simplified so you can understand what's happening here without
#   getting bogged down by the preprocessing
#   """
#   def __init__(self, X, Y):
#     self.X = X
#     self.Y = Y
#     if len(self.X) != len(self.Y):
#       raise Exception("The length of X does not match the length of Y")

#   def __len__(self):
#     return len(self.X)

#   def __getitem__(self, index):
#     # note that this isn't randomly selecting. It's a simple get a single item that represents an x and y
#     _x = self.X[index]
#     _y = self.Y[index]

#     return _x, _y
  
class poly_model(nn.Module):
    def __init__(self):
        super(poly_model,self).__init__()
        self.poly = nn.Linear(5,1)

    def forward(self, x):
        out = self.poly(x)
        return out


class poly_model_init(nn.Module):
    def __init__(self):
        super(poly_model_init,self).__init__()
        self.y1 = nn.Linear(5,1)
        self.y2 = nn.Linear(5,1)

    def forward(self, x):
        cpu = x[:,-1:]
        # print(cpu)
        # print('=====')
        # print(x[:,0:-1])
        # print(x)
        # print('===')
        x_use = x[:,0:-1]
        out1 = self.y1(x_use)
        out2 = self.y2(x_use)
        out3 = out2.div(cpu)
        return out1 + out3

class poly_model_less(nn.Module):
    def __init__(self):
        super(poly_model_less,self).__init__()
        self.y1 = nn.Linear(2,1)
        self.y2 = nn.Linear(2,1)

    def forward(self, x):
        cpu = x[:,-1:]
        x_use = x[:,0:-1]
        out1 = self.y1(x_use)
        out2 = self.y2(x_use)
        out3 = out2.div(cpu)
        return out1 + out3


class Net(nn.Module):
    def __init__(self):
        super(Net,self).__init__()
        self.l1 = nn.Linear(6, 20)
        self.l2 = nn.ReLU()
        self.l3 = nn.Linear(20, 1)
        # self.l4 = nn.ReLU()
        # self.l5 = nn.Linear(10, 1)

    def forward(self, x):
        out1 = self.l1(x)
        out2 = self.l2(out1)
        out3 = self.l3(out2)
        # out4 = self.l4(out3)
        # out5 = self.l5(out4)
        return out3



# print(args)
# exit(0)

if args.mode == 1:
    model = poly_model_init()
elif args.mode == 2: 
    model = poly_model_less()
else:
    model = Net()


train_loader, test_loader = getDataLoader(args.mode)
    
criterion = nn.MSELoss()
optimizer = torch.optim.SGD(model.parameters(), lr=1e-3)

model.train()
epoch = args.epoch
# while True:
for i in range(epoch):
    loss_sum = 0
    for batch_x, batch_y in train_loader:
        output = model(batch_x)
        loss = criterion(output, batch_y)
        loss_sum += loss.data
        # print_loss = loss.data
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        # print('Loss: {:.6f} after {} batches'.format(loss, epoch))
        # print('==> Learned function:\t' + poly_desc(model.poly.weight.view(-1), model.poly.bias))
    print('Loss: {:.9f} after {} epoch'.format(loss_sum / len(train_loader), i))
    # epoch += 1
    #     if print_loss < 1e-3:
    #         break
    # if print_loss < 1e-3:
    #     print()
    #     print("==========End of Training==========")
    #     break

if args.mode == 1:
    print('weight y1', model.y1.weight, model.y1.bias)
    print('weight y2', model.y2.weight, model.y2.bias)
elif args.mode == 2:
    print('weight y1', model.y1.weight, model.y1.bias)
    print('weight y2', model.y2.weight, model.y2.bias) 
else:
    print('weight l1', model.l1.weight, model.l1.bias)
    print('weight l3', model.l3.weight, model.l3.bias)
    
print('=========eval==========')
model.eval()
cnt = 0
MSEloss = 0
for eval_x, eval_y in test_loader:
    cnt += 1
    out = model(eval_x)
    loss = criterion(out, eval_y)
    MSEloss += loss.data
    # print(loss)
    
print('MSE loss of eval is:', MSEloss / cnt)