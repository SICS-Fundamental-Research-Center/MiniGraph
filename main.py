import torch
from torch.autograd import Variable
from torch import nn
from preprocessing import getWebsk
from torch.utils.data import Dataset, DataLoader
from load import getDataLoader
import argparse
import numpy as np
import random

def setup_seed(seed):
     torch.manual_seed(seed)
     torch.cuda.manual_seed_all(seed)
     np.random.seed(seed)
     random.seed(seed)
     torch.backends.cudnn.deterministic = True
# 设置随机数种子
setup_seed(20)

class TimeSeriesDataSet(Dataset):
  """
  This is a custom dataset class. It can get more complex than this, but simplified so you can understand what's happening here without
  getting bogged down by the preprocessing
  """
  def __init__(self, X, Y):
    self.X = X
    self.Y = Y
    if len(self.X) != len(self.Y):
      raise Exception("The length of X does not match the length of Y")

  def __len__(self):
    return len(self.X)

  def __getitem__(self, index):
    # note that this isn't randomly selecting. It's a simple get a single item that represents an x and y
    _x = self.X[index]
    _y = self.Y[index]

    return _x, _y


# The Dataloader class handles all the shuffles for you
# loader = iter(DataLoader(TimeSeriesDataSet(x_train, y_train), batch_size=32, shuffle=True))



def make_features(x):
    x = x.unsqueeze(1)
    return torch.cat([x ** i for i in range(1, POLY_DEGREE+1)], 1)
  
W_target = torch.FloatTensor([3,6,2]).unsqueeze(1)
b_target = torch.FloatTensor([8])

def f(x):
    return x.mm(W_target) + b_target.item()
  
def get_batch(batch_size=64):
    random = torch.randn(batch_size)
    x = make_features(random)
    # print(x.shape)
    y = f(x)  # + torch.rand(1)
    return Variable(x), Variable(y)
  
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
        self.y1 = nn.Linear(3,1)
        self.y2 = nn.Linear(3,1)

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


class Net(nn.Module):
    def __init__(self):
        super(Net,self).__init__()
        self.l1 = nn.Linear(6, 8)
        self.l2 = nn.ReLU()
        self.l3 = nn.Linear(8, 1)

    def forward(self, x):
        out1 = self.l1(x)
        out2 = self.l2(out1)
        out3 = self.l3(out2)
        return out3


parser = argparse.ArgumentParser(description='cost model')
parser.add_argument('--mode', type=int, default=1)

args = parser.parse_args()
# print(args)
# exit(0)

if args.mode == 1:
    model = poly_model_init()
elif args.mode == 2: 
    model = poly_model_less()
else:
    model = Net()


loader = getDataLoader(args.mode)
    
criterion = nn.MSELoss()
optimizer = torch.optim.SGD(model.parameters(), lr=1e-3)
  
epoch = 0
# while True:
n_iter = 1000
for i in range(n_iter):
    for batch_x, batch_y in loader:
        output = model(batch_x)
        loss = criterion(output, batch_y)
        print_loss = loss.data
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        print('Loss: {:.6f} after {} batches'.format(loss, epoch))
        # print('==> Learned function:\t' + poly_desc(model.poly.weight.view(-1), model.poly.bias))
    # epoch += 1
        if print_loss < 1e-3:
            break
    if print_loss < 1e-3:
        print()
        print("==========End of Training==========")
        break

if args.mode == 1:
    print('Loss: {:.6f} after {} batches'.format(loss, epoch))
    print('weight y1', model.y1.weight, model.y1.bias)
    print('weight y2', model.y2.weight, model.y2.bias)
elif args.mode == 2:
    print('Loss: {:.6f} after {} batches'.format(loss, epoch))
    print('weight y1', model.y1.weight, model.y1.bias)
    print('weight y2', model.y2.weight, model.y2.bias) 
else:
    print('Loss: {:.6f} after {} batches'.format(loss, epoch))
    print('weight y1', model.l1.weight, model.l1.bias)
    print('weight y2', model.l3.weight, model.l3.bias)