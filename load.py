
from random import random, shuffle
import torch
# 读取数据
import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split



def getDataLoader(mode=1):
  # 读取csv 格式文件
  if mode == 1:
    dt = pd.read_csv("./websk_1_nor.csv")
  elif mode == 2:
    dt = pd.read_csv('./websk_2_nor.csv')
  else:
    dt = pd.read_csv('./websk_3_nor.csv')
  dt.head()
  # print(dt.head())   #打印标记文件头
  data_set = dt.values
  # print(data_set)
  X = data_set[:,:-1].astype(float)  # X输入的数据点（向量值），前n- 列都是输入X： 最后一列是输出： Y
  # print(X)
  Y = data_set[:,-1:].astype(float) # Y 是输出输出结果，取出最后一列的值 [-1:]
  # print(Y)

  X_train ,X_test,Y_train,Y_test =train_test_split(X,Y,test_size= 0.2,shuffle=True)  # 设置测试集百分比
  X_train, Y_train = torch.FloatTensor(X_train),torch.FloatTensor(Y_train)     #训练集的输入： tensor浮点数 训练集的输出： tensor 整数
  X_test,  Y_test= torch.FloatTensor(X_test),torch.FloatTensor(Y_test)         #测试集的输入： tensor浮点数 测试集的输出： tensor 整数

  # print('len X_train = ',len(X_train))
  # #转换韦pytorch 的张量
  # print('X_train = ',X_train)
  # print('X_test = ',X_test)
  # print('Y_train = ',Y_train)
  # print('Y_test = ',Y_test)

  train_dataset =  torch.utils.data.TensorDataset(X_train,Y_train)
  print(train_dataset)
  test_dataset = torch.utils.data.TensorDataset(X_test,Y_test)
  # print(test_dataset)

  train_loader = torch.utils.data.DataLoader(dataset =train_dataset ,batch_size = 4, shuffle =True)  # 设置每次训练的数据的数目：batch_size =400
  test_loader = torch.utils.data.DataLoader(dataset =test_dataset ,batch_size = 4, shuffle =True)
  # print(train_loader)
  return train_loader


if __name__ == '__main__':
  loader = getDataLoader()
  a = loader.next()
  print(a)
  print(1111)