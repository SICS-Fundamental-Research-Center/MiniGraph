import csv
from sklearn import preprocessing
import argparse

def preprocess(filepath, mode=1):
  f1 = open(filepath)
  cr = csv.reader(f1)
  flag = True
  file = filepath.split('/')[-1].split('_')[-1]
  cpu = str(int(40 / int(file)))
  res = []
  for row in cr:
    if mode == 1:
      # control data need to be train
      t = row[3:8]
      t.append(cpu)
      t.append(row[-1])
    elif mode == 2:
      t = [row[3], row[5], cpu, row[-1]]
    else:
      t = row[3:8]
      t.append(cpu)
      t.append(row[-1])
    
    if flag:
      flag = False
      continue
    else:
      res.append(t)
  return res
  

# data = preprocess('./inputs/training_data/websk_2') + preprocess('./inputs/training_data/websk_4') + preprocess('./inputs/training_data/websk_8')
# print(data)

def getWebsk(mode=1, dataset='websk'):
  # dataset for web-sk
  if dataset == 'websk':
    data = preprocess('./inputs/training_data/websk_2', mode) + preprocess('./inputs/training_data/websk_4', mode) + preprocess('./inputs/training_data/websk_8', mode) 
    data_normal = preprocessing.scale(data)
  # dataset for clueWeb
  else:
    data = preprocess('./inputs/training_data/clueweb_2', mode) + preprocess('./inputs/training_data/clueweb_4', mode) + preprocess('./inputs/training_data/clueweb_8', mode) 
    data_normal = preprocessing.scale(data)
  return data_normal

def mergeWebsk(outpath='./websk.csv', mode=1, dataset='websk'):
  data = getWebsk(mode, dataset)
  f2 = open(outpath, 'w')
  cw = csv.writer(f2)
  cw.writerows(data)
  
  
if __name__ == '__main__':
  # getWebsk(mode=1)
  parser = argparse.ArgumentParser(description='cost model')
  parser.add_argument('--mode', type=int, default=1)
  parser.add_argument('--dataset', type=str, default='websk')

  args = parser.parse_args()

  if args.mode == 1:
    mergeWebsk('./websk_1_nor.csv', mode=1, dataset=args.dataset)
  elif args.mode == 2:
    mergeWebsk('./websk_2_nor.csv', mode=2, dataset=args.dataset)
  else: 
    mergeWebsk('./websk_3_nor.csv', mode=3, dataset=args.dataset)