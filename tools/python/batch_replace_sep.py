import pandas as pd
import sys
from os import listdir
from os.path import join, isfile


argv = sys.argv[1:]

in_dir_pt = argv[0]
out_dir_pt = argv[1]
print("#### Input path: ", in_dir_pt, " Output path:", out_dir_pt, " ####")


list_dir = listdir(in_dir_pt)
print(list_dir)

final_data = pd.DataFrame()
for file_name in list_dir:
    in_pt = in_dir_pt + file_name
    out_pt = out_dir_pt + file_name
    print(in_pt)
    data = pd.read_csv(in_pt, sep="\t",header=None)
    print(data)
    final_data = pd.concat([final_data,data], axis=0)
    
print(final_data)
final_data.to_csv(out_dir_pt, sep = " ", index=0)
