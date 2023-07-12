import pandas as pd
import sys
argv = sys.argv[1:]
in_pt = argv[0]
out_pt = argv[1]
sep = argv[2]
print("#### Input path: ", in_pt, " Output path:", out_pt, " ####")
data = pd.read_csv(in_pt, sep=sep)
print(data)
data = data.drop(list(data)[2:], axis=1)
print(data)
data.to_csv(out_pt, sep = sep, index=0)

