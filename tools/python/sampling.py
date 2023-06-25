import pandas as pd
import sys
argv = sys.argv[1:]
in_pt = argv[0]
out_pt = argv[1]
sep = argv[2]
r = float(argv[3])
print("#### Input path: ", in_pt, " Output path:", out_pt, " sep: ", sep, " Sampling rate: ", r, " ####")
data = pd.read_csv(in_pt, sep)
print(data)
n = int(data.shape[0] * r )
data = data.head(n)


print(data)
data.to_csv(out_pt, sep = sep, index=0)

