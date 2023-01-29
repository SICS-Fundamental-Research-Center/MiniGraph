import pandas as pd
import sys
argv = sys.argv[1:]
in_pt = argv[0]
out_pt = argv[1]
print("#### Input path: ", in_pt, " Output path:", out_pt, " ####")
data = pd.read_csv(in_pt, sep="\t")
print(data)
data.to_csv(out_pt, sep = " ", index=0)

