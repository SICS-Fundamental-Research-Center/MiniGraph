# Dataset Information
This dataset consists of runtime information from running MiniGraph. 
It consists of tuples <type, level, current_niter, total_niters, sum_indegree, sum_outdegree, num_avtive_vertexes, elapsed_time>.
Among these, 'level' denotes where the information collected from (type = 0 means this tuple is from avtive vertexes-level, type = 1 denotes that the tuple is from fragement-level).
'type' indicates whether tuple is from IncEval or not, in which type=1 means the tuple is collected in IncEval, type=0 means from PEval.
