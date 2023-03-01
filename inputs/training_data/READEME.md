# Dataset Information

This dataset consists of runtime information from running MiniGraph. It consists of tuples

```python
[inc_type, niters, n_active_vertexes, sum_dlv, sum_dgv, sum_dlv_times_dlv, sum_dlv_times_dgv, sum_dgv_times_dgv,
 sum_in_border_vertexes, sum_out_border_vertexes, num_edges, num_vertexes, elapsed_time]
```

# Explanation of the Tuple Attributes

The following are the selected tuple attributes and their meanings:

- 'inc_type': This indicates whether the tuple is from IncEval or not. 'type=1' means the tuple is collected from
  IncEval, while 'inc_type=0' means it is from PEval.
- 'niters': This attribute indicates the number of iterations within an IncEval or PEval function.
- 'sum_dlv': This attribute represents the sum degree in $F_i$. Formally, it is defined as $|{u|(u,v)\in E_i}|$.
- 'sum_dgv': This attribute represents the sum degree in $G$. Formally, it is defined as $|{u|(u,v)\in E}|$.
- 'elapse_time': This attribute records the running time from the beginning of PEval (resp. IncEval) to the end of the
  PEval (resp. IncEval).



