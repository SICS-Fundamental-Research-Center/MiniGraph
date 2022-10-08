# tools/graph_gen.cpp
tools/graph_gen.cpp privides a family of graph generators for quickly generating realistic graph that match the power law behaviors. 


### Useage
For example, to use R-MAT generator, one should first set 4 probabilities (a,b,c and d) in which (a+b+c+d = 1).
Here, a and b correspod to the ratio of falling into separate groups of vertexes.
b and c are the cross links between a and b.

It may have duplicate edges, but the generator only keep one of them.
```shell
$cd bin
$./graph_gen_exec -a [probability] -b [probability] -c [probability] -d [probability] -o [output file] -power [power of 2] -edges [the count of edges]
```
or
```shell
$cd bin
$./graph_gen_exec -x [probability] -y [probability] -o [output file] -power [power of 2] -edges [the count of edges]
```

