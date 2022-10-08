# tools/compacted_graph.cpp
tools/compacted_graph.cpp prvides a simple method to compact graph. The method do not change the topology of the graph.

### Useage
To use this method, one should only provides the input path and the outout path.

```shell
$./bin/compacted_graph_exec -i [input path] -o [output paht] -cores [the degree of parallelism] -sep [seperator, e.g. "," in csv format]
```
