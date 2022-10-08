# MiniGraph

## Build

First, clone the project and install dependencies on your system.

```shell
# Clone the project (SSH).
# Make sure you have your public key has been uploaded to GitHub!
git clone git@github.com:SICS-Fundamental-Research-Center/MiniGraph.git
# Install dependencies.
$cd MiniGraph
$./dependencies.sh
```

Build the project.

```shell
$cd build
$cmake ..
$make
```
# Running MiniGraph Applications


## Partition & convert graph.
We store graphs in a binary CSR format. Edge-list in csv format can be converted to our CSR format with graph_convert tool provided in tools. You can use graph_convert_exec as follows:
```shell
$ ./bin/graph_convert_exec -t csr_bin -p -n [the number of fragments] -i [input in csv format] -o [output path] -init_model val -init_val 0 -vertexes [the maximum vid] -cores [degree of parallelism]
```


## Executing WCC

```shell
$./bin/wcc_vc_exec -i [workspace] -lc [The number of LoadComponent] -cc [The number of ComputingComponent] -dc [The number of DischargeComponent] -buffer_size [The size of task queue] -vertexes [the maximum vid]  -cores [Degree of parallelism]
```
