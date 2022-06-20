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


## Partition input.
To generate graph where vertexes are all 0.
```shell
$cd bin
$./preprocess_exec  -p -n [the number of fragments] -i [input in csv format] -o [output path] -init_model val -init_val 0
```
To obtain graph where vertexes are initialized to its vid (or to all maximum value).
```shell
$cd bin
$./preprocess_exec  -p -n [the number of fragments] -i [input in csv format] -o [output path] -init_model vid
%./preprocess_exec  -p -n [the number of fragments] -i [input in csv format] -o [output path] -init_model max
```

## Executing BFS

```shell
$cd bin
$./bfs_exec -i [workspace] -lc [The number of LoadComponent] -cc [The number of ComputingComponent] -dc [The number of DischargeComponent] -cores [total parallism]
```

## Executing SubIso

```shell
$cd bin
$./subiso_exec -pattern [pattern path, e.g. ../inputs/pattern/p1.csv] -i [workspace] -lc [The number of LoadComponent] -cc [The number of ComputingComponent] -dc [The number of DischargeComponent] -cores [total parallism]
```
