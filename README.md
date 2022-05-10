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
#Running Galois Applications


##Partition input.
```shell
$cd bin
$./preprocess_exec  -p -n 1 -i [input in csv format] -o [output path]
```

##Executing MiniGraph

```shell
$cd bin
$[App] -i [workspace] -lc [The number of LoadComponent] -cc [The number of ComputingComponent] -dc [The number of DischargeComponent] -threads [Size of CPUThreadsPool]
```
