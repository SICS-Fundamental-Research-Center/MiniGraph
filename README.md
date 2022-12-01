# MiniGraph

## What is MiniGraph?
MiniGraph is an out-of-core system for graph computations with a single machine.
Compared to the existing out-of-core single machine
graph systems, MiniGraph has the following features.
* A pipelined architecture. MiniGraph proposes an architecture
that pipelines access to disk for read and write, and CPU operations
for query answering. The idea is to overlap I/O and CPU operations,
so as to “cancel” the excessive I/O cost. Moreover, this architecture
decouples computation from memory management and scheduling,
giving rises to new opportunities for optimizations.
* A hybrid parallel model. MiniGraph extends the graph-centric
model (GC) from multiple machines to multiple cores. GC
allows a core to operate on a subgraph at a time, speed up beyond
neighborhood computation and reduce its I/O; it also simplifies parallel
programming by parallelizing existing sequential algorithms
across cores. MiniGraph also proposes a hybrid model for VC and
GC, and a unified interface such that the users can benefit from both
and can choose one that fits their problems and graphs the best.
* Two-level parallelism. The hybrid model also enables two-level
parallelism: inter-subgraph parallelism via high-level GC abstraction,
and intra-subgraph parallelism for low-level VC operations.
This presents new opportunities for improving multi-core parallelism.
However, its relevant scheduling problem is NP-complete.
This said, we develop an efficient heuristics to allocate resources,
which is dynamically adapted based on resource availability.
* System optimizations. MiniGraph develops unique optimization
strategies enabled by the hybrid parallel model. It employs a lightweight
state machine to model the progress of cores working on
different subgraphs, and tracks messages between cores. These
allow it to explore shortcuts in the process to avoid redundant I/O.


 
This repository is the main codebase for the MiniGraph project, 
containing both the utility used to generate and convert graphs
as well as the main graph engine, MiniGraph.


## Getting Started
### Dependencies
MiniGraph builds, runs, and has been tested on GNU/Linux. 
At the minimum, MiniGraph depends on the following software:
* A modern C++ compiler compliant with the C++17 standard 
(gcc >= 9)
* CMake (>= 2.8)
* Facebook folly library (>= v2022.11.28.00)
* GoogleTest (>= 1.11.0)
* RapidCSV (>= 8.65)
* Boost::ext SML (>= 1.1.6)






### Build

First, clone the project and install dependencies on your enviroment.

```shell
# Clone the project (SSH).
# Make sure you have your public key has been uploaded to GitHub!
git clone git@github.com:SICS-Fundamental-Research-Center/MiniGraph.git
# Install dependencies.
$SRC_DIR=`MiniGraph` # top-level MiniGraph source dir
$cd $SRC_DIR
$./dependencies.sh
```

Build the project.
```shell
$BUILD_DIR=<path-to-your-build-dir>
$mkdir -p $BUILD_DIR
$cd $BUILD_DIR
$cmake ..
$make
```
### Running MiniGraph Applications


#### Preparation
##### Partition & convert graph.
We store graphs in a binary CSR format. Edge-list in csv format 
can be converted to our CSR format with graph_convert tool provided in tools.
You can use graph_convert_exec as follows:
```shell
$ ./bin/graph_convert_exec -t csr_bin -p -n [the number of fragments] -i [graph in csv format] -sep "," -o [workspace] -cores [degree of parallelism]
```

#### Executing 
Implementations of five graph applications 
(PageRank, Connected Components, 
Single-Source Shortest Paths, 
Breadth-First Search, Simulation) are inclulded in the apps/cpp/ directory.
Finally, MiniGraph is ready to run, 
e.g. to execute WCC in VC:

```shell
$cd $SRC_DIR
$./bin/wcc_vc_exec  -i [workspace] -lc [The number of LoadComponent] -cc [The number of ComputingComponent] -dc [The number of DischargeComponent] -buffer_size [The size of task queue] -cores [Degree of parallelism]
```

Other applications, such as Simulation require input pattern. 
User should provide pattern by -pattern [pattern in csv format] command.
For example,

```shell
$cd $SRC_DIR
$./bin/sim_vc_exec  -i [workspace] -lc [The number of LoadComponent] -cc [The number of ComputingComponent] -dc [The number of DischargeComponent] -buffer_size [The size of task queue] -cores [Degree of parallelism] -pattern [pattern in csv format]
```





In addition, users can write their own algorithms in MiniGraph. 
Currently, MiniGraph supports users to write their own algorithms in PIE model 
and PIE+ model.
Next, we will walk you through a concrete example to illustrate how MiniGraph 
can be used by developers to effectively  analyze large graphs.

#### Demo: WCC on roadNet-CA
Given a graph $G$, WCC is to compute the maximum subgraphs of
$G$ in which all vertices are connected to each other via a path, regardless
of the direction of edges.

To implement the PIE model, you just need to fulfill this class
'''

template <typename GRAPH_T, typename CONTEXT_T>
class WCCAutoMap : public minigraph::AutoMapBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  WCCAutoMap() : minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>() {}

  bool F(const VertexInfo& u, VertexInfo& v,
         GRAPH_T* graph = nullptr) override {
    return false;
  }

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr,
         VID_T* vid_map = nullptr) override {
    return false;
  }

  static bool kernel_init(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                          const size_t step) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByVid(i);
      graph->vdata_[i] = graph->localid2globalid(u.vid);
    }
    return true;
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class WCCPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  WCCPIE(minigraph::VMapBase<GRAPH_T, CONTEXT_T>* vmap,
         minigraph::EMapBase<GRAPH_T, CONTEXT_T>* emap,
         const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(vmap, emap, context) {}

  WCCPIE(minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
         const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(auto_map, context) {}

  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

  bool Init(GRAPH_T& graph,
            minigraph::executors::TaskRunner* task_runner) override {
            // Plast you algorithm here
  }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
            // Plast you algorithm here
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
            // Plast you algorithm here
  }

  bool Aggregate(void* a, void* b,
                 minigraph::executors::TaskRunner* task_runner) override {
    if (a == nullptr || b == nullptr) return false;
  }
};
'''






