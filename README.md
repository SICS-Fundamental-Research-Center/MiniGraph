# MiniGraph overview

## What is MiniGraph?
MiniGraph is an out-of-core system for graph computations with a single machine.
This repository is the main codebase for the MiniGraph project, 
containing both the utility used to generate and convert graphs
as well as the main graph engine, MiniGraph.

## Features
Compared to the existing out-of-core single-machine
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
This said, we develop efficient heuristics to allocate resources,
which is dynamically adapted based on resource availability.
* System optimizations. MiniGraph develops unique optimization
strategies enabled by the hybrid parallel model. It employs a lightweight
state machine to model the progress of cores working on
different subgraphs and tracks messages between cores. These
allow it to explore shortcuts in the process to avoid redundant I/O.


 



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
* jemalloc (>=5.30)






### Build

First, clone the project and install dependencies on your environment.

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


#### Preparation: Partition & convert graph.
We store graphs in a binary CSR format. Edge-list in CSV format 
can be converted to our CSR format with graph_convert tool provided in tools.
You can use graph_convert_exec as follows:
```shell
$./bin/graph_partition_exec -t csr_bin -p -n  [the number of fragments] -i [graph in csv format] -sep [seperator, e.g. ","] -o [workspace]  -cores [degree of parallelism] -tobin -partitioner ["vertexcut" or "edgecut"]
```

#### Executing 
Implementations of five graph applications 
(PageRank, Connected Components, 
Single-Source Shortest Paths, 
Breadth-First Search, Simulation) are included in the apps/cpp/ directory.


For instance to run a WCC workload.
You may adjust the number of evaluators to control 
inter graphs parallelism and varying the degree of total parallelism 
by specifying parameters "-cc" and "-cores" as follows:

```shell
$cd $SRC_DIR
$./bin/wcc_vc_exec  -i [workspace] -cc [The number of ComputingComponent] -buffer_size [The size of task queue] -cores [Degree of parallelism]
```
"buffer_size" is used to control the number of fragment that can residented in memory.

Other applications, such as Simulation require the input pattern. 
User should provide pattern by -pattern [pattern in CSV format] command.
For example,

```shell
$cd $SRC_DIR
$./bin/sim_vc_exec  -i [workspace] -cc [The number of ComputingComponent] -buffer_size [The size of task queue] -cores [Degree of parallelism] -pattern [pattern in csv format]
```

#### Demo: WCC on road-Net
```shell
$cd $SRC_DIR
$./bin/graph_partition_exec -t csr_bin -p -n 1 -i inputs/roadNet-CA.csv -sep "," -o inputs/workspace/ -cores 10 -tobin -partitioner vertexcut
$./bin/wcc_vc_exec -i inputs/workspace/ -cc 1 -buffer_size 1 -cores 4
```

## Writing Your Own Graph Algorithms: WCC
In addition, users can write their algorithms in MiniGraph. 
Currently, MiniGraph supports users to write their  algorithms in PIE model 
and PIE+ model.
Next, we will walk you through a concrete example of WCC 
to illustrate how MiniGraph can be used by developers to effectively 
analyze large graphs.

One method of computing the WCCs of a graph
is to initialize each vertex to its IDs, 
and iteratively have every vertex update its IDs entry to be the
minimum IDs entry of all of its neighbors in $G$. 


To implement this algorithm in PIE+, users just need to fulfill the following 
two classes, i.e.  AutoMapBase and AutoAppBase.

```c++
template <typename GRAPH_T, typename CONTEXT_T>
class WCCAutoMap : public minigraph::AutoMapBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;

 public:
  WCCAutoMap() : minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>() {}

  // EMap applies the function F to all edges
  bool F(const VertexInfo& u, VertexInfo& v,
         GRAPH_T* graph = nullptr) override {
    return false;
  }

  // VMap applies the function F to all vertex
  bool F(VertexInfo& u, GRAPH_T* graph = nullptr,
         VID_T* vid_map = nullptr) override {
    return false;
  }

};

template <typename GRAPH_T, typename CONTEXT_T>
class WCCPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;


 public:
  WCCPIE(minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
         const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(auto_map, context) {}


  bool Init(GRAPH_T& graph,
            minigraph::executors::TaskRunner* task_runner) override {
            // Insert your code here
  }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::Task Runner* task_runner) override {
             // Insert your code heree
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
               // Insert your code here
  }

  bool Aggregate(void* a, void* b,
                 minigraph::executors::TaskRunner* task_runner) override {
                 // Insert your code here
  }
};
```

### Fulfill Init Function
The Init function are mainly responsable for setting the initial value for 
each node & edges.
Users could use auto_map_->ActiveMap() to do this in parallel.
Here the Init function sets the initial value for each vertex u by 
graph->localid2globalid(u.vid)

```c++
template <typename GRAPH_T, typename CONTEXT_T>
class WCCAutoMap : public minigraph::AutoMapBase<GRAPH_T, CONTEXT_T> {
 public:
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
 public:
  bool Init(GRAPH_T& graph,
            minigraph::executors::TaskRunner* task_runner) override {
    Bitmap* visited = new Bitmap(graph.max_vid_);
    visited->fill();
    this->auto_map_->ActiveMap(graph, task_runner, visited,
                               WCCAutoMap<GRAPH_T, CONTEXT_T>::kernel_init);
    delete visited;
    return true;
  }
};
```

### Fulfill PEval Function
In PEval of WCC, 
it gets the vertex that has minimum IDs in $G$ by 
this->context_.root_id.
PEval checks each fragment whether it contains the root_id by
!graph.IsInGraph(this->context_.root_id). 

If a fragment contains the root_id, 
it will traverse the outgoing edges of the root_id by auto_map_->ActiveEMap.
Then F is applied to each vertex stem from root_id.
It updates the value of v if it is large than the value of u by write_min.
Finally, kernel_pull_border_vertexes update all labels of border vertices 
in parallel.

```c++
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
    return write_min(v.vdata, u.vdata[0]);
  }
  
  static bool kernel_push_border_vertexes(GRAPH_T* graph, const size_t tid,
                                          Bitmap* visited, const size_t step,
                                          Bitmap* global_border_vid_map,
                                          VDATA_T* global_border_vdata) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      if (global_border_vid_map->get_bit(graph->localid2globalid(u.vid)) == 0)
        continue;
      auto global_id = graph->localid2globalid(u.vid);
      write_min((global_border_vdata + global_id), u.vdata[0]);
    }
    return true;
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class WCCPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
 public:
  WCCPIE(minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
         const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(auto_map, context) {}

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
    if (!graph.IsInGraph(this->context_.root_id)) return false;
    auto vid_map = this->msg_mngr_->GetVidMap();
    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    in_visited->fill();
    out_visited->clear();
    Bitmap visited(graph.get_num_vertexes());
    visited.clear();
    bool run = true;
    while (run) {
      run = this->auto_map_->ActiveEMap(in_visited, out_visited, graph,
                                        task_runner, vid_map, &visited);
      std::swap(in_visited, out_visited);
    }
    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        WCCAutoMap<GRAPH_T, CONTEXT_T>::kernel_push_border_vertexes,
        this->msg_mngr_->GetGlobalBorderVidMap(),
        this->msg_mngr_->GetGlobalVdata());

    delete in_visited;
    delete out_visited;
    return true;
  }
};

```

### Fulfill IncEval Function
The  differences between IncEval and PEval of WCC algorithm are 
(1) IncEval is invoked on each fragment, rather than only the fragment 
with root_id. 
(2) A pull operation, kernel_pull_border_vertexes,  
to capture incremental information from border vertices.

```c++

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
  static bool kernel_pull_border_vertexes(GRAPH_T* graph, const size_t tid,
                                          Bitmap* visited, const size_t step,
                                          Bitmap* in_visited,
                                          Bitmap* global_border_vid_map,
                                          VDATA_T* global_border_vdata) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      for (size_t nbr_i = 0; nbr_i < u.indegree; nbr_i++) {
        if (global_border_vid_map->get_bit(u.in_edges[nbr_i]) == 0) continue;
        if (global_border_vdata[u.in_edges[nbr_i]] < u.vdata[0]) {
          u.vdata[0] = global_border_vdata[u.in_edges[nbr_i]];
          in_visited->set_bit(u.vid);
        }
      }
    }
    return true;
  }
};


template <typename GRAPH_T, typename CONTEXT_T>
class WCCPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {

 public:
  WCCPIE(minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
         const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(auto_map, context) {}


  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("IncEval() - Processing gid: ", graph.gid_);
    Bitmap visited(graph.get_num_vertexes());
    visited.clear();
    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    auto vid_map = this->msg_mngr_->GetVidMap();

    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        WCCAutoMap<GRAPH_T, CONTEXT_T>::kernel_pull_border_vertexes, in_visited,
        this->msg_mngr_->GetGlobalBorderVidMap(),
        this->msg_mngr_->GetGlobalVdata());

    bool run = true;
    while (run) {
      run = this->auto_map_->ActiveEMap(in_visited, out_visited, graph,
                                        task_runner, vid_map, &visited);
      std::swap(in_visited, out_visited);
    }

    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        WCCAutoMap<GRAPH_T, CONTEXT_T>::kernel_push_border_vertexes,
        this->msg_mngr_->GetGlobalBorderVidMap(),
        this->msg_mngr_->GetGlobalVdata());

    delete in_visited;
    delete out_visited;
    auto visited_num = visited.get_num_bit();
    return !visited.empty();
  }
};

```

A fragment will repeat the IncEval until there are no messages received. 
When all the fragments are finished with computation, the algorithm is terminated.

## Contact Us
For bugs, please raise an issue on GiHub. 
Questions and comments are also welcome at my email: 
zhuxk@buaa.edu.cn



