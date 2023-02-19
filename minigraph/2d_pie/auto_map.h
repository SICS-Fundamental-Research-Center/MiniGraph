#ifndef MINIGRAPH_2D_PIE_AUTO_MAP_REDUCE_H
#define MINIGRAPH_2D_PIE_AUTO_MAP_REDUCE_H

#include <condition_variable>
#include <functional>
#include <future>
#include <vector>

#include <folly/MPMCQueue.h>
#include <folly/ProducerConsumerQueue.h>
#include <folly/concurrency/DynamicBoundedQueue.h>
#include <folly/executors/ThreadPoolExecutor.h>

#include "executors/task_runner.h"
#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/atomic.h"
#include "utility/bitmap.h"
#include "utility/thread_pool.h"

namespace minigraph {

template <typename GRAPH_T, typename CONTEXT_T>
class AutoMapBase {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo =
      graphs::VertexInfo<typename GRAPH_T::vid_t, typename GRAPH_T::vdata_t,
                         typename GRAPH_T::edata_t>;

 public:
  AutoMapBase() = default;
  ~AutoMapBase() = default;

  virtual bool F(const VertexInfo& u, VertexInfo& v,
                 GRAPH_T* graph = nullptr) = 0;

  virtual bool F(VertexInfo& u, GRAPH_T* graph = nullptr,
                 VID_T* vid_map = nullptr) = 0;

  bool ActiveEMap(Bitmap* in_visited, Bitmap* out_visited, GRAPH_T& graph,
                  executors::TaskRunner* task_runner, VID_T* vid_map = nullptr,
                  Bitmap* visited = nullptr) {
    assert(task_runner != nullptr);
    if (in_visited == nullptr || out_visited == nullptr) {
      LOG_INFO("Segmentation fault: ", "visited is nullptr.");
    }
    out_visited->clear();
    std::vector<std::function<void()>> tasks;
    bool global_visited = false;
    size_t active_vertices = 0;
    for (size_t tid = 0; tid < task_runner->GetParallelism(); ++tid) {
      auto task = std::bind(&AutoMapBase<GRAPH_T, CONTEXT_T>::ActiveEReduce,
                            this, &graph, in_visited, out_visited, tid,
                            task_runner->GetParallelism(), &global_visited,
                            &active_vertices, vid_map, visited);
      tasks.push_back(task);
    }
    LOG_INFO("AutoMap ActiveEMap Run");
    task_runner->Run(tasks, false);
    LOG_INFO("# ", active_vertices);
    return global_visited;
  };

  bool ActiveVMap(Bitmap* in_visited, Bitmap* out_visited, GRAPH_T& graph,
                  executors::TaskRunner* task_runner, VID_T* vid_map,
                  Bitmap* visited) {
    assert(task_runner != nullptr);
    if (in_visited == nullptr || out_visited == nullptr) {
      LOG_INFO("Segmentation fault: ", "visited is nullptr.");
    }
    out_visited->clear();
    std::vector<std::function<void()>> tasks;
    bool global_visited = false;
    size_t active_vertices = 0;
    for (size_t tid = 0; tid < task_runner->GetParallelism(); ++tid) {
      auto task = std::bind(&AutoMapBase<GRAPH_T, CONTEXT_T>::ActiveVReduce,
                            this, &graph, in_visited, out_visited, tid,
                            task_runner->GetParallelism(), &global_visited,
                            &active_vertices, vid_map, visited);
      tasks.push_back(task);
    }
    LOG_INFO("AutoMap ActiveVMap Run");
    task_runner->Run(tasks, false);
    LOG_INFO("# ", active_vertices);
    return global_visited;
  };

  template <class F, class... Args>
  auto ActiveMap(GRAPH_T& graph, executors::TaskRunner* task_runner,
                 Bitmap* visited, F&& f, Args&&... args) -> void {
    assert(task_runner != nullptr);
    std::vector<std::function<void()>> tasks;
    for (size_t tid = 0; tid < task_runner->GetParallelism(); ++tid) {
      auto task = std::bind(f, &graph, tid, visited,
                            task_runner->GetParallelism(), args...);
      tasks.push_back(task);
    }
    LOG_INFO("AutoMap ActiveMap Run");
    task_runner->Run(tasks, false);
    LOG_INFO("# ");
    return;
  };

  template <class F, class... Args>
  auto VMap(GRAPH_T& graph, executors::TaskRunner* task_runner, Bitmap* visited,
            Bitmap* in_visited, Bitmap* out_visited, F&& f, Args&&... args)
      -> void {
    assert(task_runner != nullptr);
    std::vector<std::function<void()>> tasks;
    size_t active_vertices = 0;
    for (size_t tid = 0; tid < task_runner->GetParallelism(); ++tid) {
      auto task = std::bind(&AutoMapBase<GRAPH_T, CONTEXT_T>::VReduce, this, f,
                            &graph, tid, task_runner->GetParallelism(),
                            in_visited, out_visited, visited);
      tasks.push_back(task);
    }
    LOG_INFO("AutoMap ActiveMap Run");
    task_runner->Run(tasks, false);
    LOG_INFO("# ", active_vertices);
    return;
  };

 private:
  void ActiveEReduce(GRAPH_T* graph, Bitmap* in_visited, Bitmap* out_visited,
                     const size_t tid, const size_t step, bool* global_visited,
                     size_t* active_vertices, VID_T* vid_map = nullptr,
                     Bitmap* visited = nullptr) {
    size_t local_active_vertices = 0;
    for (size_t index = tid; index < graph->get_num_vertexes(); index += step) {
      if (in_visited->get_bit(index) == 0) continue;
      VertexInfo&& u = graph->GetVertexByIndex(index);
      for (size_t i = 0; i < u.outdegree; ++i) {
        if (!graph->IsInGraph(u.out_edges[i])) continue;
        VID_T local_id = VID_MAX;
         if(vid_map != nullptr)
           local_id = vid_map[u.out_edges[i]];
         else
          local_id = graph->globalid2localid(u.out_edges[i]);
        VertexInfo&& v = graph->GetVertexByVid(local_id);
        if (F(u, v)) {
          out_visited->set_bit(local_id);
          visited->set_bit(local_id);
          *global_visited == true ? 0 : *global_visited = true;
          ++local_active_vertices;
        }
      }
    }
    write_add(active_vertices, local_active_vertices);
  }

  void ActiveVReduce(GRAPH_T* graph, Bitmap* in_visited, Bitmap* out_visited,
                     const size_t tid, const size_t step, bool* global_visited,
                     size_t* active_vertices, VID_T* vid_map,
                     Bitmap* visited = nullptr) {
    size_t local_active_vertices = 0;
    for (size_t index = tid; index < graph->get_num_vertexes(); index += step) {
      if (!in_visited->get_bit(index)) continue;
      if (!graph->IsInGraph(index)) continue;
      VertexInfo&& u = graph->GetVertexByIndex(index);
      if (F(u, graph, vid_map)) {
        for (size_t j = 0; j < u.outdegree; j++) {
          if (graph->IsInGraph(u.out_edges[j]))
            out_visited->set_bit(vid_map[u.out_edges[j]]);
        }
        out_visited->set_bit(u.vid);
        visited->set_bit(u.vid);
        *global_visited == true ? 0 : *global_visited = true;
        ++local_active_vertices;
      }
    }
    write_add(active_vertices, local_active_vertices);
  }

  template <class F, class... Args>
  void VReduce(F&& f, GRAPH_T* graph, const size_t tid, const size_t step,
               Bitmap* in_visited, Bitmap* out_visited, Bitmap* visited,
               size_t* active_vertices, Args&&... args) {
    size_t local_active_vertices = 0;
    for (size_t index = tid; index < graph->get_num_vertexes(); index += step) {
      if (!in_visited->get_bit(index)) continue;
      if (!graph->IsInGraph(index)) continue;
      VertexInfo&& u = graph->GetVertexByIndex(index);
      if (f(u, args...)) {
        visited->set_bit(index);
        out_visited->set_bit(index);
      }
    }
    write_add(active_vertices, local_active_vertices);
  }
};

}  // namespace minigraph
#endif  // MINIGRAPH_2d_PIE_EDGE_MAP_REDUCE_H
