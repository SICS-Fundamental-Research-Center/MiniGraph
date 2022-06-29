#ifndef MINIGRAPH_2D_PIE_EDGE_MAP_REDUCE_H
#define MINIGRAPH_2D_PIE_EDGE_MAP_REDUCE_H

#include "executors/task_runner.h"
#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/thread_pool.h"
#include <folly/MPMCQueue.h>
#include <folly/ProducerConsumerQueue.h>
#include <folly/concurrency/DynamicBoundedQueue.h>
#include <folly/executors/ThreadPoolExecutor.h>
#include <condition_variable>
#include <functional>
#include <future>
#include <vector>

namespace minigraph {

template <typename GRAPH_T, typename CONTEXT_T>
class EMapBase {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo =
      graphs::VertexInfo<typename GRAPH_T::vid_t, typename GRAPH_T::vdata_t,
                         typename GRAPH_T::edata_t>;
  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

 public:
  EMapBase() = default;
  EMapBase(const CONTEXT_T context) { context_ = context; };
  ~EMapBase() = default;

  Frontier* Map(Frontier* frontier_in, bool* visited, GRAPH_T& graph,
                executors::TaskRunner* task_runner) {
    if (visited == nullptr) {
      LOG_INFO("Segmentation fault: ", "visited is nullptr.");
    }
    Frontier* frontier_out =
        new Frontier(graph.get_num_vertexes() + frontier_in->size() + 1024);
    VertexInfo u;
    std::vector<std::function<void()>> tasks;
    tasks.reserve(graph.get_num_vertexes() + frontier_in->size());
    while (!frontier_in->empty()) {
      frontier_in->dequeue(u);
      auto task = std::bind(&EMapBase<GRAPH_T, CONTEXT_T>::Reduce, this, u,
                            &graph, frontier_out, visited);
      tasks.push_back(task);
    }
    LOG_INFO("EMap Run: ", tasks.size());
    task_runner->Run(tasks, false);
    delete frontier_in;
    LOG_INFO("#");
    return frontier_out;
  };

  // template <class F, class... Args>
  // Frontier* Map(Frontier* frontier_in, bool* visited, GRAPH_T& graph,
  //               executors::TaskRunner* task_runner, F&& f, Args&&... args) {
  //   Frontier* frontier_out = new Frontier(graph.get_num_vertexes() + 1);
  //   VertexInfo vertex_info;
  //   std::vector<std::function<void()>> tasks;
  //   size_t tid = 0;
  //   while (!frontier_in->empty()) {
  //     frontier_in->dequeue(vertex_info);
  //     auto task = std::bind(f, tid++, frontier_out, vertex_info, args...);
  //     tasks.push_back(task);
  //   }
  //   LOG_INFO("EMap Run: ", tasks.size());
  //   task_runner->Run(tasks, false);
  //   delete frontier_in;
  //   LOG_INFO("#");
  //   return frontier_out;
  // }

  template <typename T, class F, class... Args>
  auto Map(folly::DMPMCQueue<T, false>* frontier_in, GRAPH_T& graph,
           executors::TaskRunner* task_runner, F&& f, Args&&... args)
      -> folly::DMPMCQueue<T, false>* {
    folly::DMPMCQueue<T, false>* frontier_out =
        new folly::DMPMCQueue<T, false>(graph.get_num_vertexes() + 1024);
    T t;
    std::vector<std::function<void()>> tasks;
    tasks.reserve(graph.get_num_vertexes() + 1024);
    size_t tid = 0;
    while (!frontier_in->empty()) {
      frontier_in->dequeue(t);
      auto task = std::bind(f, tid++, t, args...);
      tasks.push_back(task);
    }
    LOG_INFO("EMap Run: ", tasks.size());
    task_runner->Run(tasks, false);
    LOG_INFO("#");
    delete frontier_in;
    return frontier_out;
  }

  CONTEXT_T context_;

  virtual bool F(const VertexInfo& u, VertexInfo& v) = 0;
  virtual bool C(const VertexInfo& u, const VertexInfo& v) = 0;

  void Reduce(VertexInfo& u, GRAPH_T* graph, Frontier* frontier_out,
              bool* visited) {
    for (size_t i = 0; i < u.outdegree; i++) {
      auto local_id = graph->globalid2localid(u.out_edges[i]);
      if (local_id == VID_MAX) {
        continue;
      }
      VertexInfo&& v = graph->GetVertexByVid(local_id);
      if (C(u, v)) {
        if (F(u, v)) {
          // LOG_INFO("Enqueue: ", v.vid);
          frontier_out->enqueue(v);
          if (!visited[local_id]) {
            visited[local_id] = true;
          }
        }
      }
    }
  }
};

}  // namespace minigraph
#endif  // MINIGRAPH_2d_PIE_EDGE_MAP_REDUCE_H
