#ifndef MINIGRAPH_2D_PIE_VERTEX_MAP_REDUCE_H
#define MINIGRAPH_2D_PIE_VERTEX_MAP_REDUCE_H

#include <condition_variable>
#include <vector>

#include <folly/MPMCQueue.h>
#include <folly/ProducerConsumerQueue.h>
#include <folly/concurrency/DynamicBoundedQueue.h>

#include "executors/task_runner.h"
#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/thread_pool.h"
#include <folly/executors/ThreadPoolExecutor.h>


namespace minigraph {

template <typename GRAPH_T, typename CONTEXT_T>
class VMapBase {
  using VertexInfo =
      graphs::VertexInfo<typename GRAPH_T::vid_t, typename GRAPH_T::vdata_t,
                         typename GRAPH_T::edata_t>;
  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

 public:
  VMapBase() = default;
  VMapBase(const CONTEXT_T& context) { context_ = context; };
  ~VMapBase() = default;

  Frontier* Map(Frontier* frontier_in, bool* visited, GRAPH_T& graph,
                executors::TaskRunner* task_runner) {
    if (visited == nullptr) {
      LOG_INFO("Segmentation fault: ", "visited is nullptr.");
    }

    Frontier* frontier_out = new Frontier(graph.get_num_vertexes() + 1);

    VertexInfo vertex_info;
    std::vector<std::function<void()>> tasks;
    tasks.reserve(65536);
    while (!frontier_in->empty()) {
      frontier_in->dequeue(vertex_info);
      auto task = std::bind(&VMapBase<GRAPH_T, CONTEXT_T>::Reduce, this,
                            vertex_info, &graph, frontier_out, visited);
      tasks.push_back(task);
    }
    LOG_INFO("VMap Run: ", tasks.size());
    task_runner->Run(tasks, false);
    LOG_INFO("#");
    delete frontier_in;
    return frontier_out;
  }

 protected:
  CONTEXT_T context_;

 private:
  virtual bool F(VertexInfo& vertex_info, GRAPH_T* graph = nullptr) = 0;
  virtual bool C(const VertexInfo& vertex_info) = 0;

  void Reduce(VertexInfo& u, GRAPH_T* graph, Frontier* frontier_out,
              bool* visited) {
    if (C(u)) {
      if (F(u, graph)) {
        frontier_out->enqueue(u);
        if (!visited[u.vid]) {
          visited[u.vid] = true;
        }
      }
    }
  }
};

}  // namespace minigraph
#endif  // MINIGRAPH_2D_PIE_VERTEX_MAP_REDUCE_H
