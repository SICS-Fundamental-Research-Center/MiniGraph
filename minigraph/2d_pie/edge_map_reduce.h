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
#include <vector>

namespace minigraph {

template <typename GRAPH_T, typename CONTEXT_T>
class EdgeMapBase {
  using VertexInfo =
      graphs::VertexInfo<typename GRAPH_T::vid_t, typename GRAPH_T::vdata_t,
                         typename GRAPH_T::edata_t>;
  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

 public:
  EdgeMapBase() = default;
  EdgeMapBase(const CONTEXT_T context) { context_ = context; };
  ~EdgeMapBase() = default;

  // void Bind(GRAPH_T* graph) { graph_ = graph; }

  Frontier* EdgeMap(Frontier* frontier_in, bool* visited, GRAPH_T& graph,
                    executors::TaskRunner* task_runner) {
    if (visited == nullptr) {
      LOG_INFO("Segmentation fault: ", "visited is nullptr.");
    }
    // run vertex centric operations.
    std::condition_variable cv;
    std::mutex mtx;
    std::unique_lock<std::mutex> lck(mtx);

    std::atomic<size_t> num_finished_tasks(0);
    std::atomic<size_t> task_count(0);
    Frontier* frontier_out = new Frontier(graph.get_num_vertexes() + 1);

    VertexInfo vertex_info;
    std::vector<std::function<void()>> tasks;
    while (!frontier_in->empty()) {
      frontier_in->dequeue(vertex_info);
      task_count.fetch_add(1);
      auto task = std::bind(&EdgeMapBase<GRAPH_T, CONTEXT_T>::EdgeReduce, this,
                            vertex_info, &graph, frontier_out, visited,
                            &num_finished_tasks, &task_count, &cv, &lck);
      tasks.push_back(task);
    }
    for (size_t i = 0; i < tasks.size(); i++) {
      task_runner->Run(std::forward<std::function<void()>&&>(tasks.at(i)));
    }
    LOG_INFO("Wait: ", task_count.load());
    cv.wait(lck, [&num_finished_tasks, &task_count]() {
      return (num_finished_tasks.load(std::memory_order_acquire) ==
              task_count.load(std::memory_order_acquire))
                 ? true
                 : false;
    });
    delete frontier_in;
    return frontier_out;
  };

 protected:
  CONTEXT_T context_;

 private:
  void EdgeReduce(VertexInfo& vertex_info, GRAPH_T* graph,
                  Frontier* frontier_out, bool* visited,
                  std::atomic<size_t>* num_finished_tasks,
                  std::atomic<size_t>* task_count, std::condition_variable* cv,
                  std::unique_lock<std::mutex>* lck) {
    for (size_t i = 0; i < vertex_info.outdegree; i++) {
      auto local_id = graph->globalid2localid(vertex_info.out_edges[i]);
      if (local_id == VID_MAX) {
        continue;
      }
      VertexInfo&& ngh_vertex_info = graph->GetVertexByIndex(local_id);
      if (C(ngh_vertex_info)) {
        if (F(ngh_vertex_info)) {
          frontier_out->enqueue(ngh_vertex_info);
          if (!visited[local_id]) {
            visited[local_id] = true;
          }
        }
      }
    }
    num_finished_tasks->fetch_add(1);
    if (task_count->load(std::memory_order_acquire) ==
        num_finished_tasks->load(std::memory_order_acquire)) {
      LOG_INFO("finished", task_count->load());
      cv->notify_one();
    }
  };

  virtual bool F(VertexInfo& vertex_info) = 0;
  virtual bool C(const VertexInfo& vertex_info) = 0;
};

}  // namespace minigraph
#endif  // MINIGRAPH_2d_PIE_EDGE_MAP_REDUCE_H
