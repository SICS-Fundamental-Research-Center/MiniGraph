#ifndef MINIGRAPH_2D_PIE_EDGE_MAP_REDUCE_H
#define MINIGRAPH_2D_PIE_EDGE_MAP_REDUCE_H

#include "executors/task_runner.h"
#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/thread_pool.h"
#include <folly/MPMCQueue.h>
#include <folly/concurrency/DynamicBoundedQueue.h>
#include <folly/executors/ThreadPoolExecutor.h>
#include <condition_variable>
#include <vector>

#define SWAP(a, b)           \
  {                          \
    Frontier* tmp = nullptr; \
    tmp = a;                 \
    a = b;                   \
    b = tmp;                 \
  }

namespace minigraph {

template <typename GRAPH_T, typename CONTEXT_T>
class EdgeMapBase {
  using VertexInfo =
      graphs::VertexInfo<typename GRAPH_T::vid_t, typename GRAPH_T::vdata_t,
                         typename GRAPH_T::edata_t>;
  using Frontier4 = folly::DMPMCQueue<VertexInfo, false>;

 public:
  EdgeMapBase() = default;
  EdgeMapBase(const CONTEXT_T context) { context_ = context; };
  ~EdgeMapBase() = default;

  void Bind(GRAPH_T* graph) { graph_ = graph; }

  Frontier4* EdgeMap(Frontier4* frontier_in, bool* visited,
                     executors::TaskRunner* task_runner) {
    // run vertex centric operations.
    std::condition_variable cv;
    std::mutex mtx;
    std::unique_lock<std::mutex> lck(mtx);

    std::atomic<size_t> num_finished_tasks(0);
    std::atomic<size_t> task_count(0);
    Frontier4* frontier_out = new Frontier4(graph_->get_num_vertexes() + 1);

    VertexInfo vertex_info;
    std::vector<std::function<void()>> tasks;
    while (!frontier_in->empty()) {
      frontier_in->dequeue(vertex_info);
      LOG_INFO("Processing VID: ", vertex_info.vid);
      task_count.store(task_count.load(std::memory_order_relaxed) + 1);

      auto task = std::bind(&EdgeMapBase<GRAPH_T, CONTEXT_T>::EdgeReduce, this,
                            vertex_info, frontier_out, visited,
                            &num_finished_tasks, &task_count, &cv, &lck);
      tasks.push_back(task);
    }
    size_t i = 0;
    for (; i < tasks.size(); i++) {
      task_runner->Run(std::forward<std::function<void()>&&>(tasks.at(i)));
    }

    while (num_finished_tasks.load(std::memory_order_acquire) <
           task_count.load(std::memory_order_acquire)) {
      cv.wait(lck);
    }

    delete frontier_in;
    return frontier_out;
  };

 protected:
  GRAPH_T* graph_ = nullptr;
  CONTEXT_T context_;

 private:
  void EdgeReduce(VertexInfo& vertex_info, Frontier4* frontier_out,
                  bool* visited, std::atomic<size_t>* num_finished_tasks,
                  std::atomic<size_t>* task_count, std::condition_variable* cv,
                  std::unique_lock<std::mutex>* lck) {
    for (size_t i = 0; i < vertex_info.outdegree; i++) {
      auto local_id = this->graph_->globalid2localid(vertex_info.out_edges[i]);
      if (local_id == VID_MAX) {
        continue;
      }
      VertexInfo&& ngh_vertex_info = graph_->GetVertex(local_id);
      if (C(ngh_vertex_info)) {
        if (F(ngh_vertex_info)) {
          frontier_out->enqueue(ngh_vertex_info);
          if (!visited[local_id]) visited[local_id] = true;
        }
      }
    }
    num_finished_tasks->fetch_add(1);

    LOG_INFO(num_finished_tasks->load(std::memory_order_seq_cst), " / ",
             task_count->load(std::memory_order_seq_cst));

    if (task_count->load(std::memory_order_acquire) ==
        num_finished_tasks->load(std::memory_order_acquire)) {
      if (lck->owns_lock()) {
        cv->notify_one();
      }
    }
  };

  virtual bool F(VertexInfo& vertex_info) = 0;
  virtual bool C(const VertexInfo& vertex_info) = 0;
};

}  // namespace minigraph
#endif  // MINIGRAPH_2d_PIE_EDGE_MAP_REDUCE_H
