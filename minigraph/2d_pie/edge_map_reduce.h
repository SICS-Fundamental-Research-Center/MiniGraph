#ifndef MINIGRAPH_2D_PIE_EDGE_MAP_REDUCE_H
#define MINIGRAPH_2D_PIE_EDGE_MAP_REDUCE_H

#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/thread_pool.h"
#include <folly/MPMCQueue.h>
#include <folly/executors/ThreadPoolExecutor.h>
#include <condition_variable>
#include <vector>

namespace minigraph {

template <typename GRAPH_T, typename CONTEXT_T>
class EdgeMapBase {
  using VertexInfo =
      graphs::VertexInfo<typename GRAPH_T::vid_t, typename GRAPH_T::vdata_t,
                         typename GRAPH_T::edata_t>;
  using Frontier = folly::MPMCQueue<typename GRAPH_T::vid_t>;

 public:
  EdgeMapBase() = default;
  EdgeMapBase(const CONTEXT_T context) { context_ = context; };
  ~EdgeMapBase() = default;

  void Bind(GRAPH_T* graph, utility::CPUThreadPool* cpu_thread_pool) {
    graph_ = graph;
    cpu_thread_pool_ = cpu_thread_pool;
  }

  void EdgeMap(Frontier& frontier_in) {
    // run vertex centric operations.
    std::condition_variable cv;
    std::mutex mtx;
    std::unique_lock<std::mutex> lck(mtx);

    size_t step = ceil((float)frontier_in.size() / (float)context_.num_threads);
    LOG_INFO("Step: ", step);
    std::atomic<size_t> num_finished_tasks(0);
    std::atomic<size_t> task_count(0);
    Frontier frontier_out(graph_->get_num_vertexes());

    std::vector<std::function<void()>> tasks;
    for (ssize_t i = 0; i < frontier_in.size(); i += step) {
      typename GRAPH_T::vid_t vid;
      frontier_in.blockingRead(vid);
      LOG_INFO("!!!!!!!!!!!!!!!!!GET frontier: ", vid);
      auto local_id = graph_->globalid2localid(vid);
      if (local_id == VID_MAX) {
        continue;
      }
      // commit to process vertex.
      LOG_INFO("Processing VID: ", vid, ", local_id: ", local_id, ", i: ", i,
               " step: ", step, ", frontier_in->size(): ", frontier_in.size(),
               ",  num_finished_tasks: ",
               num_finished_tasks.load(std::memory_order_relaxed),
               " task count: ", task_count.load());
      task_count.store(task_count.load(std::memory_order_relaxed) + 1);
      auto task = std::bind(&EdgeMapBase<GRAPH_T, CONTEXT_T>::EdgeReduce, this,
                            local_id, &frontier_out, &num_finished_tasks,
                            &task_count, &cv);
      tasks.push_back(task);
    }
    for (size_t i = 0; i < tasks.size(); i++) {
      cpu_thread_pool_->Commit(tasks.at(i));
    }
    cv.wait(lck);
    frontier_in = std::move(frontier_out);
    LOG_INFO("finish edgeMap");
  };

 protected:
  utility::CPUThreadPool* cpu_thread_pool_ = nullptr;
  GRAPH_T* graph_ = nullptr;
  CONTEXT_T context_;

 private:
  void EdgeReduce(const typename GRAPH_T::vid_t& vid,
                  folly::MPMCQueue<typename GRAPH_T::vid_t>* frontier_out,
                  std::atomic<size_t>* num_finished_tasks,
                  std::atomic<size_t>* task_count,
                  std::condition_variable* cv) {
    num_finished_tasks->store(
        num_finished_tasks->load(std::memory_order_relaxed) + 1);
    VertexInfo&& vertex_info = graph_->GetVertex(vid);
    LOG_INFO("EdgeReduce - vid: ", vid);
    for (size_t i = 0; i < vertex_info.outdegree; i++) {
      LOG_INFO(vertex_info.out_edges[i]);
    }
    for (size_t i = 0; i < vertex_info.outdegree; i++) {
      if (C(vertex_info.out_edges[i])) {
        LOG_INFO("To do: ", vertex_info.out_edges[i]);
        if (F(vertex_info.out_edges[i])) {
          LOG_INFO("XXXXXXXXWrite frontier: ", vertex_info.out_edges[i]);
          frontier_out->write(vertex_info.out_edges[i]);
        }
      }
    }
    if (task_count->load(std::memory_order_relaxed) ==
        num_finished_tasks->load(std::memory_order_relaxed)) {
      cv->notify_all();
    }
  };

  virtual bool F(const typename GRAPH_T::vid_t& vid) = 0;
  virtual bool C(const typename GRAPH_T::vid_t& vid) = 0;
};

}  // namespace minigraph
#endif  // MINIGRAPH_2d_PIE_EDGE_MAP_REDUCE_H
