#ifndef MINIGRAPH_2D_PIE_EDGE_MAP_REDUCE_H
#define MINIGRAPH_2D_PIE_EDGE_MAP_REDUCE_H

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
  using Frontier = folly::DMPMCQueue<typename GRAPH_T::vid_t, false>;
  using Frontier2 = folly::DMPMCQueue<VertexInfo*, false>;

 public:
  EdgeMapBase() = default;
  EdgeMapBase(const CONTEXT_T context) { context_ = context; };
  ~EdgeMapBase() = default;

  void Bind(GRAPH_T* graph, utility::CPUThreadPool* cpu_thread_pool) {
    graph_ = graph;
    cpu_thread_pool_ = cpu_thread_pool;
  }

  Frontier* EdgeMap(Frontier* frontier_in, bool* visited) {
    // run vertex centric operations.
    std::condition_variable cv;
    std::mutex mtx;
    std::unique_lock<std::mutex> lck(mtx);

    size_t step =
        ceil((float)frontier_in->size() / (float)context_.num_threads);
    std::atomic<size_t> num_finished_tasks(0);
    std::atomic<size_t> task_count(0);
    Frontier* frontier_out = new Frontier(graph_->get_num_vertexes() + 100);

    std::vector<std::function<void()>> tasks;
    typename GRAPH_T::vid_t vid;
    while (!frontier_in->empty()) {
      frontier_in->dequeue(vid);
      if (vid == VID_MAX) {
        continue;
      }
      // commit to process vertex.
      LOG_INFO("Processing VID: ", vid, " step: ", step,
               ", frontier_in->size(): ", frontier_in->size(),
               ",  num_finished_tasks: ",
               num_finished_tasks.load(std::memory_order_relaxed),
               " task count: ", task_count.load());
      task_count.store(task_count.load(std::memory_order_relaxed) + 1);
      auto task = std::bind(&EdgeMapBase<GRAPH_T, CONTEXT_T>::EdgeReduce, this,
                            vid, frontier_out, visited, &num_finished_tasks,
                            &task_count, &cv);
      tasks.push_back(task);
    }
    for (size_t i = 0; i < tasks.size(); i++) {
      cpu_thread_pool_->Commit(tasks.at(i));
    }
    cv.wait(lck);
    delete frontier_in;
    return frontier_out;
  };

  Frontier2* EdgeMap2(Frontier2* frontier_in, bool* visited) {
    // run vertex centric operations.
    std::condition_variable cv;
    std::mutex mtx;
    std::unique_lock<std::mutex> lck(mtx);

    size_t step =
        ceil((float)frontier_in->size() / (float)context_.num_threads);
    std::atomic<size_t> num_finished_tasks(0);
    std::atomic<size_t> task_count(0);
    Frontier2* frontier_out = new Frontier2(graph_->get_num_vertexes() + 100);

    std::vector<std::function<void()>> tasks;
    typename GRAPH_T::vid_t vid;
    VertexInfo* vertex_info;
    // while (!frontier_in->empty()) {
    //   frontier_in->dequeue(vertex_info);
    //  if (vid == VID_MAX) {
    //    continue;
    //  }
    //// commit to process vertex.
    // LOG_INFO("Processing VID: ", vid, " step: ", step,
    //          ", frontier_in->size(): ", frontier_in->size(),
    //          ",  num_finished_tasks: ",
    //          num_finished_tasks.load(std::memory_order_relaxed),
    //          " task count: ", task_count.load());
    // task_count.store(task_count.load(std::memory_order_relaxed) + 1);
    // auto task = std::bind(&EdgeMapBase<GRAPH_T, CONTEXT_T>::EdgeReduce,
    // this,
    //                       vid, frontier_out, visited, &num_finished_tasks,
    //                       &task_count, &cv);
    // tasks.push_back(task);
    //}
    // for (size_t i = 0; i < tasks.size(); i++) {
    //  cpu_thread_pool_->Commit(tasks.at(i));
    //}
    // cv.wait(lck);
    // delete frontier_in;
    // return frontier_out;
  };

 protected:
  utility::CPUThreadPool* cpu_thread_pool_ = nullptr;
  GRAPH_T* graph_ = nullptr;
  CONTEXT_T context_;

 private:
  void EdgeReduce(const typename GRAPH_T::vid_t& vid, Frontier* frontier_out,
                  bool* visited, std::atomic<size_t>* num_finished_tasks,
                  std::atomic<size_t>* task_count,
                  std::condition_variable* cv) {
    num_finished_tasks->store(
        num_finished_tasks->load(std::memory_order_relaxed) + 1);

    VertexInfo&& vertex_info = graph_->GetVertex(vid);
    for (size_t i = 0; i < vertex_info.outdegree; i++) {
      auto local_id = this->graph_->globalid2localid(vertex_info.out_edges[i]);
      LOG_INFO("LOCAL_ID: ", local_id, ", ", vertex_info.out_edges[i]);
      if (local_id == VID_MAX) {
        continue;
      }
      if (visited[local_id] == true) {
        continue;
      }
      if (C(local_id)) {
        if (F(local_id)) {
          frontier_out->enqueue(local_id);
          visited[local_id] = true;
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
