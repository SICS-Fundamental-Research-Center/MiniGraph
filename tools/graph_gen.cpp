#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <math.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <random>
#include <string>

#include <rapidcsv.h>

#include "portability/sys_types.h"
#include "utility/bitmap.h"
#include "utility/logging.h"
#include "utility/thread_pool.h"


class GraphGen {
 public:
  GraphGen() = default;
  GraphGen(const size_t num_vertexes, const size_t num_edges,
           const std::string& out_pt) {
    num_vertexes_ = num_vertexes;
    num_edges_ = num_edges;
    out_pt_ = out_pt;
  };
  ~GraphGen() = default;

  size_t num_vertexes_;
  size_t num_edges_;
  std::string out_pt_;

  static void MakeDirectory(const std::string& pt) {
    std::string dir = pt;
    int len = dir.size();
    if (dir[len - 1] != '/') {
      dir[len] = '/';
      len++;
    }
    std::string temp;
    for (int i = 1; i < len; i++) {
      if (dir[i] == '/') {
        temp = dir.substr(0, i);
        if (access(temp.c_str(), 0) != 0) {
          if (mkdir(temp.c_str(), 0777) != 0) {
            VLOG(1) << "failed operaiton.";
          }
        }
      }
    }
  }

  static bool IsExist(const std::string& pt) {
    struct stat buffer;
    return (stat(pt.c_str(), &buffer) == 0);
  }

  static void Touch(const std::string& pt) {
    std::ofstream file(pt, std::ios::binary);
    file.close();
  };
};

class RMAT final : public GraphGen {
 public:
  RMAT(const size_t power, const size_t num_edges, const std::string& out_pt,
       const float a, const float b, const float c, const float d)
      : GraphGen(std::pow(2, power), num_edges, out_pt) {
    a_ = a;
    b_ = b;
    c_ = c;
    d_ = d;
    x_ = b_ + d_;
    y_ = c_ + d_;
    power_ = power;
    x_signal_ = (bool*)malloc(sizeof(bool) * this->num_vertexes_);
    y_signal_ = (bool*)malloc(sizeof(bool) * this->num_vertexes_);
    memset(x_signal_, 0, sizeof(bool) * this->num_vertexes_);
    memset(y_signal_, 0, sizeof(bool) * this->num_vertexes_);
    std::cout << "GraphInfo. num_vertexes: " << 2 << "^" << power_ << "="
              << std::pow(2, power) << ", num_edges: " << num_edges
              << ", a: " << a_ << ", b: " << b_ << ", c: " << c_
              << ", d_: " << d_ << std::endl;
  }

  RMAT(const size_t power, const size_t num_edges, const std::string& out_pt,
       const float x, const float y)
      : GraphGen(std::pow(2, power), num_edges, out_pt) {
    x_ = x;
    y_ = y;
    power_ = power;
    x_signal_ = (bool*)malloc(sizeof(bool) * this->num_vertexes_);
    y_signal_ = (bool*)malloc(sizeof(bool) * this->num_vertexes_);
    x_signal_bit_ = new Bitmap(this->num_vertexes_);
    y_signal_bit_ = new Bitmap(this->num_vertexes_);

    memset(x_signal_, 0, sizeof(bool) * this->num_vertexes_);
    memset(y_signal_, 0, sizeof(bool) * this->num_vertexes_);
    std::cout << "GraphInfo. num_vertexes: " << 2 << "^" << power_ << "="
              << std::pow(2, power) << ", num_edges: " << num_edges
              << ", x: " << x_ << ", y_: " << y_ << std::endl;
  }

  std::pair<std::vector<size_t>, std::vector<size_t>> Run() {
    std::vector<size_t> src;
    std::vector<size_t> dst;
    src.reserve(this->num_edges_);
    dst.reserve(this->num_edges_);

    for (size_t i = 0; i < this->num_edges_; i++) {
      auto xy = Falling();
      if (x_signal_[xy.first] && y_signal_[xy.second]) {
        continue;
      } else {
        x_signal_[xy.first] ? 0 : x_signal_[xy.first] = true;
        y_signal_[xy.second] ? 0 : y_signal_[xy.second] = true;
        src.push_back(xy.first);
        dst.push_back(xy.second);
      }
    }
    return std::make_pair(src, dst);
  }

  std::pair<std::vector<size_t>, std::vector<size_t>> ParallelRun(
      const size_t cores) {
    std::vector<size_t>** p_src = new std::vector<size_t>*[cores];
    std::vector<size_t>** p_dst = new std::vector<size_t>*[cores];

    for (size_t i = 0; i < cores; i++) {
      p_src[i] = new std::vector<size_t>;
      p_dst[i] = new std::vector<size_t>;
      p_src[i]->reserve(this->num_edges_ / cores);
      p_dst[i]->reserve(this->num_edges_ / cores);
    }

    auto thread_pool = minigraph::utility::CPUThreadPool(cores, 1);

    x_signal_bit_->clear();
    y_signal_bit_->clear();

    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);
    std::atomic<size_t> pending_packages(cores);
    for (size_t i = 0; i < cores; i++) {
      thread_pool.Commit(
          [this, i, cores, &p_src, &p_dst, &pending_packages, &finish_cv]() {
            for (size_t j = i; j < this->num_edges_; j += cores) {
              auto xy = Falling();
              if (x_signal_bit_->get_bit(xy.first) != 0 &&
                  y_signal_bit_->get_bit(xy.second) != 0)
                continue;
              x_signal_bit_->set_bit(xy.first);
              y_signal_bit_->set_bit(xy.second);
              p_src[i]->push_back(xy.first);
              p_dst[i]->push_back(xy.second);
            }
            if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
            return;
          });
    }

    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    std::vector<size_t>* src = new std::vector<size_t>;
    std::vector<size_t>* dst = new std::vector<size_t>;
    src->reserve(this->num_edges_);
    dst->reserve(this->num_edges_);

    for (size_t i = 0; i < cores; i++) {
      src->insert(src->end(), p_src[i]->begin(), p_src[i]->end());
      dst->insert(dst->end(), p_dst[i]->begin(), p_dst[i]->end());
      delete p_src[i];
      delete p_dst[i];
    }
    return std::make_pair(*src, *dst);
  }

  void WriteEdgeList(
      const std::pair<std::vector<size_t>, std::vector<size_t>>& data,
      const std::string& out_pt) {
    rapidcsv::Document doc("", rapidcsv::LabelParams(-1, -1),
                           rapidcsv::SeparatorParams(',', false, false));
    doc.SetColumnName(0, "src");
    doc.SetColumnName(1, "dst");
    doc.SetColumn<size_t>(0, data.first);
    doc.SetColumn<size_t>(1, data.second);
    doc.Save(out_pt);
    std::cout << "Save to " << out_pt << " num edges: " << data.first.size()
              << std::endl;
  };

 private:
  size_t power_ = 0;
  float a_ = 0;
  float b_ = 0;
  float c_ = 0;
  float d_ = 0;
  float x_ = 0;
  float y_ = 0;

  bool* x_signal_ = nullptr;
  bool* y_signal_ = nullptr;
  Bitmap* x_signal_bit_ = nullptr;
  Bitmap* y_signal_bit_ = nullptr;

  std::pair<size_t, size_t> GetCoordinate() {
    std::random_device rd;
    std::default_random_engine e(rd());
    std::srand((unsigned)time(NULL));
    std::bernoulli_distribution X(x_);
    std::bernoulli_distribution Y(y_);
    auto x = X(e);
    auto y = Y(e);
    return std::make_pair(x, y);
  }

  std::pair<size_t, size_t> Falling() {
    std::pair<size_t, size_t> X_scope =
        std::make_pair(0, std::pow(2, power_) - 1);
    std::pair<size_t, size_t> Y_scope =
        std::make_pair(0, std::pow(2, power_) - 1);
    size_t scope = std::pow(2, power_);
    for (size_t i = 0; i < power_; i++) {
      auto coordinate = this->GetCoordinate();
      scope = scope >> 1;
      if (coordinate.first == 0) {
        X_scope.second -= scope;
      } else {
        X_scope.first += scope;
      }
      if (coordinate.second == 0) {
        Y_scope.second -= scope;
      } else {
        Y_scope.first += scope;
      }
    }
    // LOG_INFO(X_scope.first, " ", Y_scope.second);
    return std::make_pair(X_scope.first, Y_scope.second);
  }
};

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  float x = FLAGS_x;
  float y = FLAGS_y;
  size_t power = FLAGS_power;
  size_t num_edges = FLAGS_edges;
  std::string output_pt = FLAGS_o;
  size_t cores = FLAGS_cores;
  std::cout << "RmatGen: x = " << x << ", y = " << y
            << ", num_vertexes: " << pow(2, power)
            << ", num_edges: " << num_edges << std::endl;
  RMAT rmat(power, num_edges, output_pt, x, y);
  // std::pair<std::vector<size_t>, std::vector<size_t>>&& data = rmat.Run();
  std::pair<std::vector<size_t>, std::vector<size_t>>&& data =
      rmat.ParallelRun(cores);
  if (!RMAT::IsExist(output_pt)) {
    RMAT::Touch(output_pt);
  }
  rmat.WriteEdgeList(data, output_pt);

  gflags::ShutDownCommandLineFlags();
}