#ifndef MINIGRAPH_UTILITY_IO_RELATION_IO_ADAPTER_H
#define MINIGRAPH_UTILITY_IO_RELATION_IO_ADAPTER_H

#include <sys/stat.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

#include "graphs/edgelist.h"
#include "graphs/relation.h"
#include "io_adapter_base.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "rapidcsv.h"
#include "utility/atomic.h"
#include "utility/thread_pool.h"
#include "table.h"

namespace minigraph {
namespace utility {
namespace io {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class RelationIOAdapter : public IOAdapterBase<GID_T, VID_T, VDATA_T, EDATA_T> {
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using EDGE_LIST_T = graphs::EdgeList<GID_T, VID_T, VDATA_T, EDATA_T>;
  using RELATION_T = graphs::Relation<GID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  RelationIOAdapter() = default;

  ~RelationIOAdapter() = default;

  template <class... Args>
  bool Read(GRAPH_BASE_T* graph, const GraphFormat& graph_format,
            const size_t cores = 1, char separator_params = ',',
            Args&&... args) {
    std::string pt[] = {(args)...};
    bool tag = false;
    switch (graph_format) {
      case batch_relation_csv:
        tag = BatchParallelReadRelationFromCSV(graph, pt[0], true,
                                               separator_params, cores);
        break;
      case relation_csv:
        break;
      case relation_bin:
        tag = ReadRelationFromBin(graph, pt[0], pt[1]);
      default:
        break;
    }
    return tag;
  }

  template <class... Args>
  bool Write(const RELATION_T& graph, const GraphFormat& graph_format,
             Args&&... args) {
    std::string pt[] = {(args)...};
    bool tag = false;
    switch (graph_format) {
      case relation_csv:
        break;
      case relation_bin:
        WriteRelation2RelationBin(graph, pt[0], pt[1]);
        break;
      default:
        break;
    }
    return tag;
  }

  bool ReadRelationFromBin(graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph,
                           const std::string& meta_pt = "",
                           const std::string& data_pt = "") {
    RELATION_T* relation = (RELATION_T*)graph;

    std::ifstream meta_file(meta_pt, std::ios::binary | std::ios::app);
    std::ifstream data_file(data_pt, std::ios::binary | std::ios::app);

    LOG_INFO("ReadBin: ", meta_pt);
    LOG_INFO("ReadBin: ", data_pt);
    meta_file.read((char*)&relation->num_table_, sizeof(size_t));

    relation->tables_ = (Table*)malloc(sizeof(Table) *
                                               relation->num_table_ * 2);

    // num_table num_terms_for each relation.
    size_t i = 0;
    size_t sum_terms = 0;

    for (size_t ti = 0; ti < relation->num_table_; ti++) {
      relation->tables_[ti].num_terms_;
      meta_file.read((char*)&relation->tables_[ti].num_terms_, sizeof(size_t));
      meta_file.read((char*)&relation->tables_[ti].num_tuples_, sizeof(size_t));
      relation->tables_[ti].max_size_per_term_ =
          (size_t*)malloc(sizeof(size_t) * relation->tables_[ti].num_terms_);
      relation->tables_[ti].aligned_max_size_per_term_ =
          (size_t*)malloc(sizeof(size_t) * relation->tables_[ti].num_terms_);
      relation->tables_[ti].offset_per_term_ =
          (size_t*)malloc(sizeof(size_t) * relation->tables_[ti].num_terms_);
      memset(relation->tables_[ti].offset_per_term_, 0,
             sizeof(size_t) * relation->tables_[ti].num_terms_);
      memset(relation->tables_[ti].max_size_per_term_, 0,
             sizeof(size_t) * relation->tables_[ti].num_terms_);
      memset(relation->tables_[ti].aligned_max_size_per_term_, 0,
             sizeof(size_t) * relation->tables_[ti].num_terms_);
      sum_terms += relation->tables_[ti].num_terms_;
    }

    for (size_t ti = 0; ti < relation->num_table_; ti++) {
      for (size_t column_i = 0; column_i < relation->tables_[ti].num_terms_;
           column_i++) {
        meta_file.read(
            (char*)&relation->tables_[ti].max_size_per_term_[column_i],
            sizeof(size_t));
        meta_file.read(
            (char*)&relation->tables_[ti].aligned_max_size_per_term_[column_i],
            sizeof(size_t));
        meta_file.read((char*)&relation->tables_[ti].offset_per_term_[column_i],
                       sizeof(size_t));
      }
    }

    for (size_t ti = 0; ti < relation->num_table_; ti++) {
      size_t size_for_all_terms = 0;
      for (size_t column_i = 0; column_i < relation->tables_[ti].num_terms_;
           column_i++) {
        size_for_all_terms +=
            relation->tables_[ti].aligned_max_size_per_term_[column_i];
      }

      relation->tables_->table_id_ = ti;
      relation->tables_[ti].buf_table_ =
          (char*)malloc(size_for_all_terms * sizeof(char) *
                        relation->tables_[ti].num_tuples_);
      memset(relation->tables_[ti].buf_table_, 0,
             size_for_all_terms * sizeof(char));
      data_file.read((char*)relation->tables_[ti].buf_table_,
                     size_for_all_terms * sizeof(char) *
                         relation->tables_[ti].num_tuples_);
    }

    return true;
  }

  bool BatchParallelReadRelationFromCSV(
      graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph,
      const std::string& path, const bool assemble = false,
      char separator_params = ',', const size_t cores = 1) {
    if (!this->Exist(path)) {
      XLOG(ERR, "Read file fault: ", path);
      return false;
    }
    if (graph == nullptr) {
      XLOG(ERR, "segmentation fault: graph is nullptr");
      return false;
    }

    LOG_INFO("BatchParallelReadRelationFromCSV");
    RELATION_T* relation = (RELATION_T*)graph;
    auto thread_pool = CPUThreadPool(cores, 1);
    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);

    std::vector<std::string> files;
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
      std::string path = entry.path();
      files.push_back(path);
    }

    ((RELATION_T*)graph)->num_table_ = files.size();
    ((RELATION_T*)graph)->tables_ =
        (Table*)malloc(sizeof(Table) * files.size());

    for (size_t i = 0; i < files.size(); i++) {
      ((RELATION_T*)graph)->tables_[i] = Table();
    }

    rapidcsv::Document p_doc[files.size()];
    for (auto pi = 0; pi < files.size(); pi++) {
      auto doc =
          rapidcsv::Document(files.at(pi), rapidcsv::LabelParams(),
                             rapidcsv::SeparatorParams(separator_params));

      p_doc[pi] = doc;
      size_t num_column = doc.GetColumnCount();
      size_t num_row = doc.GetRowCount();

      ((RELATION_T*)graph)->tables_[pi].table_id_ = pi;
      ((RELATION_T*)graph)->tables_[pi].num_terms_ = num_column;
      ((RELATION_T*)graph)->tables_[pi].num_tuples_ = num_row;
      ((RELATION_T*)graph)->tables_[pi].max_size_per_term_ =
          (size_t*)malloc(sizeof(size_t) * num_column);
      ((RELATION_T*)graph)->tables_[pi].aligned_max_size_per_term_ =
          (size_t*)malloc(sizeof(size_t) * num_column);
      ((RELATION_T*)graph)->tables_[pi].offset_per_term_ =
          (size_t*)malloc(sizeof(size_t) * num_column);
      memset(((RELATION_T*)graph)->tables_[pi].max_size_per_term_, 0,
             sizeof(size_t) * num_column);

      std::string* buf_relation = new std::string[num_column * num_row];

      // Read table from csv.
      std::atomic<size_t> pending_packages(cores);
      for (size_t i = 0; i < cores; i++) {
        size_t tid = i;
        thread_pool.Commit([tid, &cores, &doc, &pi, &num_column, &num_row,
                            &buf_relation, &graph, &pending_packages,
                            &finish_cv]() {
          for (size_t row_i = tid; row_i < num_row; row_i += cores) {
            auto row = doc.GetRow<std::string>(row_i);
            for (size_t column_i = 0; column_i < num_column; column_i++) {
              write_max(&((RELATION_T*)graph)
                             ->tables_[pi]
                             .max_size_per_term_[column_i],
                        row.at(column_i).length());
              *(buf_relation + row_i * num_column + column_i) =
                  row.at(column_i);
            }
          }
          if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
          return;
        });
      }
      finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

      for (size_t row_i = 0; row_i < num_row; row_i++) {
        for (size_t column_i = 0; column_i < num_column; column_i++) {
        }
      }

      size_t size_for_all_terms = 0;
      for (size_t i = 0; i < ((RELATION_T*)graph)->tables_[pi].num_terms_;
           i++) {
        ((RELATION_T*)graph)->tables_[pi].aligned_max_size_per_term_[i] =
            ceil(
                (float)((RELATION_T*)graph)->tables_[pi].max_size_per_term_[i] /
                ALIGNMENT_FACTOR) *
            ALIGNMENT_FACTOR;
        size_for_all_terms +=
            ((RELATION_T*)graph)->tables_[pi].aligned_max_size_per_term_[i];
      }
      for (size_t i = 0; i < ((RELATION_T*)graph)->tables_[pi].num_terms_;
           i++) {
        ((RELATION_T*)graph)->tables_[pi].offset_per_term_[i] =
            i * ((RELATION_T*)graph)->tables_[pi].aligned_max_size_per_term_[i];
      }

      ((RELATION_T*)graph)->tables_[pi].buf_table_ = (char*)malloc(
          sizeof(char) * ((RELATION_T*)graph)->tables_[pi].num_tuples_ *
          size_for_all_terms);
      memset(relation->tables_[pi].buf_table_, 0,
             sizeof(char) * relation->tables_[pi].num_tuples_ *
                 size_for_all_terms);

      // Contracting table.
      pending_packages.store(cores);
      for (size_t i = 0; i < cores; i++) {
        size_t tid = i;
        thread_pool.Commit([tid, &cores, &pi, &size_for_all_terms, &num_column,
                            &num_row, &buf_relation, &relation, &graph,
                            &pending_packages, &finish_cv]() {
          for (size_t row_i = tid; row_i < num_row; row_i += cores) {
            size_t row_offset = size_for_all_terms * row_i;
            for (size_t column_i = 0; column_i < num_column; column_i++) {
              size_t column_offset =
                  relation->tables_[pi].offset_per_term_[column_i];
              memcpy(
                  (relation->tables_[pi].buf_table_ + row_offset +
                   column_offset),
                  &(*(buf_relation + row_i * num_column + column_i)->c_str()),
                  sizeof(char) *
                      (*(buf_relation + row_i * num_column + column_i)).size());
            }
          }
          if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
          return;
        });
      }
      finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
    }
    return true;
  }

  bool WriteRelation2RelationBin(const RELATION_T& relation,
                                 const std::string& meta_pt,
                                 const std::string& data_pt) {
    if (this->Exist(meta_pt)) remove(meta_pt.c_str());
    if (this->Exist(data_pt)) remove(data_pt.c_str());

    std::ofstream meta_file(meta_pt, std::ios::binary | std::ios::app);
    std::ofstream data_file(data_pt, std::ios::binary | std::ios::app);

    size_t* meta_buff =
        (size_t*)malloc(sizeof(size_t) * (relation.num_table_ * 2));
    // num_table num_terms for each relation.
    size_t i = 0;
    size_t sum_terms = 0;
    meta_file.write((char*)&relation.num_table_, sizeof(size_t));

    for (size_t ti = 0; ti < relation.num_table_; ti++) {
      meta_file.write((char*)&relation.tables_[ti].num_terms_, sizeof(size_t));
      meta_file.write((char*)&relation.tables_[ti].num_tuples_, sizeof(size_t));
      sum_terms += relation.tables_[ti].num_terms_;
    }

    i = 0;
    for (size_t ti = 0; ti < relation.num_table_; ti++) {
      for (size_t column_i = 0; column_i < relation.tables_[ti].num_terms_;
           column_i++) {
        meta_file.write(
            (char*)&relation.tables_[ti].max_size_per_term_[column_i],
            sizeof(size_t));
        meta_file.write(
            (char*)&relation.tables_[ti].aligned_max_size_per_term_[column_i],
            sizeof(size_t));
        meta_file.write((char*)&relation.tables_[ti].offset_per_term_[column_i],
                        sizeof(size_t));
      }
    }

    for (size_t ti = 0; ti < relation.num_table_; ti++) {
      size_t size_for_all_terms = 0;
      for (size_t column_i = 0; column_i < relation.tables_[ti].num_terms_;
           column_i++) {
        size_for_all_terms +=
            relation.tables_[ti].aligned_max_size_per_term_[column_i];
      }
      data_file.write((char*)relation.tables_[ti].buf_table_,
                      size_for_all_terms * relation.tables_[ti].num_tuples_);
    }

    relation.tables_[0].ShowTable();
    relation.tables_[1].ShowTable();
    data_file.close();
    meta_file.close();
    return true;
  }
};

}  // namespace io
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_IO_RELATION_IO_ADAPTER_H
