#ifndef HMG_GRAPHS_RELATION_H_
#define HMG_GRAPHS_RELATION_H_

#include <iostream>
#include <stdlib.h>

class Table {
 public:
  size_t table_id_ = 0;
  size_t num_terms_ = 1;
  size_t num_tuples_ = 1;
  size_t* max_size_per_term_ = nullptr;
  size_t* aligned_max_size_per_term_ = nullptr;
  size_t* offset_per_term_ = nullptr;
  char* buf_table_ = nullptr;

 public:
  Table() = default;
  ~Table() = default;

  void ShowTable(size_t row = 2) {
    // LOG_INFO("------------------ Table: ", table_id_,
    //          " num_terms: ", num_terms_, " num_tuples: ", num_tuples_,
    //          "---------------- ");
    size_t size_for_all_terms = 0;
    for (size_t i = 0; i < num_terms_; i++)
      size_for_all_terms += aligned_max_size_per_term_[i];
    for (size_t i = 0; i < num_tuples_; i++) {
      std::cout << i << " : ";
      for (size_t j = 0; j < num_terms_; j++) {
        size_t c_i = 0;
        while (*(buf_table_ + i * size_for_all_terms * sizeof(char) +
                 offset_per_term_[j] + c_i) != '\0') {
          std::cout << *(buf_table_ + i * size_for_all_terms * sizeof(char) +
                         offset_per_term_[j] + c_i++);
        }
        std::cout << " | ";
      }
      std::cout << std::endl;
    }
    return;
  }
};

#endif  // HMG_GRAPHS_RELATION_H_
