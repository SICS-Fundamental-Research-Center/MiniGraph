#ifndef BITMAP_H
#define BITMAP_H

#include "utility/logging.h"
#include <cassert>
#include <cstring>
#include <stdio.h>

#define WORD_OFFSET(i) (i >> 6)
#define BIT_OFFSET(i) (i & 0x3f)

class Bitmap {
 public:
  size_t size_ = 0;
  unsigned long* data_ = nullptr;

  Bitmap() = default;

  Bitmap(const size_t size) {
    init(size);
    return;
  }

  Bitmap(const size_t size, unsigned long* data) {
    init(size, data);
    return;
  }

  ~Bitmap() {
    if (data_ != nullptr) free(data_);
    size_ = 0;
    return;
  }

  void init(size_t size) {
    this->size_ = size;
    this->data_ = new unsigned long[WORD_OFFSET(size) + 1];
    return;
  }

  void init(size_t size, unsigned long* data) {
    this->size_ = size;
    this->data_ = data;
    return;
  }

  void clear() {
    size_t bm_size = WORD_OFFSET(size_);
    for (size_t i = 0; i <= bm_size; i++) {
      data_[i] = 0;
    }
    return;
  }

  bool empty() {
    size_t bm_size = WORD_OFFSET(size_);
    for (size_t i = 0; i <= bm_size; i++)
      if (data_[i] != 0) return false;
    return true;
  }

  bool is_equal_to(Bitmap& b) {
    if (size_ != b.size_) return false;
    size_t bm_size = WORD_OFFSET(size_);
    for (size_t i = 0; i <= bm_size; i++)
      if (data_[i] != b.data_[i]) return false;
    return true;
  }

  void fill() {
    size_t bm_size = WORD_OFFSET(size_);
    for (size_t i = 0; i < bm_size; i++) {
      data_[i] = 0xffffffffffffffff;
    }
    data_[bm_size] = 0;
    for (size_t i = (bm_size << 6); i < size_; i++) {
      data_[bm_size] |= 1ul << BIT_OFFSET(i);
    }
    return;
  }

  unsigned long get_bit(size_t i) {
    if (i > size_) return 0;
    return data_[WORD_OFFSET(i)] & (1ul << BIT_OFFSET(i));
  }

  size_t get_data_size(size_t size) {
    return (sizeof(unsigned long) * (WORD_OFFSET(size) + 1));
  }

  void set_bit(size_t i) {
    if (i > size_) return;

    __sync_fetch_and_or(data_ + WORD_OFFSET(i), 1ul << BIT_OFFSET(i));
    return;
  }

  void rm_bit(const size_t i) {
    assert(i <= size_);
    __sync_fetch_and_and(data_ + WORD_OFFSET(i), ~(1ul << BIT_OFFSET(i)));
    return;
  }

  bool batch_rm_bit(Bitmap& b) {
    if (size_ != b.size_) return false;
    size_t bm_size = WORD_OFFSET(size_);
    for (size_t i = 0; i <= bm_size; i++) {
      __sync_fetch_and_and(data_ + WORD_OFFSET(i), ~(b.data_[i]));
    }
    return true;
  }

  bool try_batch_rm_bit(Bitmap& b) {
    if (size_ != b.size_) return false;
    size_t bm_size = WORD_OFFSET(size_);
    for (size_t i = 0; i <= bm_size; i++)
      *(data_ + i) = *(data_ + i) & ~(b.data_[i]);
    return true;
  }

  bool batch_or_bit(Bitmap& b) {
    if (size_ != b.size_) return false;
    size_t bm_size = WORD_OFFSET(size_);
    for (size_t i = 0; i <= bm_size; i++) {
      __sync_fetch_and_or(data_ + i, b.data_[i]);
    }
    return true;
  }

  bool copy_bit(Bitmap& b) {
    if (size_ != b.size_) return false;
    memcpy(data_, b.data_, sizeof(unsigned long) * (WORD_OFFSET(size_) + 1));
    return true;
  }

  size_t get_num_bit() {
    size_t count = 0;
    for (size_t i = 0; i < size_; i++)
      if (get_bit(i)) count++;
    return count;
  }

  void invert_bit() {
    *data_ = ~(*data_);
    return;
  }
};

#endif