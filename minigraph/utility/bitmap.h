#ifndef BITMAP_H
#define BITMAP_H
#include <stdio.h>

#define WORD_OFFSET(i) (i >> 6)
#define BIT_OFFSET(i) (i & 0x3f)

class Bitmap {
 public:
  size_t size_ = 0;
  unsigned long* data_ = nullptr;

  Bitmap() = default;

  Bitmap(size_t size) { init(size); }
  Bitmap(size_t size, unsigned long* data) { init(size, data); }

  ~Bitmap() {
    if (data_ != nullptr) free(data_);
    size_ = 0;
  }

  void init(size_t size) {
    this->size_ = size;
    this->data_ = new unsigned long[WORD_OFFSET(size) + 1];
  }

  void init(size_t size, unsigned long* data) {
    this->size_ = size;
    this->data_ = data;
  }

  void clear() {
    size_t bm_size = WORD_OFFSET(size_);
    for (size_t i = 0; i <= bm_size; i++) {
      data_[i] = 0;
    }
  }

  bool empty() {
    size_t bm_size = WORD_OFFSET(size_);
    for (size_t i = 0; i <= bm_size; i++)
      if (data_[i] != 0) return false;
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
  }

  unsigned long get_bit(size_t i) {
    return data_[WORD_OFFSET(i)] & (1ul << BIT_OFFSET(i));
  }

  size_t get_data_size(size_t size) {
    return (sizeof(unsigned long) * (WORD_OFFSET(size) + 1));
  }

  void set_bit(size_t i) {
    __sync_fetch_and_or(data_ + WORD_OFFSET(i), 1ul << BIT_OFFSET(i));
  }

  void rm_bit(const size_t i) {
    __sync_fetch_and_and(data_ + WORD_OFFSET(i), ~(1ul << BIT_OFFSET(i)));
  }
};

#endif