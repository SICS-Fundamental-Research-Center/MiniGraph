#ifndef ATOMIC_H
#define ATOMIC_H

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

template <class ET>
inline bool cas(ET* ptr, ET oldv, ET newv) {
  if (sizeof(ET) == 8) {
    return __sync_bool_compare_and_swap((long*)ptr, *((long*)&oldv),
                                        *((long*)&newv));
  } else if (sizeof(ET) == 4) {
    return __sync_bool_compare_and_swap((int*)ptr, *((int*)&oldv),
                                        *((int*)&newv));
  } else if (sizeof(ET) == 2) {
    return __sync_bool_compare_and_swap((unsigned short*)ptr,
                                        *((unsigned short*)&oldv),
                                        *((unsigned short*)&newv));
  } else if (sizeof(ET) == 1) {
    printf("XXX");
    return __sync_bool_compare_and_swap((uint8_t*)ptr, *((uint8_t*)&oldv),
                                        *((uint8_t*)&newv));
  } else {
    assert(false);
  }
}

template <class ET>
inline bool write_min(ET* a, ET b) {
  ET c;
  bool r = 0;
  do c = *a;
  while (c > b && !(r = cas(a, c, b)));
  return r;
}

template <class ET>
inline bool write_max(ET* a, ET b) {
  ET c;
  bool r = 0;
  do c = *a;
  while (c < b && !(r = cas(a, c, b)));
  return r;
}

template <class ET>
inline void write_add(ET* a, ET b) {
  volatile ET newV, oldV;
  do {
    oldV = *a;
    newV = oldV + b;
  } while (!cas(a, oldV, newV));
}

#endif