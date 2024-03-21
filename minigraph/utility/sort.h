#ifndef SORT_H
#define SORT_H

#include <algorithm>
#include <iostream>
#include <math.h>
#include <stdio.h>

using namespace std;

template <typename T>
int part(T* r, int low, int hight) {
  int i = low, j = hight;
  T pivot = r[low];
  while (i < j) {
    while (i < j && r[j] > pivot) {
      j--;
    }
    if (i < j) {
      swap(r[i++], r[j]);
    }
    while (i < j && r[i] <= pivot) {
      i++;
    }
    if (i < j) {
      swap(r[i], r[j--]);
    }
  }
  return i;
}

template <typename T>
void QuickSort(T* r, int low, int hight) {
  int mid;
  if (low < hight) {
    mid = part(r, low, hight);
    QuickSort(r, low, mid - 1);
    QuickSort(r, mid + 1, hight);
  }
}

#endif