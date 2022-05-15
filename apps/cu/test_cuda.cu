#include <cuda_runtime.h>
int main() {
  char* a;
  cudaMalloc((void**)&a, 10);
}
