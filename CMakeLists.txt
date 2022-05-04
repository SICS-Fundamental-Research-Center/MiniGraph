cmake_minimum_required(VERSION 3.12)

project(minigraph)
set(MINIGRAPH_COPYRIGHT_YEAR "2022")

###### Cmake options ######
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")

# Set default cmake type to Debug
if(NOT CMAKE_BUILD_TYPE)
  # cmake default flags with relwithdebinfo is -O2 -g
  # cmake default flags with release is -O3 -DNDEBUG
  set(CMAKE_BUILD_TYPE "Debug")
endif()
message(STATUS "[MiniGraph] will build in type: ${CMAKE_BUILD_TYPE}")


###### Directories shortcuts ######
set(PROJECT_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(THIRD_PARTY_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin)


###### Add third party to the project, if necessary ######
if(test)
  enable_testing()
  message(STATUS "Enabling GoogleTest...")
  set (BUILD_GMOCK ON CACHE BOOL "Option: build gmock and gtest -- ON" FORCE)
  set (INSTALL_GTEST OFF CACHE BOOL "Option: install gtest -- OFF" FORCE)
  add_subdirectory("${THIRD_PARTY_ROOT}/googletest")
endif()


#######################
# Libraries
#######################

# Folly
find_package(folly REQUIRED)
if (NOT TARGET Folly::folly)
  include("${FOLLY_CMAKE_DIR}/folly-targets.cmake")
endif()
set(FOLLY_LIBRARIES Folly::folly)
if (NOT folly_FIND_QUIETLY)
  message(STATUS "Found folly: ${PACKAGE_PREFIX_DIR}")
endif()

# jemalloc
if (USE_JEMALLOC)
  include("cmake/FindJemalloc.cmake" OPTIONAL)
  if (NOT JEMALLOC_FOUND)
    message(STATUS "jemalloc not found, build without jemalloc")
  else ()
    add_definitions(-DUSE_JEMALLOC)
    include_directories(SYSTEM ${JEMALLOC_INCLUDE_DIRS})
  endif ()
endif ()

# find Threads
set(CMAKE_THREAD_PREFER_PTHREAD ON)
find_package(Threads REQUIRED)

# find gflag
find_package(gflags REQUIRED)

# Boost-ext sml
include_directories(${THIRD_PARTY_ROOT}/sml/include)

# rapidcsv
include_directories(${THIRD_PARTY_ROOT}/rapidcsv/src)


#######################
# Artifacts
#######################
add_subdirectory(minigraph)
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/minigraph
)

add_executable(bfs_exec apps/BFS.cpp)
target_link_libraries (bfs_exec
    minigraph
)

add_executable(preprocess_exec tools/preprocess.cpp)
target_link_libraries (preprocess_exec
    minigraph
)
