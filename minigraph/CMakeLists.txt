cmake_minimum_required(VERSION 3.12)
project(libminigraph)

# Directories
set (EXECUTABLE_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin)
set (LIBRARY_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin)

###### Custom options ######
option(USE_JEMALLOC "Whether to use jemalloc, default: ON." ON)

#######################
# Libraries
#######################
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
)

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
# Children CMakeLists
#######################

#include_directories ("third_party/")
#add_subdirectory("tests")


#######################
# Artifacts
#######################

file(GLOB MINIGRAPH_SOURCES
    "*.cpp"
    "graphs/*.cpp"
    "portability/*.cpp"
    "state_machine/*.cpp"
    "utility/*.cpp"
    )


#######################
# Tests
#######################
if (test)
  add_subdirectory("tests")
endif()


#######################
# Generate libminigraph
#######################
add_library (libminigraph
        ${MINIGRAPH_SOURCES} utility/io/data_mngr.h 2d_pie/edge_map_reduce.h 2d_pie/auto_app_base.h)

target_link_libraries(libminigraph
        ${FOLLY_LIBRARIES} )
