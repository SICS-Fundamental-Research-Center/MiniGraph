cmake_minimum_required(VERSION 3.12)
project(minigraph)

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

# folly
find_package(folly REQUIRED)
if (NOT TARGET Folly::folly)
  include("${FOLLY_CMAKE_DIR}/folly-targets.cmake")
endif()
set(FOLLY_LIBRARIES Folly::folly)

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

# yaml-cpp
include("${CMAKE_CURRENT_SOURCE_DIR}/../cmake/Findyaml-cpp.cmake" OPTIONAL)
if (NOT YAML_CPP_FOUND)
    message(STATUS "yaml-cpp not found, build without yaml-cpp")
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



#######################
# Artifacts
#######################

file(GLOB MINIGRAPH_SOURCES
        "*.cpp"
        "executors/*.cpp"
        "2d_pie/*.cpp"
        "graphs/*.cpp"
        "portability/*.cpp"
        "state_machine/*.cpp"
        "utility/*.cpp"
        "message_manager/*.cpp"
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
add_library (minigraph ${MINIGRAPH_SOURCES})

target_link_libraries(minigraph
        ${FOLLY_LIBRARIES}
        ${YAML_CPP_LIBRARIES}
        ${JEMALLOC_LIBRARIES}
        )
