cmake_minimum_required(VERSION 3.12)

project(minigraph)
set(MINIGRAPH_COPYRIGHT_YEAR "2022")

###### Cmake options ######
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -Wall")

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
  set (BUILD_GMOCK OFF CACHE BOOL " ")
  set (BUILD_GTEST ON CACHE BOOL " ")
  add_subdirectory("${THIRD_PARTY_ROOT}/googletest")
endif()


#######################
# Artifacts
#######################
add_subdirectory(minigraph)

#add_executable(minigraph_exec main.cpp)
#target_link_libraries (minigraph_exec
#    minigraph
#)
