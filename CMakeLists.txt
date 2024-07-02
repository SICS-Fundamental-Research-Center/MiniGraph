cmake_minimum_required(VERSION 2.8)

project(minigraph)
set(MINIGRAPH_COPYRIGHT_YEAR "2022")

###### Cmake options ######
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")

# Set default cmake type to Debug
if (NOT CMAKE_BUILD_TYPE)
    # cmake default flags with relwithdebinfo is -O2 -g
    # cmake default flags with release is -O3 -DNDEBUG
    set(CMAKE_BUILD_TYPE "Debug")
endif ()
message(STATUS "[MiniGraph] will build in type: ${CMAKE_BUILD_TYPE}")


###### Directories shortcuts ######
set(PROJECT_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(THIRD_PARTY_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin)


###### Add third party to the project, if necessary ######
if (test)
    enable_testing()
    message(STATUS "Enabling GoogleTest...")
    set(BUILD_GMOCK ON CACHE BOOL "Option: build gmock and gtest -- ON" FORCE)
    set(INSTALL_GTEST OFF CACHE BOOL "Option: install gtest -- OFF" FORCE)
    add_subdirectory("${THIRD_PARTY_ROOT}/googletest")
endif ()


#######################
# Libraries
#######################
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

# yaml-cpp
add_subdirectory("${THIRD_PARTY_ROOT}/yaml-cpp")
include_directories(${THIRD_PARTY_ROOT}/yaml-cpp/include/)

# common
include_directories(${PROJECT_ROOT_DIR}/common)

#######################
# Artifacts
#######################
add_subdirectory(minigraph)
include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}/minigraph
)

file(GLOB tools "${CMAKE_CURRENT_SOURCE_DIR}/tools/*.cpp")
foreach (appfile ${tools})
    file(GLOB appfiles "${CMAKE_CURRENT_SOURCE_DIR}/tools/*.cpp")
    message(STATUS "Found apps: ${appfile}")
    get_filename_component(app ${appfile} NAME_WE)
    add_executable("${app}_exec" ${appfile})
    target_link_libraries("${app}_exec" minigraph_core yaml-cpp::yaml-cpp)
endforeach ()

file(GLOB appfiles "${CMAKE_CURRENT_SOURCE_DIR}/apps/cpp/*.cpp")
foreach (appfile ${appfiles})
    message(STATUS "Found apps: ${appfile}")
    get_filename_component(app ${appfile} NAME_WE)
    add_executable("${app}_exec" ${appfile})
    target_link_libraries("${app}_exec" minigraph_core yaml-cpp::yaml-cpp)
endforeach ()

