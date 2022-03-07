project(MiniGraph)
cmake_minimum_required(VERSION 3.0)
set(MINIGRAPH_COPYRIGHT_YEAR "2022")
set(CMAKE_CXX_STANDARD 17)

###### Set default cmake type to Debug ######
if(NOT CMAKE_BUILD_TYPE)
  # cmake default flags with relwithdebinfo is -O2 -g
  # cmake default flags with release is -O3 -DNDEBUG
  set(CMAKE_BUILD_TYPE "Debug")
endif()
message(STATUS "[MiniGraph] will build in type: ${CMAKE_BUILD_TYPE}")



###### Cmake options ######
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -Wall")

####### Find library dependencies ######
# Folly
find_package(folly REQUIRED)

# Boost-ext sml
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/sml)

macro(install_libminigraph_target target)
  # install
  install(TARGETS ${target}
          EXPORT libgminigraph-targets
          ARCHIVE DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/lib
          LIBRARY DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/lib
          RUNTIME DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/bin
  )
endmacro()

###### Generate MiniGraph ######
file(GLOB_RECURSE CORE_SRC_FILES
    "minigraph/*.cc"
    "minigraph/*.cpp"
)
add_library(${PROJECT_NAME}-lite ${CORE_SRC_FILES})
install_libminigraph_target(${PROJECT_NAME}-lite)
target_include_directories(${PROJECT_NAME}-lite PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/minigraph third_party/sml/include)


add_executable(analytical_system_components examples/system_components/state_machine/test_state_machine.cc examples/system_components/run_anal_system.cc ${CORE_SRC_FILES})
target_include_directories(analytical_system_components PRIVATE examples/system_components third_party/sml/include ${CMAKE_CURRENT_SOURCE_DIR}/minigraph)

set_target_properties(analytical_system_components PROPERTIES OUTPUT_NAME run_anal_system)
target_link_libraries(analytical_system_components ${PROJECT_NAME}-lite)
install_libminigraph_target(analytical_system_components)


###### Format code ######
file(GLOB_RECURSE FILES_NEED_FORMAT "minigraph/*.cc"
                                    "minigraph/*.h"
                                    "examples/*.h"
                                    "examples/*.cc")
foreach (file_path ${FILES_NEED_FORMAT})
  if (${file_path} MATCHES ".*third_party.*")
    list(REMOVE_ITEM FILES_NEED_FORMAT ${file_path})
  endif ()
endforeach ()


###### cpplint, check for readability with Google style ######
add_custom_target(clformat
                  COMMAND clang-format --style=file -i ${FILES_NEED_FORMAT}
                  COMMENT "Running clang-format."
                  VERBATIM)

###### Generate docs ######
if(BUILD_LIBGRAPELITE_DOCS)
  add_custom_target(doc COMMAND doxygen "${CMAKE_CURRENT_SOURCE_DIR}/misc/doc-config"
                    COMMENT "Generating docs."
                    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                    VERBATIM)
endif()


#include_directories(SYSTEM ${MPI_CXX_INCLUDE_PATH})



#message(STATUS "[MiniGraph]" ${CMAKE_CURRENT_SOURCE_DIR})
#get_property(dirs DIRECTORY ${CMAKE_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
#message(">>> include_dirs=${dirs}") #打印一下目录情况
#option(USE_JEMALLOC "Whether to use jemalloc." OFF)
#
#
#
#message(STATUS "[MiniGraph] CP: " ${CMAKE_INSTALL_RPATH})
#message(STATUS "[MiniGraph] CP: " ${CMAKE_INSTALL_PREFIX}/lib)


#include_directories(third_party)
#include_directories(minigraph)
#macro(FIND_INCLUDE_DIR result curdir)
#    file(GLOB_RECURSE children "${curdir}/*.hpp" "${curdir}/*.h" )
#    message(STATUS "children= ${children}")
#    set(dirlist "")
#    foreach(child ${children})
#        string(REGEX REPLACE "(.*)/.*" "\\1" LIB_NAME ${child})
#        if(IS_DIRECTORY ${LIB_NAME})
#            LIST(APPEND dirlist ${LIB_NAME})
#        endif()
#    endforeach()
#    set(${result} ${dirlist})
#endmacro()
#
#
#
#file(GLOB_RECURSE FILES_NEED_FORMAT "minigraph/*.cc"
#                                    "minigraph/*.h"
#                                    "minigraph/*.hpp"
#                                    "minigraph/*.cpp"
#                                    "examples/*.h"
#                                    "examples/*.hpp"
#                                    "examples/*.cc"
#                                    "examples/*.cpp"
#                                    )
#foreach (file_path ${FILES_NEED_FORMAT})
#if (${file_path} MATCHES ".*third_party.*")
#  list(REMOVE_ITEM FILES_NEED_FORMAT ${file_path})
#endif ()
#endforeach ()
#
#message(STATUS "[MiniGraph] ${FILES_NEED_FORMAT}")
#
#
#
####### Find Libraies ######
#
## find gflags-------------------------------------------------------------------
#include("cmake/FindGFlags.cmake")
#if (NOT GFLAGS_FOUND)
#  message(STATUS "gflags not found, build without gflags")
#else ()
#  include_directories(SYSTEM ${GFLAGS_INCLUDE_DIRS})
#endif ()
#
#
#
####### Generate libminigraph ######
#
#set(EXAMPLES_DIR ${PROJECT_SOURCE_DIR}/examples)
#
#message(STATUS "[MiniGraph] Project source DIR" "${PROJECT_SOURCE_DIR}")
#
#install(DIRECTORY ${EXAMPLES_DIR}/system_components/thread_pool
#        DESTINATION include/minigraph/analytical_apps
#        FILES_MATCHING
#        PATTERN "*.h"
#)

#macro(install_libminigraph_target target)
#  # install
#  install(TARGETS ${target}
#          EXPORT libminigraph-targets
#          ARCHIVE DESTINATION lib
#          LIBRARY DESTINATION lib
#          RUNTIME DESTINATION bin
#  )
#endmacro()
#
#
#file(GLOB_RECURSE CORE_SRC_FILES "minigraph/*.cc")
##add_library(MiniGraph ${CORE_SRC_FILES})
#install_libminigraph_target(MiniGraph)
#
#target_link_libraries(MiniGraph ${GLOG_LIBRARIES})
#message(STATUS "[MiniGraph]" "Core_SRC_FILES: " "${CORE_SRC_FILES}")
#
#
#
#



#
#set(MINIGRAPH_NUM_TEST_THREADS "" CACHE STRING "Maximum number of threads to use when running tests (default: number of physical cores)")
#
#if(NOT MINIGRAPH_NUM_TEST_THREADS)
#  cmake_host_system_information(RESULT MINIGRAPH_NUM_TEST_THREADS QUERY NUMBER_OF_PHYSICAL_CORES)
#endif()
#if(MINIGRAPH_NUM_TEST_THREADS LESS_EQUAL 0)
#  set(MINIGRAPH_NUM_TEST_THREADS 1)
#endif()
#
#
#set(CMAKE_CXX_STANDARD 17)
#set(CMAKE_CXX_STANDARD_REQUIRED ON)
#set(CMAKE_CXX_EXTENSIONS OFF) #...without compiler extensions like gnu++11
#set(CMAKE_POSITION_INDEPENDENT_CODE ON)
#
## Always include debug info
#add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-g>")
#
## GCC
#if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
#  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7)
#    message(FATAL_ERROR "gcc must be version 7 or higher. Found ${CMAKE_CXX_COMPILER_VERSION}.")
#  endif()
#
#  add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Wall;-Wextra>")
#
#  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 11)
#    add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Werror>")
#  endif()
#endif()
#
####### Source finding ######
## Core libraries (lib)
#add_subdirectory(libminigraph)
##add_subdirectory(tools)
#
#set(MINIGRAPH_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/libutil)
#
####### Find libraries ######
#
## Find jemalloc-----------------------------------------------------------------
#if (USE_JEMALLOC)
#  include("cmake/FindJemalloc.cmake")
#  if (NOT JEMALLOC_FOUND)
#    message(STATUS "jemalloc not found, build without jemalloc")
#  else ()
#    add_definitions(-DUSE_JEMALLOC)
#    include_directories(SYSTEM ${JEMALLOC_INCLUDE_DIRS})
#  endif ()
#endif ()
#
#
#
#

#
#