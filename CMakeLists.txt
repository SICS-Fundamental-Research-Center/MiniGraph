project(MiniGraph)
cmake_minimum_required(VERSION 3.0)
set(MINIGRAPH_COPYRIGHT_YEAR "2022")
set(CMAKE_CXX_STANDARD 14)

###### Set default cmake type to Debug ######
if(NOT CMAKE_BUILD_TYPE)
  # cmake default flags with relwithdebinfo is -O2 -g
  # cmake default flags with release is -O3 -DNDEBUG
  set(CMAKE_BUILD_TYPE "Debug")
endif()
message(STATUS "[MiniGraph] will build in type: ${CMAKE_BUILD_TYPE}")
option(USE_JEMALLOC "Whether to use jemalloc." ON)



###### Cmake options ######
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -Wall")

####### Find library dependencies ######
# Folly
find_package(folly REQUIRED)
if (NOT TARGET Folly::folly)
  include("${FOLLY_CMAKE_DIR}/folly-targets.cmake")
endif()
set(FOLLY_LIBRARIES Folly::folly)
if (NOT folly_FIND_QUIETLY)
  message(STATUS "Found folly: ${PACKAGE_PREFIX_DIR}")
endif()

# find jemalloc
if (USE_JEMALLOC)
  include("cmake/FindJemalloc.cmake")
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

# gflag
find_package(gflags REQUIRED)

# Boost-ext sml
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/sml/include)
message(STATUS "Including Boost-ext:SML library in: ${CMAKE_CURRENT_SOURCE_DIR}/third_party/sml/include")

# RapidCsv
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/rapidcsv/src)
message(STATUS "Including rapidcsv:rapidcsv library in: ${CMAKE_CURRENT_SOURCE_DIR}/third_party/rapidcsv/src")

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
SET(USAGE "STATEMACHINE" CACHE STRING "Some user-specified option")

set(EXAMPLES_DIR ${PROJECT_SOURCE_DIR}/examples)
set(TEST_DIR ${PROJECT_SOURCE_DIR}/test)

file(GLOB_RECURSE CORE_SRC_FILES
    "minigraph/*.cc"
    "minigraph/*.cpp"
)

message(STATUS "CORE_SRC: " ${CORE_SRC_FILES})
message(STATUS "[MiniGraph] TEST: |${USAGE}|")
add_library(${PROJECT_NAME}-lite ${CORE_SRC_FILES} minigraph/utility/io/csr_io_adapter.h minigraph/utility/paritioner/edge_cut_partitioner.h minigraph/portability/sys_types.h minigraph/components/load_component.h minigraph/components/computing_component.h minigraph/components/discharge_component.h test/system_components/test_folly_pc.cpp minigraph/minigraph_sys.h)
install_libminigraph_target(${PROJECT_NAME}-lite)
target_include_directories(${PROJECT_NAME}-lite PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/minigraph third_party/sml/include ${CMAKE_THREAD_LIBS_INIT})

if(${USAGE} MATCHES "STATEMACHINE")
  add_executable(analytical_system_components ${TEST_DIR}/system_components/state_machine/test_state_machine.cpp ${CORE_SRC_FILES})
  target_include_directories(analytical_system_components PRIVATE ${TEST_DIR}/system_components third_party/sml/include ${CMAKE_CURRENT_SOURCE_DIR}/minigraph)
  set_target_properties(analytical_system_components PROPERTIES OUTPUT_NAME run_anal_system)
  target_link_libraries(analytical_system_components ${PROJECT_NAME}-lite ${FOLLY_LIBRARIES})
  install_libminigraph_target(analytical_system_components)
elseif(${USAGE} MATCHES  "LOGGING")
  add_executable(analytical_utility ${TEST_DIR}/logging/test_logging.cpp ${CORE_SRC_FILES})
  target_include_directories(analytical_utility PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/minigraph)
  set_target_properties(analytical_utility PROPERTIES OUTPUT_NAME run_anal_system)
  target_link_libraries(analytical_utility ${PROJECT_NAME}-lite ${FOLLY_LIBRARIES})
  install_libminigraph_target(analytical_utility)
elseif(${USAGE} MATCHES  "THREADPOOL")
  add_executable(analytical_utility ${TEST_DIR}/system_components/thread_pool/test_folly_thread_pool.cpp ${CORE_SRC_FILES})
  target_include_directories(analytical_utility PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/minigraph)
  set_target_properties(analytical_utility PROPERTIES OUTPUT_NAME run_anal_system)
  target_link_libraries(analytical_utility ${PROJECT_NAME}-lite ${FOLLY_LIBRARIES})
  install_libminigraph_target(analytical_utility)
elseif(${USAGE} MATCHES  "FILE")
  add_executable(analytical_utility ${TEST_DIR}/system_components/file_system/test_folly_file.cpp ${CORE_SRC_FILES})
  message(STATUS "CORE_SRC: " ${CORE_SRC_FILES})
  target_include_directories(analytical_utility PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/minigraph)
  set_target_properties(analytical_utility PROPERTIES OUTPUT_NAME run_anal_system)
  target_link_libraries(analytical_utility ${PROJECT_NAME}-lite ${FOLLY_LIBRARIES})
  install_libminigraph_target(analytical_utility)
elseif(${USAGE} MATCHES  "PC")
  add_executable(analytical_utility ${TEST_DIR}/system_components/test_folly_pc.cpp ${CORE_SRC_FILES})
  message(STATUS "CORE_SRC: " ${CORE_SRC_FILES})
  target_include_directories(analytical_utility PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/minigraph)
  set_target_properties(analytical_utility PROPERTIES OUTPUT_NAME run_anal_system)
  target_link_libraries(analytical_utility ${PROJECT_NAME}-lite ${FOLLY_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
  install_libminigraph_target(analytical_utility)
elseif(${USAGE} MATCHES  "SYSTEM")
  add_executable(analytical_utility ${TEST_DIR}/test_system.cpp ${CORE_SRC_FILES})
  message(STATUS "CORE_SRC: " ${CORE_SRC_FILES})
  target_include_directories(analytical_utility PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/minigraph)
  set_target_properties(analytical_utility PROPERTIES OUTPUT_NAME run_anal_system)
  target_link_libraries(analytical_utility ${PROJECT_NAME}-lite ${FOLLY_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
  install_libminigraph_target(analytical_utility)
endif()

###### Format code ######
file(GLOB_RECURSE FILES_NEED_FORMAT "minigraph/*.cc"
                                    "minigraph/*.h"
                                    "minigraph/*.cpp"
                                    "examples/*.h"
                                    "examples/*.cc"
                                    "examples/*.cpp"
                                    "test/*.h"
                                    "test/*.cc"
                                    "test/*.cpp"
                                    )
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

