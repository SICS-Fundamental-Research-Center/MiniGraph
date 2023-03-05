# This file is used to find yaml-cpp library in CMake script, modifeid from the
# code from
#
#   https://github.com/BVLC/caffe/blob/master/cmake/Modules/FindGlog.cmake
#
# which is licensed under the 2-Clause BSD License.
#
# - To find yaml-cpp
#
# The following variables are optionally searched for defaults
#  YAML_CPP_ROOT_DIR:            Base directory where all JEMALLOC components are found
#
# The following are set after configuration is done:
#  YAML_CPP_FOUND
#  YAML_CPP_INCLUDE_DIRS
#  YAML_CPP_LIBRARIES

include(FindPackageHandleStandardArgs)

set(YAML_CPP_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/yaml-cpp)
set(YAML_CPP_INCLUDE_DIR ${YAML_CPP_ROOT_DIR}/include)
set(YAML_CPP_LIBRARY ${YAML_CPP_ROOT_DIR}/build/libyaml-cpp.a)

find_package_handle_standard_args(YAML_CPP DEFAULT_MSG YAML_CPP_INCLUDE_DIR YAML_CPP_LIBRARY)

if(YAML_CPP_FOUND)
    set(YAML_CPP_INCLUDE_DIRS ${YAML_CPP_INCLUDE_DIR})
    set(YAML_CPP_LIBRARIES ${YAML_CPP_LIBRARY})
    message(STATUS "Found yaml (include: ${YAML_CPP_INCLUDE_DIRS}, library: ${YAML_CPP_LIBRARIES})")
    mark_as_advanced(YAMP_CPP_LIBRARY_DEBUG YAML_CPP_LIBRARY_RELEASE YAMP_CPP_LIBRARY YAML_CPP_INCLUDE_DIR YAML_CPP_ROOT_DIR)
endif()
