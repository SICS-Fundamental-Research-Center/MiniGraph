# This file is used to find jemalloc library in CMake script, modifeid from the
# code from
#
#   https://github.com/BVLC/caffe/blob/master/cmake/Modules/FindGlog.cmake
#
# which is licensed under the 2-Clause BSD License.
#
# - Try to find Jemalloc
#
# The following variables are optionally searched for defaults
#  JEMALLOC_ROOT_DIR:            Base directory where all JEMALLOC components are found
#
# The following are set after configuration is done:
#  JEMALLOC_FOUND
#  JEMALLOC_INCLUDE_DIRS
#  JEMALLOC_LIBRARIES
#  JEMALLOC_LIBRARY_DIRS

include(FindPackageHandleStandardArgs)

set(FOLLY_ROOT_DIR "/root/env/folly/folly" CACHE PATH "Folder contains libjemalloc")

# We are testing only a couple of files in the include directories
find_path(FOLLY_INCLUDE_DIR folly PATHS ${FOLLY_ROOT_DIR}/include)

find_library(FOLLY_LIBRARY folly PATHS  ${FOLLY_ROOT_DIR}/lib)

find_package_handle_standard_args(FOLLY DEFAULT_MSG FOLLY_INCLUDE_DIR FOLLY_LIBRARY)


if(FOLLY_FOUND)
    set(FOLLY_INCLUDE_DIRS ${JEMALLOC_INCLUDE_DIR})
    set(FOLLY_LIBRARIES ${JEMALLOC_LIBRARY})
    message(STATUS "Found folly (include: ${FOLLY_INCLUDE_DIRS}, library: ${FOLLY_LIBRARIES})")
    mark_as_advanced(FOLLY_LIBRARY_DEBUG FOLLY_LIBRARY_RELEASE
        FOLLY_LIBRARY FOLLY_INCLUDE_DIR FOLLY_ROOT_DIR)
endif()
