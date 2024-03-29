project (MiniGraph_tests)

include_directories(SYSTEM ${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})
include_directories(SYSTEM ${gmock_SOURCE_DIR}/include ${gmock_SOURCE_DIR})

set (EXECUTABLE_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin)

####################
# Build Unit Tests #
####################
file (GLOB testfiles
        "${CMAKE_CURRENT_SOURCE_DIR}/*_test.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/executors/*_test.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/2d_pie/*_test.cpp"
    )
foreach (testfile ${testfiles})
    get_filename_component (filename ${testfile} NAME_WE)
    string (REPLACE "_test"
            "" testname
            ${filename})
    string (TOUPPER ${testname} TESTNAME)
    add_executable(${filename} "${testfile}")
    target_link_libraries(${filename}
        gtest
        gtest_main
        ${FOLLY_LIBRARIES}
        minigraph
        )
    add_test(NAME "${TESTNAME}" COMMAND "${filename}")
endforeach()