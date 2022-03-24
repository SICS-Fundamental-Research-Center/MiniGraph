project (MiniGraph_tests)

include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

set (EXECUTABLE_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin)

####################
# Build Unit Tests #
####################
file (GLOB testfiles "${CMAKE_CURRENT_SOURCE_DIR}/*_test.cpp")
foreach (testfile ${testfiles})
    get_filename_component (filename ${testfile} NAME_WE)
    string (REPLACE "_test"
            "" testname
            ${filename})
    string (TOUPPER ${testname} TESTNAME)
    add_executable(${filename} "${filename}.cpp")
    target_link_libraries(${filename}
        gtest
        gtest_main
        minigraph
        )
    add_test(NAME "${TESTNAME}" COMMAND "${filename}")
endforeach()