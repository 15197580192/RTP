# 设置最低CMake版本要求
cmake_minimum_required(VERSION 3.0)

# 设置项目名称
project(rsp_query)

# 查找Python库
find_package(PythonLibs REQUIRED)
# 查找MPI库（这里以OpenMPI为例，不同MPI实现可能稍有不同）
find_package(MPI REQUIRED)

# 添加头文件查找路径
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PYTHON_INCLUDE_DIRS}
    ${MPI_INCLUDE_PATH}
)

add_executable(is1 mpiQueryExecution_short_1.cpp)
add_executable(is2 mpiQueryExecution_short_2.cpp)
add_executable(is3 mpiQueryExecution_short_3.cpp)
add_executable(is4 mpiQueryExecution_short_4.cpp)
add_executable(is5 mpiQueryExecution_short_5.cpp)
add_executable(is6 mpiQueryExecution_short_6.cpp)
add_executable(is7 mpiQueryExecution_short_7.cpp)

add_executable(ic1 mpiQueryExecution_complex_1.cpp)
add_executable(ic2 mpiQueryExecution_complex_2.cpp)
add_executable(ic3 mpiQueryExecution_complex_3.cpp)
add_executable(ic4 mpiQueryExecution_complex_4.cpp)
add_executable(ic5 mpiQueryExecution_complex_5.cpp)
add_executable(ic6 mpiQueryExecution_complex_6.cpp)
add_executable(ic7 mpiQueryExecution_complex_7.cpp)
add_executable(ic8 mpiQueryExecution_complex_8.cpp)
add_executable(ic9 mpiQueryExecution_complex_9.cpp)
add_executable(ic10 mpiQueryExecution_complex_10.cpp)
add_executable(ic11 mpiQueryExecution_complex_11.cpp)
add_executable(ic12 mpiQueryExecution_complex_12.cpp)
add_executable(ic13 mpiQueryExecution_complex_13.cpp)
add_executable(ic14 mpiQueryExecution_complex_14.cpp)

# 链接Python库和MPI库
target_link_libraries(is1
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)
target_link_libraries(is2
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)
target_link_libraries(is3
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)
target_link_libraries(is4
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)
target_link_libraries(is5
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)
target_link_libraries(is6
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)
target_link_libraries(is7
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

target_link_libraries(ic1
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

target_link_libraries(ic2
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

target_link_libraries(ic3
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

target_link_libraries(ic4
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

target_link_libraries(ic5
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

target_link_libraries(ic6
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

target_link_libraries(ic7
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

target_link_libraries(ic8
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

target_link_libraries(ic9
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

target_link_libraries(ic10
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

target_link_libraries(ic11
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

target_link_libraries(ic12
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

target_link_libraries(ic13
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

target_link_libraries(ic14
    ${PYTHON_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

