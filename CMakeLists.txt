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

