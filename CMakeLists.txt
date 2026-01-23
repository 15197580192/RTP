# 设置最低CMake版本要求
cmake_minimum_required(VERSION 3.0)

# 设置项目名称
project(rsp_query)

# 设置链接器标志，增大栈大小
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-z,stack-size=4294967296")

# 添加编译定义
add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0 -D__STDC_LIMIT_MACROS)

# 查找依赖库
find_package(PythonLibs REQUIRED)
find_package(MPI REQUIRED) #OpenMPI

# 添加头文件查找路径
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PYTHON_INCLUDE_DIRS}
    ${MPI_INCLUDE_PATH}
)

# ========== 核心优化：用列表+循环替代重复代码 ==========
# 定义可执行文件列表（名称 + 对应的源文件）
set(EXECUTABLE_TARGETS
    is1     mpiQueryExecution_short_1.cpp
    is1o     mpiQueryExecution_short_1-other.cpp
    is2     mpiQueryExecution_short_2.cpp
    is2p    mpiQueryExecution_short_2_plus.cpp
    is2o    mpiQueryExecution_short_2-other.cpp
    is3     mpiQueryExecution_short_3.cpp
    is4     mpiQueryExecution_short_4.cpp
    is5     mpiQueryExecution_short_5.cpp
    is6     mpiQueryExecution_short_6.cpp
    is6p    mpiQueryExecution_short_6_plus.cpp
    is7     mpiQueryExecution_short_7.cpp
    is7p    mpiQueryExecution_short_7_plus.cpp
    ic1     mpiQueryExecution_complex_1.cpp
    ic1p    mpiQueryExecution_complex_1_plus.cpp
    ic2     mpiQueryExecution_complex_2.cpp
    ic2p    mpiQueryExecution_complex_2_plus.cpp
    ic3     mpiQueryExecution_complex_3.cpp
    ic4     mpiQueryExecution_complex_4.cpp
    ic4p    mpiQueryExecution_complex_4_plus.cpp
    ic5     mpiQueryExecution_complex_5.cpp
    ic5p    mpiQueryExecution_complex_5_plus.cpp
    ic6     mpiQueryExecution_complex_6.cpp
    ic6p    mpiQueryExecution_complex_6_plus.cpp
    ic7     mpiQueryExecution_complex_7.cpp
    ic8     mpiQueryExecution_complex_8.cpp
    ic8p    mpiQueryExecution_complex_8_plus.cpp
    ic9     mpiQueryExecution_complex_9.cpp
    ic9p    mpiQueryExecution_complex_9_plus.cpp
    ic10    mpiQueryExecution_complex_10.cpp
    ic10p   mpiQueryExecution_complex_10_plus.cpp
    ic11    mpiQueryExecution_complex_11.cpp
    ic11p   mpiQueryExecution_complex_11_plus.cpp
    ic12    mpiQueryExecution_complex_12.cpp
    ic12p   mpiQueryExecution_complex_12_plus.cpp
    ic13    mpiQueryExecution_complex_13.cpp
    ic14    mpiQueryExecution_complex_14.cpp
    ic14p   mpiQueryExecution_complex_14_plus.cpp
)

# 正确遍历成对元素：用while循环+列表弹出
list(LENGTH EXECUTABLE_TARGETS list_len)
math(EXPR list_len "${list_len} - 1")
set(index 0)
while(index LESS ${list_len})
    # 取出目标名（第index个元素）
    list(GET EXECUTABLE_TARGETS ${index} target_name)
    math(EXPR index "${index} + 1")
    # 取出源文件名（第index个元素）
    list(GET EXECUTABLE_TARGETS ${index} src_file)
    math(EXPR index "${index} + 1")
    
    # 添加可执行文件
    add_executable(${target_name} ${src_file})
    # 链接依赖库
    target_link_libraries(${target_name}
        ${PYTHON_LIBRARIES}
        ${MPI_CXX_LIBRARIES}
    )
endwhile()