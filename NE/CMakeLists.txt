cmake_minimum_required(VERSION 3.5)
project(DistributedNE)

# ========== 改用CMake标准MPI模块（推荐） ==========
# 查找MPI（会自动识别mpic++/mpicc编译器）
find_package(MPI REQUIRED)
# 自动添加MPI的头文件路径
include_directories(${MPI_INCLUDE_PATH})

# 指定C++编译器（可选，find_package(MPI)会自动识别）
set(CMAKE_CXX_COMPILER mpic++)
# 编译标志：C++11 + O3优化 + OpenMP + 并行STL
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O3 -D_GLIBCXX_PARALLEL -fopenmp")

# 编译模式选择（二选一）
# 调试模式：add_compile_options(-g)
# 发布模式：
add_compile_options(-DNDEBUG)

###### User defined parameters #####
## UINT32_VERTT: Vertex ID type becomes 32-bit unsigned int instead of 64-bit
#add_compile_options(-DUINT32_VERTT)
#
## MAX_NUM_PARTS: The maximum number of partitions. Default is 1024
#add_compile_options(-DMAX_NUM_PARTS=2028)
####################################

# 指定头文件路径
include_directories(. include ${CMAKE_BINARY_DIR}/include)

# 编译可执行文件
add_executable(DistributedNE src/main.cpp)

# ========== 链接MPI库（标准方式） ==========
# MPI_CXX_LIBRARIES 是find_package(MPI)自动生成的变量，包含MPI库路径
target_link_libraries(DistributedNE ${MPI_CXX_LIBRARIES})
# 额外链接OpenMP（确保OpenMP生效）
find_package(OpenMP REQUIRED)
target_link_libraries(DistributedNE OpenMP::OpenMP_CXX)