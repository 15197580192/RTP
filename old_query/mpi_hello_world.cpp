#include <mpi.h>
#include <iostream>

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv); // 初始化MPI环境

    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size); // 获取总进程数

    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank); // 获取当前进程的秩

    std::cout << "Hello world from rank " << world_rank << " out of " << world_size << " processors\n";

    MPI_Finalize(); // 结束MPI环境
    return 0;
}
