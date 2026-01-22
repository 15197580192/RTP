#include <iostream>
#include <cstring>
#include <algorithm>
#include <vector>
#include <map>
#include <fstream>
#include <sstream>
#include <optional>
#include <chrono>
#include <Python.h>
#include <mpi.h>
using namespace std;

int main(int argc, char** argv) {
    string oid = "";
    string tid = "";
    string woid = " ";
    int tid_length = 0;
    int oid_length = 0;
    int woid_length = 0;

    MPI_Init(&argc, &argv);

    int world_rank, world_size;
    // world_rank = 9;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    for (int i = 0; i < 3; i++) {
        if (i == 0) {

            if (world_rank == 0) {
                tid = "我是tid\n";
            }
            tid_length = tid.size();
            cout << "节点0的tid_length: " << tid_length << endl;
            MPI_Bcast(&tid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            tid.resize(tid_length);
            MPI_Bcast(&tid[0], tid_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            MPI_Barrier(MPI_COMM_WORLD);
        }
        if (i == 1) {
            MPI_Bcast(&tid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            if (world_rank != 0) {
                tid.resize(tid_length);
            }
            MPI_Bcast(&tid[0], tid_length, MPI_CHAR, 0, MPI_COMM_WORLD);
            // woid = tid;
            cout << "节点 " << world_rank << " tid: " << tid << endl;
            if (world_rank != 0) {
                // string().swap(tid);
                tid = "";
                tid.clear();
            }

            if (world_rank == 0) {
                oid = "我是一个更长的oid\n";
            }
            oid_length = oid.size();
            woid_length = oid.size() - 10;
            cout << "节点0的oid_length: " << oid_length << endl;
            MPI_Bcast(&oid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            oid.resize(oid_length);
            // MPI_Bcast(&woid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            MPI_Bcast(&oid[0], oid_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            MPI_Barrier(MPI_COMM_WORLD);
        }
        if (i == 2) {
            MPI_Bcast(&oid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            if (world_rank != 0) {
                cout << "节点 " << world_rank << " oid_length: " << oid_length << endl;
                oid.resize(oid_length);
            }
            cout << "节点 " << world_rank << " oid: " << oid << endl;
            // MPI_Bcast(&woid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            MPI_Bcast(&oid[0], oid_length, MPI_CHAR, 0, MPI_COMM_WORLD);
            cout << "节点 " << world_rank << " woid: " << woid_length << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            cout << "经过同步墙" << endl;
        }
    }

    MPI_Finalize();
    return 0;
}