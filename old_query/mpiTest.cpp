#include <mpi.h>
#include <iostream>
#include <string>
#include <vector>
#include <Python.h>
using namespace std;

// 假设这是您提供的函数
string connect2Neo4j(string sentence, string nowIP) {
    // 初始化 Python 解释器
    Py_Initialize();
    string str_result;

    // 添加当前目录到 Python 系统路径，以便能够导入本地 Python 文件
    PyRun_SimpleString("import sys");
    PyRun_SimpleString("sys.path.append('.')");

    // 导入 Python 模块
    PyObject* pName = PyUnicode_DecodeFSDefault("getConnect");
    PyObject* pModule = PyImport_Import(pName);
    Py_DECREF(pName);

    if (pModule != nullptr) {
        // 获取模块中的函数
        PyObject* pFunc = PyObject_GetAttrString(pModule, "get_result");

        if (pFunc && PyCallable_Check(pFunc)) {
            PyObject* pArgs = PyTuple_New(2); // 创建一个包含一个元素的元组
            PyObject* pValue = PyUnicode_FromString(sentence.c_str()); // 将 C++ 字符串转换为 Python 字符串
            PyTuple_SetItem(pArgs, 0, pValue); // 将 Python 字符串放入元组
            pValue = PyUnicode_FromString(nowIP.c_str());
            PyTuple_SetItem(pArgs, 1, pValue);
            
            cout << pFunc << " " << pArgs << endl;

            if (pArgs != NULL && PyTuple_Check(pArgs)) {
                Py_ssize_t n = PyTuple_Size(pArgs);
                printf("Function arguments tuple of size %zd:\n", n);
                for (Py_ssize_t i = 0; i < n; i++) {
                    PyObject* item = PyTuple_GetItem(pArgs, i); // Borrowed reference
                    PyObject* repr = PyObject_Repr(item); // Get a string representation of the item
                    const char* item_str = PyUnicode_AsUTF8(repr);
                    printf("Argument %zd: %s\n", i, item_str);
                    Py_XDECREF(repr);
                }
            } else {
                printf("pArgs is not a tuple.\n");
            }

            PyObject* pResult = PyObject_CallObject(pFunc, pArgs);
            // cout << "能调用完" << endl;
            Py_DECREF(pArgs);
            // cout << "能调用完" << endl;
            if (pResult != nullptr) {
                fprintf(stderr, "Python function executed successfully.\n");
                // cout << "能调用完" << endl;
                if (!PyTuple_Check(pResult)) {
                    PyErr_Print();
                    fprintf(stderr, "Provided object is not a tuple\n");
                    return NULL;  // 或其他错误处理
                }
                // cout << pResult << endl;
                const char* result = PyUnicode_AsUTF8(pResult);
                cout << "result: " << result << endl;
                str_result = result;
                printf("Result from Python: %s\n", result);
                // printf("Result: %s\n", PyUnicode_AsUTF8(pResult));
                Py_DECREF(pResult);
            } else {
                PyErr_Print();
            }
        }
        else {
            if (PyErr_Occurred())
                PyErr_Print();
            fprintf(stderr, "Cannot find function \"get_result\"\n");
        }
        Py_XDECREF(pFunc);
        Py_DECREF(pModule);
    }
    else {
        PyErr_Print();
        fprintf(stderr, "Failed to load \"mypyfile\"\n");
    }

    // 关闭 Python 解释器
    Py_Finalize();
    return str_result;
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int world_rank, world_size;
    // world_rank = 9;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    // 准备一些示例查询和服务器IP
    vector<string> queries = {"MATCH (c:Comment)\n RETURN c\n LIMIT 2"};
    vector<string> servers = {"http://122.9.162.58:7474/", "http://116.63.178.28:7474/", "http://122.9.132.101:7474/", "http://139.9.233.77:7474/", "http://122.9.144.74:7474/", "http://116.63.188.96:7474/", "http://139.9.246.15:7474/", "http://116.63.183.28:7474/"};

    if (world_rank == 0) {
        // 主节点: 收集来自其他节点的结果
        vector<string> results(world_size - 1);
        for (int i = 1; i < world_size; ++i) {
            char buffer[512];
            MPI_Recv(buffer, 512, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            results[i - 1] = string(buffer);
        }

        // 显示或处理结果
        for (int i = 0; i < results.size(); ++i) {
            cout << "从节点 " << (i + 1) << " 收到的结果: " << results[i] << endl;
        }
    } else {
        // 从节点: 执行查询并发送结果回主节点
        string result = connect2Neo4j(queries[0], servers[world_rank - 1]);
        cout << "节点 " << world_rank - 1 << " 向服务器ip" << servers[world_rank - 1] << "发送信息" << endl;
        MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}