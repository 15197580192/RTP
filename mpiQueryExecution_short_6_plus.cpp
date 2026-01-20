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
#include <iomanip>
#include <ctime>
using namespace std;

struct Forum{
    string id;
    string title;
    string creationDate;
};

struct Post{
    string id;
    string imageFile;
    string creationDate;
    string locationIP;
    string browserUsed;
    string language;
    string content;
    string length;
};

vector<string> query_list;
vector<string> query1_ans_list;
vector<string> query2_ans_list;
vector<string> query3_ans_list;

struct Query1Result {
    Post p;
};

struct Query2Result {
    Forum f;
};

struct Query3Result {
    string forumId;
    string forumTitle;
    string moderatorId;
    string moderatorFirstName;
    string moderatorLastName;
};

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
            
            PyObject* pResult = PyObject_CallObject(pFunc, pArgs);
            Py_DECREF(pArgs);
            if (pResult != nullptr) {
                const char* result = PyUnicode_AsUTF8(pResult);
                str_result = result;
                // printf("Result from Python: %s\n", result);
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

void getQueryList(string filePath) {
    ifstream file(filePath);
    string line, currentQuery;

    if (file) {
        while (getline(file, line)) {
            if (line.empty()) {  // 假设空行为查询之间的分隔符
                if (!currentQuery.empty()) {
                    query_list.push_back(currentQuery); // 保存当前查询
                    currentQuery.clear(); // 重置当前查询字符串
                }
            } else {
                currentQuery += line + "\n";  // 添加行到当前查询
            }
        }
        if (!currentQuery.empty()) { // 不要忘记添加最后一个查询
            query_list.push_back(currentQuery);
        }
    } else {
        cerr << "无法打开文件：" << filePath << endl;
        return ;
    }
}

void stringSplit(string str, string split, vector<string>& res) {
    size_t start = 0;
    size_t end = str.find(split);

    // 循环直到字符串结束
    while (end != std::string::npos) {
        // 添加从start到end位置的子字符串
        res.push_back(str.substr(start, end - start));

        // 更新start的位置，跳过分隔符
        start = end + split.length();

        // 在剩余的字符串中查找下一个分隔符的位置
        end = str.find(split, start);
    }

    // 添加最后一个分段
    res.push_back(str.substr(start, end));
}

vector<string> getAnsList(string ans) {
    vector<string> ans_list;
    string tans = ans.substr(1);
    tans = tans.substr(0, tans.length() - 1);
    // cout << "tans: " << tans << endl;
    stringSplit(tans, "}, {", ans_list);
    // cout << "ans_list: " << endl;
    for (int i = 0; i < ans_list.size(); i++) {
        if (ans_list[i][0] == '{') {
            ans_list[i] = ans_list[i].substr(1);
        }
        if (ans_list[i][ans_list[i].length() - 1] == '}') {
            ans_list[i] = ans_list[i].substr(0, ans_list[i].length() - 1);
        }
        // cout << ans_list[i] << endl;
    }
    // cout << "-------------------------------------------" << endl;
    return ans_list;
}

Forum parseForum(string data) {
    istringstream dataStream(data);
    Forum info;
    string token;

    while (getline(dataStream, token, ',')) {
        // cout << "token: " << token << endl;
        istringstream tokenStream(token);
        string key, value;
        if (getline(getline(tokenStream, key, '='), value)) {
            if (key[0] == ' '){
                key = key.substr(1);
            }
            if (key == "title") info.title = value;
            else if (key == "creationDate") info.creationDate = value;
            else if (key == "id") info.id = value;
        }
    }

    return info;
}

Post parsePost(string data) {
    istringstream dataStream(data);
    Post info;
    string token;

    while (getline(dataStream, token, ',')) {
        // cout << "token: " << token << endl;
        istringstream tokenStream(token);
        string key, value;
        if (getline(getline(tokenStream, key, '='), value)) {
            if (key[0] == ' '){
                key = key.substr(1);
            }
            if (key == "id") info.id = value;
            else if (key == "imageFile") info.imageFile = value;
            else if (key == "creationDate") info.creationDate = value;
            else if (key == "locationIP") info.locationIP = value;
            else if (key == "browserUsed") info.browserUsed = value;
            else if (key == "language") info.language = value;
            else if (key == "content") info.content = value;
            else if (key == "length") info.length = value;
        }
    }

    return info;
}

Query1Result parseQuery1Results(string data) {
    Query1Result q1;
    // cout << "data:" << data << endl;
    Post p = parsePost(data);
    q1.p = p;
    return q1;
}

Query2Result parseQuery2Results(string data) {
    Query2Result q2;
    // cout << "data:" << data << endl;
    Forum f = parseForum(data);
    q2.f = f;
    return q2;
}

Query3Result parseQuery3Results(string data) {
    Query3Result q3;
    // cout << "data:" << data << endl;
    vector<string> sub_data;
    stringSplit(data, ", '", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        // cout << sub_data[i] << endl;
        int pos = sub_data[i].find(':');
        string tdata = sub_data[i].substr(pos + 2);
        if (i == 0) {
            q3.forumId = tdata;
        }
        if (i == 1) {
            q3.forumTitle = tdata;
        }
        if (i == 2) {
            q3.moderatorId = tdata;
        }
        if (i == 3) {
            q3.moderatorFirstName = tdata;
        }
        if (i == 4) {
            q3.moderatorLastName = tdata;
        }
    }
    return q3;
}

void print(Query3Result q) {
    cout << "---------------------------------------" << endl;
    cout << "'forumId': " << q.forumId << ", 'forumTitle': " << q.forumTitle << ", 'moderatorId': " << q.moderatorId;
    cout << ", 'moderatorFirstName': " << q.moderatorFirstName << ", 'moderatorLastName': " << q.moderatorLastName << endl;
}

int main(int argc, char** argv) {
    auto start = std::chrono::high_resolution_clock::now();

    // vector<string> queries = {"MATCH (c:Comment)\n RETURN c\n LIMIT 2"};
    vector<string> servers = {"bolt://10.157.197.82:16200/","bolt://10.157.197.82:16201/","bolt://10.157.197.82:16202/","bolt://10.157.197.82:16203/","bolt://10.157.197.82:16204/","bolt://10.157.197.82:16205/","bolt://10.157.197.82:16206/","bolt://10.157.197.82:16207/"};

    string file_path = "./queries/interactive-short-6-plus.txt";
    string neo4jResult;
    string pid = " ";
    string fid = " ";
    int pid_length = 0;
    int fid_length = 0;
    vector<Query1Result> query1Results;
    vector<Query2Result> query2Results;
    vector<Query3Result> query3Results;

    getQueryList(file_path);

    MPI_Init(&argc, &argv);

    int world_rank, world_size;
    // world_rank = 9;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    ofstream fout;
    string outfile_name = "file_time_" + to_string(world_rank);
    fout.open(outfile_name);

    if (world_rank == 0) {
        auto now = chrono::system_clock::now();
        auto duration_since_epoch = now.time_since_epoch();
        auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
        auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
        time_t now_time_t = chrono::system_clock::to_time_t(now);
        tm* now_tm = localtime(&now_time_t);
        fout << 0 << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
    }

    for (int i = 0; i < query_list.size(); i++) {
        // cout << "当前节点: " << world_rank << endl;
        // cout << "now_query: " << query_list[i] << endl;
        if (i == 0) {
            if (world_rank == 0) {
                // 主节点: 收集来自其他节点的结果
                vector<string> results(world_size - 1);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[1131377];
                    MPI_Recv(buffer, 1131377, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    results[j - 1] = string(buffer);
                    auto now = chrono::system_clock::now();
                    auto duration_since_epoch = now.time_since_epoch();
                    auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                    auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                    time_t now_time_t = chrono::system_clock::to_time_t(now);
                    tm* now_tm = localtime(&now_time_t);
                    fout << i << " " << j << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
                }

                // 显示或处理结果
                for (int j = 0; j < results.size(); ++j) {
                    cout << "从节点 " << (j + 1) << " 收到的结果: " << results[j] << " " << results[j].length() << endl;
                    if (results[j].length() == 2) {
                        continue;
                    }
                    else {
                        vector<string> now_ans = getAnsList(results[j]);
                        for (int k = 0; k < now_ans.size(); k++) {
                            query2_ans_list.push_back(now_ans[k]);
                        }
                    }
                }

                // cout << "query1_ans_list: " << query1_ans_list.size() << endl;

                // pid = "WHERE p.id IN [";
                // for (int j = 0; j < query1_ans_list.size(); j++) {
                //     Query1Result q1;
                //     q1 = parseQuery1Results(query1_ans_list[j]);
                //     query1Results.push_back(q1);
                //     if (j == 0) {
                //         pid = pid + q1.p.id;
                //     }
                //     else {
                //         pid = pid + ", " + q1.p.id;
                //     }
                // }
                // pid = pid + "]\n";
                fid = "WHERE f.id IN [";
                for (int j = 0; j < query2_ans_list.size(); j++) {
                    Query2Result q2;
                    q2 = parseQuery2Results(query2_ans_list[j]);
                    query2Results.push_back(q2);
                    if (j == 0) {
                        fid = fid + q2.f.id;
                    }
                    else {
                        fid = fid + ", " + q2.f.id;
                    }
                }
                fid = fid + "]\n";
                // cout << "pid: " << pid << endl;
            } else {
                auto now = chrono::system_clock::now();
                auto duration_since_epoch = now.time_since_epoch();
                auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                time_t now_time_t = chrono::system_clock::to_time_t(now);
                tm* now_tm = localtime(&now_time_t);
                fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                string result = connect2Neo4j(query_list[i], servers[world_rank - 1]);
                // string result = connect2Neo4j(try_query, servers[world_rank - 1]);
                // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
                // cout << "节点 " << world_rank << " 数据大小为: " << result.size() + 1 << endl;
                MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
            }

            fid_length = fid.size();
            // cout << "节点0的pid_length: " << pid_length << endl;
            MPI_Bcast(&fid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            fid.resize(fid_length);
            MPI_Bcast(&fid[0], fid_length, MPI_CHAR, 0, MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 到达i0同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i0同步墙" << endl;
        }
        // if (i == 1) {
        //     int pos = query_list[i].find('\n');
        //     string str1 = query_list[i].substr(0, pos + 1);
        //     string str2 = query_list[i].substr(pos + 1);

        //     MPI_Bcast(&fid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
        //     // cout << "pid_length: " << pid_length << endl;
        //     if (world_rank != 0) {
        //         // cout << "这不是0号节点" << endl;
        //         // 调整字符串大小以适应接收的数据
        //         fid.resize(fid_length);
        //     }
        //     MPI_Bcast(&fid[0], fid_length, MPI_CHAR, 0, MPI_COMM_WORLD);

        //     if (world_rank != 0) {
        //         string new_query = str1 + pid + str2;

        //         auto now = chrono::system_clock::now();
        //         auto duration_since_epoch = now.time_since_epoch();
        //         auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
        //         auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
        //         time_t now_time_t = chrono::system_clock::to_time_t(now);
        //         tm* now_tm = localtime(&now_time_t);
        //         fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

        //         string result = connect2Neo4j(new_query, servers[world_rank - 1]);
        //         // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
        //         MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        //     }
        //     else {
        //         string new_query = str1 + pid + str2;
        //         // cout << "节点 " << world_rank << endl;
        //         // cout << "new_query: " << new_query << endl;
        //         // cout << "tid: " << tid << endl;

        //         vector<string> results(world_size - 1);
        //         // vector<string> results_1(8);
        //         for (int j = 1; j < world_size; ++j) {
        //             char buffer[89775677];
        //             MPI_Recv(buffer, 89775677, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        //             results[j - 1] = string(buffer);

        //             auto now = chrono::system_clock::now();
        //             auto duration_since_epoch = now.time_since_epoch();
        //             auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
        //             auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
        //             time_t now_time_t = chrono::system_clock::to_time_t(now);
        //             tm* now_tm = localtime(&now_time_t);
        //             fout << i << " " << j << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
        //         }
        //         // cout << "------------------------" << endl;
        //         // cout << "节点0 results.size: " << results.size() << endl;
        //         for (int j = 0; j < results.size(); ++j) {
        //             // cout << "从节点 " << (j + 1) << " 收到的结果: " << results[j] << " " << results[j].length() << endl;
        //             if (results[j].length() == 2) {
        //                 continue;
        //             }
        //             else {
        //                 vector<string> now_ans = getAnsList(results[j]);
        //                 for (int k = 0; k < now_ans.size(); k++) {
        //                     query2_ans_list.push_back(now_ans[k]);
        //                 }
        //             }
        //         }

        //         // cout << "query2_ans_list: " << query2_ans_list.size() << endl;

        //         fid = "WHERE f.id IN [";
        //         for (int j = 0; j < query2_ans_list.size(); j++) {
        //             Query2Result q2;
        //             q2 = parseQuery2Results(query2_ans_list[j]);
        //             query2Results.push_back(q2);
        //             if (j == 0) {
        //                 fid = fid + q2.f.id;
        //             }
        //             else {
        //                 fid = fid + ", " + q2.f.id;
        //             }
        //         }
        //         fid = fid + "]\n";
        //         // cout << "fid: " << fid << endl;
        //     }

        //     fid_length = fid.size();
        //     // cout << "节点0的fid_length: " << fid_length << endl;
        //     MPI_Bcast(&fid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
        //     fid.resize(fid_length);
        //     MPI_Bcast(&fid[0], fid_length, MPI_CHAR, 0, MPI_COMM_WORLD);
        //     // cout << "节点 " << world_rank << " 到达i0同步墙" << endl;
        //     MPI_Barrier(MPI_COMM_WORLD);
        //     // cout << "节点 " << world_rank << " 离开i0同步墙" << endl;
        // }
        if (i == 1) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            MPI_Bcast(&fid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            // cout << "pid_length: " << pid_length << endl;
            if (world_rank != 0) {
                // cout << "这不是0号节点" << endl;
                // 调整字符串大小以适应接收的数据
                fid.resize(fid_length);
            }
            MPI_Bcast(&fid[0], fid_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            if (world_rank != 0) {
                string new_query = str1 + fid + str2;

                auto now = chrono::system_clock::now();
                auto duration_since_epoch = now.time_since_epoch();
                auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                time_t now_time_t = chrono::system_clock::to_time_t(now);
                tm* now_tm = localtime(&now_time_t);
                fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
                
                string result = connect2Neo4j(new_query, servers[world_rank - 1]);
                // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
                MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
            }
            else {
                string new_query = str1 + fid + str2;
                // cout << "节点 " << world_rank << endl;
                // cout << "new_query: " << new_query << endl;
                // cout << "tid: " << tid << endl;

                vector<string> results(world_size - 1);
                // vector<string> results_1(8);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[89775677];
                    MPI_Recv(buffer, 89775677, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    results[j - 1] = string(buffer);

                    auto now = chrono::system_clock::now();
                    auto duration_since_epoch = now.time_since_epoch();
                    auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                    auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                    time_t now_time_t = chrono::system_clock::to_time_t(now);
                    tm* now_tm = localtime(&now_time_t);
                    fout << i << " " << j << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
                }
                // cout << "------------------------" << endl;
                // cout << "节点0 results.size: " << results.size() << endl;
                for (int j = 0; j < results.size(); ++j) {
                    // cout << "从节点 " << (j + 1) << " 收到的结果: " << results[j] << " " << results[j].length() << endl;
                    if (results[j].length() == 2) {
                        continue;
                    }
                    else {
                        vector<string> now_ans = getAnsList(results[j]);
                        for (int k = 0; k < now_ans.size(); k++) {
                            query3_ans_list.push_back(now_ans[k]);
                        }
                    }
                }

                // cout << "query3_ans_list: " << query3_ans_list.size() << endl;

                for (int j = 0; j < query3_ans_list.size(); j++) {
                    Query3Result q3;
                    q3 = parseQuery3Results(query3_ans_list[j]);
                    query3Results.push_back(q3);
                }

                auto now = chrono::system_clock::now();
                auto duration_since_epoch = now.time_since_epoch();
                auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                time_t now_time_t = chrono::system_clock::to_time_t(now);
                tm* now_tm = localtime(&now_time_t);
                fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
                
                for (int i = 0; i < query3Results.size(); i++) {
                    print(query3Results[i]);
                }
            }

            // cout << "节点 " << world_rank << " 到达i0同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i0同步墙" << endl;
        }
        // cout << "i: " << i << endl;
        // cout << "循环结束的当前节点: " << world_rank << endl;
    }

    MPI_Finalize();

    auto end = std::chrono::high_resolution_clock::now();

    // 计算所经过的时间
    std::chrono::duration<double> elapsed = end - start;

    // 输出结果
    // std::cout << "Elapsed time: " << elapsed.count() << " seconds." << std::endl;

    return 0;
}