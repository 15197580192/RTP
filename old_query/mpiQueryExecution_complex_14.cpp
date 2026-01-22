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

struct Person{
    string id;
    string firstName;
    string lastName;
    string gender;
    string birthday;
    string creationDate;
    string locationIP;
    string browserUsed;
};

vector<string> query_list;
vector<string> query1_ans_list;
vector<string> query2_ans_list;
vector<string> query3_ans_list;
vector<string> query4_ans_list;
vector<string> query5_ans_list;
vector<string> query6_ans_list;

struct Query1Result {
    string pathNodes;
    vector<string> node_list;
    float pathWeight;
};

struct Query2Result {
    string m1Id;
};

struct Query3Result {
    string m2Id;
};

struct Query4Result {
    string m2Id;
};

struct Query5Result {
    string m2Id;
};

struct Query6Result {
    string m2Id;
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

Person parsePerson(string data) {
    istringstream dataStream(data);
    Person info;
    string token;

    while (getline(dataStream, token, ',')) {
        // cout << "token: " << token << endl;
        istringstream tokenStream(token);
        string key, value;
        if (getline(getline(tokenStream, key, '='), value)) {
            if (key[0] == ' '){
                key = key.substr(1);
            }
            if (key == "birthday") info.birthday = value;
            else if (key == "browserUsed") info.browserUsed = value;
            else if (key == "creationDate") info.creationDate = value;
            else if (key == "firstName") info.firstName = value;
            else if (key == "gender") info.gender = value;
            else if (key == "id") info.id = value;
            else if (key == "lastName") info.lastName = value;
            else if (key == "locationIP") info.locationIP = value;
        }
    }

    return info;
}

Query1Result parseQuery1Results(string data) {
    Query1Result q1;
    // cout << "data:" << data << endl;
    vector<string> sub_data;
    vector<string> tlist;
    stringSplit(data, "Node(", sub_data);
    string pathId = "[";
    for (int i = 1; i < sub_data.size(); i++) {
        // cout << sub_data[i] << endl;
        Person p = parsePerson(sub_data[i]);
        string now_id = p.id;
        tlist.push_back(now_id);
        if (i < sub_data.size() - 1) {
            pathId = pathId + now_id + ", ";
        }
        else {
            pathId = pathId + now_id + "]";
        }
    }
    // cout << "pathId: " << pathId << endl;
    q1.pathNodes = pathId;
    q1.node_list = tlist;
    return q1;
}

Query2Result parseQuery2Results(string data) {
    Query2Result q2;
    // cout << "data:" << data << endl;
    int pos = data.find(':');
    string now_id = data.substr(pos + 2);
    // cout << "now_id: " << now_id << endl;
    q2.m1Id = now_id;
    return q2;
}

Query3Result parseQuery3Results(string data) {
    Query3Result q3;
    // cout << "data:" << data << endl;
    int pos = data.find(':');
    string now_id = data.substr(pos + 2);
    // cout << "now_id: " << now_id << endl;
    q3.m2Id = now_id;
    return q3;
}

Query4Result parseQuery4Results(string data) {
    Query4Result q4;
    // cout << "data:" << data << endl;
    int pos = data.find(':');
    string now_id = data.substr(pos + 2);
    // cout << "now_id: " << now_id << endl;
    q4.m2Id = now_id;
    return q4;
}

Query5Result parseQuery5Results(string data) {
    Query5Result q5;
    // cout << "data:" << data << endl;
    int pos = data.find(':');
    string now_id = data.substr(pos + 2);
    // cout << "now_id: " << now_id << endl;
    q5.m2Id = now_id;
    return q5;
}

Query6Result parseQuery6Results(string data) {
    Query6Result q6;
    // cout << "data:" << data << endl;
    int pos = data.find(':');
    string now_id = data.substr(pos + 2);
    // cout << "now_id: " << now_id << endl;
    q6.m2Id = now_id;
    return q6;
}

void print(Query1Result q) {
    cout << "---------------------------------------" << endl;
    cout << "'pathNodes': " << q.pathNodes << ", 'pathWeight': " << q.pathWeight << endl;
}

int main(int argc, char** argv) {
    auto start = std::chrono::high_resolution_clock::now();

    // vector<string> queries = {"MATCH (c:Comment)\n RETURN c\n LIMIT 2"};
    vector<string> servers = {"http://122.9.162.58:7474/", "http://116.63.178.28:7474/", "http://122.9.132.101:7474/", "http://139.9.233.77:7474/", "http://122.9.144.74:7474/", "http://116.63.188.96:7474/", "http://139.9.246.15:7474/", "http://116.63.183.28:7474/"};
    // vector<string> servers = {"http://122.9.162.58:7474/", "http://116.63.178.28:7474/"};
    // vector<string> servers = {"http://122.9.162.58:7474/", "http://116.63.178.28:7474/", "http://122.9.132.101:7474/", "http://139.9.233.77:7474/"};
    // vector<string> servers = {"http://122.9.162.58:7474/", "http://116.63.178.28:7474/", "http://122.9.132.101:7474/", "http://139.9.233.77:7474/", "http://122.9.144.74:7474/", "http://116.63.188.96:7474/"};


    string file_path = "./queries/interactive-complex-14.txt";
    string neo4jResult;
    string mid = " ";
    string nlist = " ";
    int mid_length = 0;
    int nlist_length = 0;
    vector<string> v_nlist;
    vector<Query1Result> query1Results;
    vector<Query2Result> query2Results;
    vector<Query3Result> query3Results;
    vector<Query4Result> query4Results;
    vector<Query5Result> query5Results;
    vector<Query6Result> query6Results;

    getQueryList(file_path);

    MPI_Init(&argc, &argv);

    int world_rank, world_size;
    // world_rank = 9;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    ofstream fout;
    string outfile_name = "file_time_" + to_string(world_rank);
    fout.open(outfile_name);

    float totalPathWeight = 0.0;

    if (world_rank == 0) {
        auto now = chrono::system_clock::now();
        auto duration_since_epoch = now.time_since_epoch();
        auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
        auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
        time_t now_time_t = chrono::system_clock::to_time_t(now);
        tm* now_tm = localtime(&now_time_t);
        fout << 0 << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
    }

    for (int i = 0; i < 1; i++) {
        // cout << "当前节点: " << world_rank << endl;
        // cout << "now_query: " << query_list[i] << endl;
        if (i == 0) {
            vector<string> all_result;
            if (world_rank == 0) {
                // 主节点: 收集来自其d他节点的结果
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
                    // cout << "从节点 " << (j + 1) << " 收到的结果: " << results[j] << " " << results[j].length() << endl;
                    if (results[j].length() == 2) {
                        continue;
                    }
                    else {
                        vector<string> now_ans = getAnsList(results[j]);
                        for (int k = 0; k < now_ans.size(); k++) {
                            query1_ans_list.push_back(now_ans[k]);
                        }
                    }
                }

                // cout << "query1_ans_list: " << query1_ans_list.size() << endl;
                
                for (int j = 0; j < query1_ans_list.size(); j++) {
                    Query1Result q1;
                    q1 = parseQuery1Results(query1_ans_list[j]);
                    q1.pathWeight = 0.0;
                    query1Results.push_back(q1);
                    nlist = q1.pathNodes;
                }
                nlist = nlist.substr(1);
                nlist = nlist.substr(0, nlist.length() - 1);
                // cout << "nlist: " << nlist << endl;
                // cout << query1Results[0].node_list.size();
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

            nlist_length = nlist.size();
            // cout << "节点0的nlist_length: " << nlist_length << endl;
            MPI_Bcast(&nlist_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            nlist.resize(nlist_length);
            MPI_Bcast(&nlist[0], nlist_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            // cout << "节点 " << world_rank << " 到达i0同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i0同步墙" << endl;
        }
        for (int T = 0; T < 4; T++) {
            if (T == 0) {
                MPI_Bcast(&nlist_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
                // cout << "nlist_length: " << nlist_length << endl;
                if (world_rank != 0) {
                    // cout << "这不是0号节点" << endl;
                    // 调整字符串大小以适应接收的数据
                    nlist.resize(nlist_length);
                }
                MPI_Bcast(&nlist[0], nlist_length, MPI_CHAR, 0, MPI_COMM_WORLD);

                // cout << world_rank << " " << nlist << endl;

                vector<string> tlist;
                stringSplit(nlist, ", ", tlist);
                for (int a = 0; a < tlist.size(); a++) {
                    v_nlist.push_back(tlist[a]);
                }
            }
            query2_ans_list.clear();
            query3_ans_list.clear();
            query4_ans_list.clear();
            query5_ans_list.clear();
            query6_ans_list.clear();
            query2Results.clear();
            query3Results.clear();
            query4Results.clear();
            query5Results.clear();
            query6Results.clear();
            for (int t = 1; t < query_list.size(); t++) {
                // cout << "t: " << t << endl;
                if (t == 1) {
                    int pos = query_list[t].find('\n');
                    string str1 = query_list[t].substr(0, pos + 1);
                    string str2 = query_list[t].substr(pos + 1);

                    if (world_rank != 0) {
                        string new_query = str1 + "WHERE pA.id = " + v_nlist[T] + "\n" + str2;

                        auto now = chrono::system_clock::now();
                        auto duration_since_epoch = now.time_since_epoch();
                        auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                        auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                        time_t now_time_t = chrono::system_clock::to_time_t(now);
                        tm* now_tm = localtime(&now_time_t);
                        fout << t << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                        string result = connect2Neo4j(new_query, servers[world_rank - 1]);
                        // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
                        MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
                    }
                    else {
                        string new_query = str1 + "WHERE pA.id = " + v_nlist[T] + "\n" + str2;
                        // cout << "节点 " << world_rank << endl;
                        // cout << "new_query: " << new_query << endl;
                        // cout << "tid: " << tid << endl;

                        vector<string> results(world_size - 1);
                        // vector<string> results_1(8);
                        for (int j = 1; j < world_size; ++j) {
                            char buffer[660000];
                            MPI_Recv(buffer, 660000, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                            results[j - 1] = string(buffer);

                            auto now = chrono::system_clock::now();
                            auto duration_since_epoch = now.time_since_epoch();
                            auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                            auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                            time_t now_time_t = chrono::system_clock::to_time_t(now);
                            tm* now_tm = localtime(&now_time_t);
                            fout << t << " " << j << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
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
                                    query2_ans_list.push_back(now_ans[k]);
                                }
                            }
                        }

                        // cout << "query2_ans_list: " << query2_ans_list.size() << endl;

                        mid = "WHERE m1.id IN [";
                        for (int j = 0; j < query2_ans_list.size(); j++) {
                            Query2Result q2;
                            q2 = parseQuery2Results(query2_ans_list[j]);
                            query2Results.push_back(q2);
                            // cout << "q2.m1Id: " << q2.m1Id << endl;
                            if (j == 0) {
                                mid = mid + q2.m1Id;
                            }
                            else {
                                mid = mid + ", " + q2.m1Id;
                            }
                        }
                        mid = mid + "]\n";
                        // cout << "mid: " << mid << endl;
                    }

                    mid_length = mid.size();
                    // cout << "节点0的mid_length: " << mid_length << endl;
                    MPI_Bcast(&mid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
                    mid.resize(mid_length);
                    MPI_Bcast(&mid[0], mid_length, MPI_CHAR, 0, MPI_COMM_WORLD);
                    // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
                    MPI_Barrier(MPI_COMM_WORLD);
                    // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
                }
                if (t == 2) {
                    int pos = query_list[t].find('\n');
                    string str1 = query_list[t].substr(0, pos + 1);
                    string str2 = query_list[t].substr(pos + 1);

                    MPI_Bcast(&mid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
                    // cout << "mid_length: " << mid_length << endl;
                    if (world_rank != 0) {
                        // cout << "这不是0号节点" << endl;
                        // 调整字符串大小以适应接收的数据
                        mid.resize(mid_length);
                    }
                    MPI_Bcast(&mid[0], mid_length, MPI_CHAR, 0, MPI_COMM_WORLD);

                    if (world_rank != 0) {
                        string new_query = str1 + "WHERE pB.id = " + v_nlist[T + 1] + "\n" + str2;

                        auto now = chrono::system_clock::now();
                        auto duration_since_epoch = now.time_since_epoch();
                        auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                        auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                        time_t now_time_t = chrono::system_clock::to_time_t(now);
                        tm* now_tm = localtime(&now_time_t);
                        fout << t << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                        string result = connect2Neo4j(new_query, servers[world_rank - 1]);
                        // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
                        MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
                    }
                    else {
                        string new_query = str1 + "WHERE pB.id = " + v_nlist[T + 1] + "\n" + str2;

                        // cout << "new_query: " << new_query << endl;
                        // cout << "v_nlist: " << v_nlist[T + 1] << endl;

                        vector<string> results(world_size - 1);
                        for (int j = 1; j < world_size; ++j) {
                            char buffer[840000];
                            MPI_Recv(buffer, 840000, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                            results[j - 1] = string(buffer);

                            auto now = chrono::system_clock::now();
                            auto duration_since_epoch = now.time_since_epoch();
                            auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                            auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                            time_t now_time_t = chrono::system_clock::to_time_t(now);
                            tm* now_tm = localtime(&now_time_t);
                            fout << t << " " << j << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
                        }
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
                    }

                    // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
                    MPI_Barrier(MPI_COMM_WORLD);
                    // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
                }
                if (t == 3) {
                    int pos = query_list[t].find('\n');
                    string str1 = query_list[t].substr(0, pos + 1);
                    string str2 = query_list[t].substr(pos + 1);

                    if (world_rank != 0) {
                        string new_query = str1 + "WHERE pB.id = " + v_nlist[T + 1] + "\n" + str2;

                        auto now = chrono::system_clock::now();
                        auto duration_since_epoch = now.time_since_epoch();
                        auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                        auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                        time_t now_time_t = chrono::system_clock::to_time_t(now);
                        tm* now_tm = localtime(&now_time_t);
                        fout << t << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                        string result = connect2Neo4j(new_query, servers[world_rank - 1]);
                        // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
                        MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
                    }
                    else {
                        string new_query = str1 + "WHERE pB.id = " + v_nlist[T + 1] + "\n" + str2;

                        // cout << "new_query: " << new_query << endl;

                        vector<string> results(world_size - 1);
                        for (int j = 1; j < world_size; ++j) {
                            char buffer[145318];
                            MPI_Recv(buffer, 145318, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                            results[j - 1] = string(buffer);

                            auto now = chrono::system_clock::now();
                            auto duration_since_epoch = now.time_since_epoch();
                            auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                            auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                            time_t now_time_t = chrono::system_clock::to_time_t(now);
                            tm* now_tm = localtime(&now_time_t);
                            fout << t << " " << j << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
                        }
                        for (int j = 0; j < results.size(); ++j) {
                            // cout << "从节点 " << (j + 1) << " 收到的结果: " << results[j] << " " << results[j].length() << endl;
                            if (results[j].length() == 2) {
                                continue;
                            }
                            else {
                                vector<string> now_ans = getAnsList(results[j]);
                                for (int k = 0; k < now_ans.size(); k++) {
                                    query4_ans_list.push_back(now_ans[k]);
                                }
                            }
                        }

                        // cout << "query4_ans_list: " << query4_ans_list.size() << endl;

                        for (int j = 0; j < query4_ans_list.size(); j++) {
                            Query4Result q4;
                            q4 = parseQuery4Results(query4_ans_list[j]);
                            query4Results.push_back(q4);
                        }
                    }
                    
                    // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
                    MPI_Barrier(MPI_COMM_WORLD);
                    // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
                }
                if (t == 4) {
                    int pos = query_list[t].find('\n');
                    string str1 = query_list[t].substr(0, pos + 1);
                    string str2 = query_list[t].substr(pos + 1);

                    if (world_rank != 0) {
                        string new_query = str1 + mid + str2;

                        auto now = chrono::system_clock::now();
                        auto duration_since_epoch = now.time_since_epoch();
                        auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                        auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                        time_t now_time_t = chrono::system_clock::to_time_t(now);
                        tm* now_tm = localtime(&now_time_t);
                        fout << t << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                        string result = connect2Neo4j(new_query, servers[world_rank - 1]);
                        // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
                        MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
                    }
                    else {
                        string new_query = str1 + mid + str2;

                        vector<string> results(world_size - 1);
                        for (int j = 1; j < world_size; ++j) {
                            char buffer[145318];
                            MPI_Recv(buffer, 145318, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                            results[j - 1] = string(buffer);

                            auto now = chrono::system_clock::now();
                            auto duration_since_epoch = now.time_since_epoch();
                            auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                            auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                            time_t now_time_t = chrono::system_clock::to_time_t(now);
                            tm* now_tm = localtime(&now_time_t);
                            fout << t << " " << j << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
                        }
                        for (int j = 0; j < results.size(); ++j) {
                            // cout << "从节点 " << (j + 1) << " 收到的结果: " << results[j] << " " << results[j].length() << endl;
                            if (results[j].length() == 2) {
                                continue;
                            }
                            else {
                                vector<string> now_ans = getAnsList(results[j]);
                                for (int k = 0; k < now_ans.size(); k++) {
                                    query5_ans_list.push_back(now_ans[k]);
                                }
                            }
                        }

                        // cout << "query5_ans_list: " << query5_ans_list.size() << endl;

                        for (int j = 0; j < query5_ans_list.size(); j++) {
                            Query5Result q5;
                            q5 = parseQuery5Results(query5_ans_list[j]);
                            query5Results.push_back(q5);
                        }
                    }
                    
                    // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
                    MPI_Barrier(MPI_COMM_WORLD);
                    // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
                }
                if (t == 5) {
                    int pos = query_list[t].find('\n');
                    string str1 = query_list[t].substr(0, pos + 1);
                    string str2 = query_list[t].substr(pos + 1);

                    if (world_rank != 0) {
                        string new_query = str1 + mid + str2;

                        auto now = chrono::system_clock::now();
                        auto duration_since_epoch = now.time_since_epoch();
                        auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                        auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                        time_t now_time_t = chrono::system_clock::to_time_t(now);
                        tm* now_tm = localtime(&now_time_t);
                        fout << t << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
                        
                        string result = connect2Neo4j(new_query, servers[world_rank - 1]);
                        // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
                        MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
                    }
                    else {
                        string new_query = str1 + mid + str2;

                        vector<string> results(world_size - 1);
                        for (int j = 1; j < world_size; ++j) {
                            char buffer[145318];
                            MPI_Recv(buffer, 145318, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                            results[j - 1] = string(buffer);

                            auto now = chrono::system_clock::now();
                            auto duration_since_epoch = now.time_since_epoch();
                            auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                            auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                            time_t now_time_t = chrono::system_clock::to_time_t(now);
                            tm* now_tm = localtime(&now_time_t);
                            fout << t << " " << j << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
                        }
                        for (int j = 0; j < results.size(); ++j) {
                            // cout << "从节点 " << (j + 1) << " 收到的结果: " << results[j] << " " << results[j].length() << endl;
                            if (results[j].length() == 2) {
                                continue;
                            }
                            else {
                                vector<string> now_ans = getAnsList(results[j]);
                                for (int k = 0; k < now_ans.size(); k++) {
                                    query6_ans_list.push_back(now_ans[k]);
                                }
                            }
                        }

                        // cout << "query6_ans_list: " << query6_ans_list.size() << endl;

                        for (int j = 0; j < query6_ans_list.size(); j++) {
                            Query6Result q6;
                            q6 = parseQuery6Results(query6_ans_list[j]);
                            query6Results.push_back(q6);
                        }

                        int cnt = 0;
                        for (int j = 0; j < query5Results.size(); j++) {
                            for (int k = 0; k < query3Results.size(); k++) {
                                if (query5Results[j].m2Id == query3Results[k].m2Id) {
                                    cnt++;
                                }
                            }
                        }
                        // cout << "cnt: " << cnt << endl;
                        query1Results[0].pathWeight += cnt;

                        cnt = 0;
                        for (int j = 0; j < query6Results.size(); j++) {
                            for (int k = 0; k < query4Results.size(); k++) {
                                if (query6Results[j].m2Id == query4Results[k].m2Id) {
                                    cnt++;
                                }
                            }
                        }
                        // cout << "cnt: " << cnt << endl;
                        query1Results[0].pathWeight += cnt * 0.5;
                    }
                    
                    // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
                    MPI_Barrier(MPI_COMM_WORLD);
                    // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
                }
            }
        }
        if (world_rank == 0) {
            auto now = chrono::system_clock::now();
            auto duration_since_epoch = now.time_since_epoch();
            auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
            auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
            time_t now_time_t = chrono::system_clock::to_time_t(now);
            tm* now_tm = localtime(&now_time_t);
            fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

            cout << "到哪了" << endl;

            for (int j = 0; j < query1Results.size(); j++) {
                print(query1Results[j]);
            }
        }
    }

    MPI_Finalize();

    auto end = std::chrono::high_resolution_clock::now();

    // 计算所经过的时间
    std::chrono::duration<double> elapsed = end - start;

    // 输出结果
    std::cout << "Elapsed time: " << elapsed.count() << " seconds." << std::endl;

    return 0;
}