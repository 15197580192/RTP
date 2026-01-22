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
vector<string> query4_ans_list;

struct Query1Result {
    Person otherPerson;
};

struct Query2Result {
    Forum friendForum;
    string membership;
};

struct Query3Result {
    Post friendPost;
    Forum friendForum;
    int postCount;
};

struct Query4Result {
    Person otherPerson2;
    Post friendPost;
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
    Person p = parsePerson(data);
    q1.otherPerson = p;
    return q1;
}

Query2Result parseQuery2Results(string data) {
    Query2Result q2;
    // cout << "data:" << data << endl;
    vector<string> sub_data;
    stringSplit(data, ", '", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        // cout << sub_data[i] << endl;
        if (i == 0) {
            Forum f = parseForum(sub_data[i]);
            q2.friendForum = f;
        }
        if (i == 1) {
            // cout << sub_data[i] << endl;
            int pos = sub_data[i].find("joinDate");
            string td = sub_data[i].substr(pos + 9);
            // cout << "td: " << td << endl;
            td = td.substr(0, td.length() - 1);
            q2.membership = td;
        }
    }
    return q2;
}

Query3Result parseQuery3Results(string data) {
    Query3Result q3;
    // cout << "data:" << data << endl;
    vector<string> sub_data;
    stringSplit(data, ", '", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        // cout << sub_data[i] << endl;
        if (i == 0) {
            Post p = parsePost(sub_data[i]);
            q3.friendPost = p;
        }
        if (i == 1) {
            Forum f = parseForum(sub_data[i]);
            q3.friendForum = f;
        }
    }
    return q3;
}

Query4Result parseQuery4Results(string data) {
    Query4Result q4;
    // cout << "data:" << data << endl;
    vector<string> sub_data;
    stringSplit(data, ", '", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        // cout << sub_data[i] << endl;
        if (i == 0) {
            Person p = parsePerson(sub_data[i]);
            q4.otherPerson2 = p;
        }
        if (i == 1) {
            Post p = parsePost(sub_data[i]);
            q4.friendPost = p;
        }
    }
    return q4;
}

int64_t convertToTimestamp(string timeStr) {
    tm tm = {};
    istringstream ss(timeStr);
    ss >> get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    // 手动处理毫秒和时区
    // 注意：这个简化的例子假设时间总是以 +0000 UTC 结尾
    ss.ignore(1); // 忽略小数点
    int milliseconds;
    ss >> milliseconds; // 读取毫秒
    auto timeT = mktime(&tm); // 转换为time_t（不包含时区和毫秒）

    // 使用chrono将time_t转换为milliseconds
    auto duration = std::chrono::system_clock::from_time_t(timeT).time_since_epoch();
    // 添加毫秒
    duration += std::chrono::milliseconds(milliseconds);

    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

bool cmp(Query3Result a, Query3Result b) {
    if (a.friendForum.id.length() != b.friendForum.id.length()) {
        return a.friendForum.id.length() < b.friendForum.id.length();
    }
    return a.friendForum.id < b.friendForum.id;
}

bool cmp1(Query3Result a, Query3Result b) {
    if (a.postCount != b.postCount) {
        return a.postCount > b.postCount;
    }
    if (a.friendForum.id.length() != b.friendForum.id.length()) {
        return a.friendForum.id.length() < b.friendForum.id.length();
    }
    return a.friendForum.id < b.friendForum.id;
}

void print(Query3Result q3) {
    cout << "---------------------------------------" << endl;
    cout << "'forumName': " << q3.friendForum.title << ", 'postCount': " << q3.postCount << endl;
    cout << q3.friendForum.id << endl;
}

int main(int argc, char** argv) {
    auto start = std::chrono::high_resolution_clock::now();

    // vector<string> queries = {"MATCH (c:Comment)\n RETURN c\n LIMIT 2"};
    // vector<string> servers = {"http://122.9.162.58:7474/", "http://116.63.178.28:7474/", "http://122.9.132.101:7474/", "http://139.9.233.77:7474/", "http://122.9.144.74:7474/", "http://116.63.188.96:7474/", "http://139.9.246.15:7474/", "http://116.63.183.28:7474/"};
    vector<string> servers = {"http://139.9.250.196:7474/"};

    string file_path = "./queries/interactive-complex-5-SP.txt";
    string neo4jResult;
    string tid = " ";
    string fid = " ";
    string pid = " ";
    int tid_length = 0;
    int fid_length = 0;
    int pid_length = 0;
    vector<Query1Result> query1Results;
    vector<Query2Result> query2Results;
    vector<Query3Result> query3Results;
    vector<Query4Result> query4Results;

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
            vector<string> all_result;
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

                tid = "WHERE otherPerson.id IN [";

                for (int j = 0; j < query1_ans_list.size(); j++) {
                    Query1Result q1;
                    q1 = parseQuery1Results(query1_ans_list[j]);
                    query1Results.push_back(q1);
                    if (j == 0) {
                        tid = tid + q1.otherPerson.id;
                    }
                    else {
                        tid = tid + ", " + q1.otherPerson.id;
                    }
                }
                tid = tid + "]\n";
                // cout << "tid: " << tid << endl;
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

            tid_length = tid.size();
            // cout << "节点0的tid_length: " << tid_length << endl;
            MPI_Bcast(&tid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            tid.resize(tid_length);
            MPI_Bcast(&tid[0], tid_length, MPI_CHAR, 0, MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 到达i0同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i0同步墙" << endl;
        }
        if (i == 1) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            MPI_Bcast(&tid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            // cout << "tid_length: " << tid_length << endl;
            if (world_rank != 0) {
                // cout << "这不是0号节点" << endl;
                // 调整字符串大小以适应接收的数据
                tid.resize(tid_length);
            }
            MPI_Bcast(&tid[0], tid_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            if (world_rank != 0) {
                string new_query = str1 + tid + str2;

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
                string new_query = str1 + tid + str2;
                // cout << "节点 " << world_rank << endl;
                // cout << "new_query: " << new_query << endl;
                // cout << "tid: " << tid << endl;

                vector<string> results(world_size - 1);
                // vector<string> results_1(8);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[8000000];
                    MPI_Recv(buffer, 8000000, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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
                            query2_ans_list.push_back(now_ans[k]);
                        }
                    }
                }

                // cout << "query2_ans_list: " << query2_ans_list.size() << endl;

                fid = "WHERE forum.id IN [";

                int flag = 0;
                for (int j = 0; j < query2_ans_list.size(); j++) {
                    Query2Result q2;
                    q2 = parseQuery2Results(query2_ans_list[j]);
                    string ttime = q2.membership.substr(1);
                    ttime = ttime.substr(0, ttime.length() - 1);
                    // cout << "ttime: " << ttime << endl;
                    int64_t timestamp = convertToTimestamp(ttime);
                    if (timestamp > 1347403577314) {
                        query2Results.push_back(q2);
                        if (flag == 0) {
                            fid = fid + q2.friendForum.id;
                            flag = 1;
                        }
                        else {
                            fid = fid + ", " + q2.friendForum.id;
                        }
                    }
                }
                fid = fid + "]\n";
                // cout << "fid: " << fid << endl;
                // cout << "q2.size(): " << query2Results.size() << endl;
            }

            fid_length = fid.size();
            // cout << "节点0的fid_length: " << fid_length << endl;
            MPI_Bcast(&fid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            fid.resize(fid_length);
            MPI_Bcast(&fid[0], fid_length, MPI_CHAR, 0, MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
        }
        if (i == 2) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            MPI_Bcast(&fid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            // cout << "fid_length: " << fid_length << endl;
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
                    char buffer[100000];
                    MPI_Recv(buffer, 100000, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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

                cout << "query3_ans_list: " << query3_ans_list.size() << endl;

                pid = "WHERE post.id IN [";

                bool flag = false;
                for (int j = 0; j < query3_ans_list.size(); j++) {
                    Query3Result q3;
                    q3 = parseQuery3Results(query3_ans_list[j]);
                    // cout << "q3: " << query3_ans_list[j] << endl;
                    // cout << "q3.friendPost.id: " << q3.friendPost.id << endl;
                    // cout << "q3.length(): " << q3.friendPost.id.length() << endl;
                    if (q3.friendPost.id.length() < 1) {
                        continue;
                    }
                    query3Results.push_back(q3);
                    if (flag == false) {
                        pid = pid + q3.friendPost.id;
                        flag = true;
                    }
                    else {
                        pid = pid + ", " + q3.friendPost.id;
                    }
                }
                pid = pid + "]\n";
                // cout << "pid: " << pid << endl;
            }

            pid_length = pid.size();
            // cout << "节点0的pid_length: " << pid_length << endl;
            MPI_Bcast(&pid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            pid.resize(pid_length);
            MPI_Bcast(&pid[0], pid_length, MPI_CHAR, 0, MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
        }
        if (i == 3) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            MPI_Bcast(&pid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            // cout << "pid_length: " << pid_length << endl;
            if (world_rank != 0) {
                // cout << "这不是0号节点" << endl;
                // 调整字符串大小以适应接收的数据
                pid.resize(pid_length);
            }
            MPI_Bcast(&pid[0], pid_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            if (world_rank != 0) {
                string new_query = str1 + pid + str2;

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
                string new_query = str1 + pid + str2;
                // cout << "节点 " << world_rank << endl;
                // cout << "new_query: " << new_query << endl;
                // cout << "pid: " << pid << endl;

                vector<string> results(world_size - 1);
                // vector<string> results_1(8);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[131072];
                    MPI_Recv(buffer, 131072, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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
                            query4_ans_list.push_back(now_ans[k]);
                        }
                    }
                }

                cout << "query4_ans_list: " << query4_ans_list.size() << endl;

                for (int j = 0; j < query4_ans_list.size(); j++) {
                    Query4Result q4;
                    q4 = parseQuery4Results(query4_ans_list[j]);
                    for (int k = 0; k < query1Results.size(); k++) {
                        if (q4.otherPerson2.id == query1Results[k].otherPerson.id) {
                            query4Results.push_back(q4);
                            break;
                        }
                    }
                }

                cout << "q3rs" << query3Results.size() << endl;

                sort(query3Results.begin(), query3Results.end(), cmp);

                for (int i = 0; i < query3Results.size(); i++) {
                    query3Results[i].postCount = 0;
                    for (int j = 0; j < query4Results.size(); j++) {
                        if (query3Results[i].friendPost.id == query4Results[j].friendPost.id) {
                            query3Results[i].postCount++;
                            break;
                        }
                    }
                }

                string now_id = query3Results[0].friendForum.id;
                int now_cnt = query3Results[0].postCount;
                for (int i = 1; i < query3Results.size(); i++) {
                    if (query3Results[i].friendForum.id == now_id) {
                        now_cnt += query3Results[i].postCount;
                        query3Results[i].postCount = -1;
                    }
                    else {
                        query3Results[i - 1].postCount = now_cnt;
                        now_id = query3Results[i].friendForum.id;
                        now_cnt = query3Results[i].postCount;
                    }
                }
                query3Results[query3Results.size() - 1].postCount = now_cnt;

                sort(query3Results.begin(), query3Results.end(), cmp1);
                cout << query3Results.size() << endl;

                auto now = chrono::system_clock::now();
                auto duration_since_epoch = now.time_since_epoch();
                auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                time_t now_time_t = chrono::system_clock::to_time_t(now);
                tm* now_tm = localtime(&now_time_t);
                fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                for (int i = 0; i < query3Results.size(); i++) {
                    if (i > 19) {
                        break;
                    }
                    print(query3Results[i]);
                }
            }

            // cout << "节点 " << world_rank << " 到达i3同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i3同步墙" << endl;
        }
        // cout << "i: " << i << endl;
        // cout << "循环结束的当前节点: " << world_rank << endl;
    }

    MPI_Finalize();

    auto end = std::chrono::high_resolution_clock::now();

    // 计算所经过的时间
    std::chrono::duration<double> elapsed = end - start;

    // 输出结果
    std::cout << "Elapsed time: " << elapsed.count() << " seconds." << std::endl;

    return 0;
}