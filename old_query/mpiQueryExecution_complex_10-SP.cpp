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

struct Place{
    string id;
    string name;
    string url;
    string type;
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

struct Tag{
    string id;
    string name;
    string url;
};

vector<string> query_list;
vector<string> query1_ans_list;
vector<string> query2_ans_list;
vector<string> query3_ans_list;
vector<string> query4_ans_list;
vector<string> query5_ans_list;
vector<string> query6_ans_list;

struct Query1Result {
    Person friendPerson;
};

struct Query2Result {
    Person friendPerson;
    Place friendCity;
};

struct Query3Result {
    Tag likeTag;
};

struct Query4Result {
    Person friendPerson;
    Post friendPost;
};

struct Query5Result {
    Post friendPost;
    Tag postTag;
};

struct Query6Result {
    string personId;
    string personFirstName;
    string personLastName;
    string personGender;
    int postCount;
    string personCityName;
    int commonInterestScore;
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

Place parsePlace(string data) {
    istringstream dataStream(data);
    Place info;
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
            else if (key == "name") info.name = value;
            else if (key == "url") info.url = value;
            else if (key == "type") info.type = value;
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

Tag parseTag(string data) {
    istringstream dataStream(data);
    Tag info;
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
            else if (key == "name") info.name = value;
            else if (key == "url") info.url = value;
        }
    }

    return info;
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

Query1Result parseQuery1Results(string data) {
    Query1Result q1;
    // cout << "data:" << data << endl;
    Person p = parsePerson(data);
    q1.friendPerson = p;
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
            Person p = parsePerson(sub_data[i]);
            q2.friendPerson = p;
        }
        if (i == 1) {
            Place p = parsePlace(sub_data[i]);
            q2.friendCity = p;
        }
    }
    return q2;
}

Query3Result parseQuery3Results(string data) {
    Query3Result q3;
    // cout << "data:" << data << endl;
    Tag t = parseTag(data);
    q3.likeTag = t;
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
            q4.friendPerson = p;
        }
        if (i == 1) {
            Post p = parsePost(sub_data[i]);
            q4.friendPost = p;
        }
    }
    return q4;
}

Query5Result parseQuery5Results(string data) {
    Query5Result q5;
    // cout << "data:" << data << endl;
    vector<string> sub_data;
    stringSplit(data, ", '", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        // cout << sub_data[i] << endl;
        if (i == 0) {
            Post p = parsePost(sub_data[i]);
            q5.friendPost = p;
        }
        if (i == 1) {
            Tag t = parseTag(sub_data[i]);
            q5.postTag = t;
        }
    }
    return q5;
}

Query6Result parseQuery6Results(string data) {
    Query6Result q6;
    // cout << "data:" << data << endl;
    vector<string> sub_data;
    stringSplit(data, ", '", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        // cout << sub_data[i] << endl;
        int pos = sub_data[i].find(':');
        string tdata = sub_data[i].substr(pos + 2);
        if (i == 0) {
            q6.personId = tdata;
        }
        if (i == 1) {
            q6.personFirstName = tdata;
        }
        if (i == 2) {
            q6.personLastName = tdata;
        }
        if (i == 3) {
            q6.personGender = tdata;
        }
        if (i == 4) {
            q6.postCount = stoi(tdata);
            // cout << "q6.postCount: " << q6.postCount << endl;
        }
    }
    return q6;
}

int getMonth(string tm) {
    int pos = tm.find('-');
    string ttm = tm.substr(pos + 1);
    pos = ttm.find('-');
    return stoi(ttm.substr(0, pos));
}

int getDay(string tm) {
    int pos = tm.rfind('-');
    string td = tm.substr(pos + 1);
    // cout << "td: " << td << endl;
    return stoi(td.substr(0, td.length() - 1));
}

bool cmp(Query6Result a, Query6Result b) {
    if (a.commonInterestScore != b.commonInterestScore) {
        return a.commonInterestScore > b.commonInterestScore;
    }
    if (a.personId.length() != b.personId.length()) {
        return a.personId.length() < b.personId.length();
    }
    return a.personId < b.personId;
}

void print(Query6Result q6) {
    cout << "---------------------------------------" << endl;
    cout << "'personId': " << q6.personId << ", 'personFirstName': " << q6.personFirstName << ", 'personLastName': " << q6.personLastName;
    cout << ", 'commonInterestScore': " << q6.commonInterestScore << ", 'personGender': " << q6.personGender;
    cout << ", 'personCityName': " << q6.personCityName << endl;
}

int main(int argc, char** argv) {
    auto start = std::chrono::high_resolution_clock::now();

    // vector<string> queries = {"MATCH (c:Comment)\n RETURN c\n LIMIT 2"};
    // vector<string> servers = {"http://122.9.162.58:7474/", "http://116.63.178.28:7474/", "http://122.9.132.101:7474/", "http://139.9.233.77:7474/", "http://122.9.144.74:7474/", "http://116.63.188.96:7474/", "http://139.9.246.15:7474/", "http://116.63.183.28:7474/"};
    vector<string> servers = {"http://139.9.250.196:7474/"};

    string file_path = "./queries/interactive-complex-10-SP.txt";
    string neo4jResult;
    string tid = " ";
    string pid = " ";
    int tid_length = 0;
    int pid_length = 0;
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

                tid = "WHERE friend.id IN [";
                for (int j = 0; j < query1_ans_list.size(); j++) {
                    Query1Result q1;
                    q1 = parseQuery1Results(query1_ans_list[j]);
                    int tmonth = getMonth(q1.friendPerson.birthday);
                    int tday = getDay(q1.friendPerson.birthday);
                    if (tmonth == 5 && tday >= 21) {
                        query1Results.push_back(q1);
                    }
                    if (j == 0) {
                        tid = tid + q1.friendPerson.id;
                    }
                    else {
                        tid = tid + ", " + q1.friendPerson.id;
                    }
                }
                tid = tid + "]\n";
                // cout << "tid: " << tid << endl;
                // cout << "query1Results.size(): " << query1Results.size() << endl;
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
            // cout << "节点0的cid_length: " << cid_length << endl;
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
                    char buffer[327680];
                    MPI_Recv(buffer, 327680, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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

                for (int j = 0; j < query2_ans_list.size(); j++) {
                    Query2Result q2;
                    q2 = parseQuery2Results(query2_ans_list[j]);
                    query2Results.push_back(q2);
                }
            }

            // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
        }
        if (i == 2) {
            if (world_rank != 0) {
                string new_query = query_list[i];

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
                string new_query = query_list[i];

                vector<string> results(world_size - 1);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[1465318];
                    MPI_Recv(buffer, 1465318, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    results[j - 1] = string(buffer);

                    auto now = chrono::system_clock::now();
                    auto duration_since_epoch = now.time_since_epoch();
                    auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                    auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                    time_t now_time_t = chrono::system_clock::to_time_t(now);
                    tm* now_tm = localtime(&now_time_t);
                    fout << i << " " << j << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
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
                // cout << "query3Results.size(): " << query3Results.size() << endl;
            }

            // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
        }
        if (i == 3) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

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

                vector<string> results(world_size - 1);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[1465318];
                    MPI_Recv(buffer, 1465318, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    results[j - 1] = string(buffer);

                    auto now = chrono::system_clock::now();
                    auto duration_since_epoch = now.time_since_epoch();
                    auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                    auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                    time_t now_time_t = chrono::system_clock::to_time_t(now);
                    tm* now_tm = localtime(&now_time_t);
                    fout << i << " " << j << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
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

                pid = "WHERE post.id IN [";
                int flag = 0;
                for (int j = 0; j < query4_ans_list.size(); j++) {
                    Query4Result q4;
                    q4 = parseQuery4Results(query4_ans_list[j]);

                    if (q4.friendPerson.id.length() < 1) {
                        continue;
                    }
                    query4Results.push_back(q4);
                    // cout << flag << " " << pid << endl;
                    if (flag == 0) {
                        pid = pid + q4.friendPost.id;
                        flag = 1;
                    }
                    else {
                        pid = pid + ", " + q4.friendPost.id;
                    }
                }
                pid = pid + "]\n";
                // cout << "pid: " << pid << endl;
                // cout << "query4Results.size(): " << query4Results.size() << endl;
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
        if (i == 4) {
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

                vector<string> results(world_size - 1);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[1465318];
                    MPI_Recv(buffer, 1465318, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    results[j - 1] = string(buffer);

                    auto now = chrono::system_clock::now();
                    auto duration_since_epoch = now.time_since_epoch();
                    auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                    auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                    time_t now_time_t = chrono::system_clock::to_time_t(now);
                    tm* now_tm = localtime(&now_time_t);
                    fout << i << " " << j << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
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
                // cout << "query5Results.size(): " << query5Results.size() << endl;
            }

            // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
        }
        if (i == 5) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

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

                vector<string> results(world_size - 1);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[1465318];
                    MPI_Recv(buffer, 1465318, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    results[j - 1] = string(buffer);

                    auto now = chrono::system_clock::now();
                    auto duration_since_epoch = now.time_since_epoch();
                    auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                    auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                    time_t now_time_t = chrono::system_clock::to_time_t(now);
                    tm* now_tm = localtime(&now_time_t);
                    fout << i << " " << j << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
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
                    for (int k = 0; k < query2Results.size(); k++) {
                        if (q6.personId == query2Results[k].friendPerson.id) {
                            q6.personCityName = query2Results[k].friendCity.name;
                            break;
                        }
                    }
                    int commonPostCount = 0;
                    int flag = 0;
                    for (int k = 0; k < query4Results.size(); k++) {
                        if (q6.personId == query4Results[k].friendPerson.id) {
                            flag = 0;
                            for (int a = 0; a < query5Results.size(); a++) {
                                if (query4Results[k].friendPost.id == query5Results[a].friendPost.id) {
                                    for (int b = 0; b < query3Results.size(); b++) {
                                        if (query5Results[a].postTag.id == query3Results[b].likeTag.id) {
                                            commonPostCount++;
                                            flag = 1;
                                            break;
                                        }
                                    }
                                }
                                if (flag == 1) {
                                    break;
                                }
                            }
                        }
                    }
                    q6.commonInterestScore = commonPostCount - (q6.postCount - commonPostCount);
                    query6Results.push_back(q6);
                }
                // cout << "query6Results.size(): " << query6Results.size() << endl;

                sort(query6Results.begin(), query6Results.end(), cmp);

                auto now = chrono::system_clock::now();
                auto duration_since_epoch = now.time_since_epoch();
                auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                time_t now_time_t = chrono::system_clock::to_time_t(now);
                tm* now_tm = localtime(&now_time_t);
                fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                int cnt = 0;
                for (int i = 0; i < query6Results.size(); i++) {
                    if (cnt > 9) {
                        break;
                    }
                    if (query6Results[i].personId == "None") {
                        continue;
                    }
                    else{
                        print(query6Results[i]);
                        cnt++;
                    }
                }
            }

            // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
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