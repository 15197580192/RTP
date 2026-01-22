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

struct Comment{
    string id;
    string creationDate;
    string locationIP;
    string browserUsed;
    string content;
    string length;
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

vector<string> query_list;
vector<string> query1_ans_list;
vector<string> query2_ans_list;
vector<string> query3_ans_list;
vector<string> query4_ans_list;
vector<string> query5_ans_list;
vector<string> query6_ans_list;
vector<string> query7_ans_list;

struct Query1Result {
    Place countryX;
    Place countryY;
};

struct Query2Result {
    Place city;
};

struct Query3Result {
    Person friendPerson;
};

struct Query4Result {
    Person friendPerson;
    Place city;
};

struct Query5Result {
    Post message;
    int messageX;
    int messageY;
};

struct Query6Result {
    Comment message;
    int messageX;
    int messageY;
};

struct Query7Result {
    string friendId;
    string friendFirstName;
    string friendLastName;
    int xCount;
    int yCount;
    int xyCount;
    vector<string> message_list;
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

Comment parseComment(string data) {
    istringstream dataStream(data);
    Comment info;
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
            else if (key == "creationDate") info.creationDate = value;
            else if (key == "locationIP") info.locationIP = value;
            else if (key == "browserUsed") info.browserUsed = value;
            else if (key == "content") info.content = value;
            else if (key == "length") info.length = value;
        }
    }

    return info;
}

Query1Result parseQuery1Results(string data) {
    Query1Result q1;
    // cout << "data:" << data << endl;
    vector<string> sub_data;
    stringSplit(data, ", '", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        // cout << sub_data[i] << endl;
        if (i == 0) {
            Place p = parsePlace(sub_data[i]);
            q1.countryX = p;
        }
        if (i == 1) {
            Place p = parsePlace(sub_data[i]);
            q1.countryY = p;
        }
    }
    return q1;
}

Query2Result parseQuery2Results(string data) {
    Query2Result q2;
    // cout << "data:" << data << endl;
    Place p = parsePlace(data);
    q2.city = p;
    return q2;
}

Query3Result parseQuery3Results(string data) {
    Query3Result q3;
    // cout << "data:" << data << endl;
    Person p = parsePerson(data);
    q3.friendPerson = p;
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
            Place p = parsePlace(sub_data[i]);
            q4.city = p;
        }
    }
    return q4;
}

Query5Result parseQuery5Results(string data) {
    Query5Result q5;
    vector<string> sub_data;
    stringSplit(data, ", '", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        // cout << sub_data[i] << endl;
        if (i == 0) {
            Post p = parsePost(sub_data[i]);
            q5.message = p;
        }
        if (i == 1) {
            int pos = sub_data[i].find(':');
            int mx = stoi(sub_data[i].substr(pos + 1));
            q5.messageX = mx;
        }
        if (i == 2) {
            int pos = sub_data[i].find(':');
            int my = stoi(sub_data[i].substr(pos + 1));
            q5.messageY = my;
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
        if (i == 0) {
            Comment p = parseComment(sub_data[i]);
            q6.message = p;
        }
        if (i == 1) {
            int pos = sub_data[i].find(':');
            int mx = stoi(sub_data[i].substr(pos + 1));
            q6.messageX = mx;
        }
        if (i == 2) {
            int pos = sub_data[i].find(':');
            int my = stoi(sub_data[i].substr(pos + 1));
            q6.messageY = my;
        }
    }
    return q6;
}

Query7Result parseQuery7Results(string data) {
    Query7Result q7;
    // cout << "data:" << data << endl;
    vector<string> sub_data;
    stringSplit(data, ", 'message", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        // cout << sub_data[i] << endl;
        if (i == 0) {
            Person p = parsePerson(sub_data[i]);
            q7.friendId = p.id;
            q7.friendFirstName = p.firstName;
            q7.friendLastName = p.lastName;
        }
        if (i == 1) {
            int pos = sub_data[i].find('[');
            string message_id = sub_data[i].substr(pos + 1);
            message_id = message_id.substr(0, message_id.length() - 1);
            // cout << "message_id: " << message_id << endl;
            vector<string> temp;
            stringSplit(message_id, ", ", temp);
            // cout << "temp: " << temp.size() << " " << temp[0] << endl;
            q7.message_list = temp;
        }
    }
    return q7;
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

void print(Query7Result q7) {
    cout << "---------------------------------------" << endl;
    cout << "'friendId': " << q7.friendId << ", 'friendFirstName': " << q7.friendFirstName << ", 'friendLastName': " << q7.friendLastName;
    cout << ", 'xCount': " << q7.xCount << ", 'yCount': " << q7.yCount << ", 'xyCount': " << q7.xyCount << endl;
}

bool cmp(Query3Result a, Query3Result b) {
    if (a.friendPerson.id.length() != b.friendPerson.id.length()) {
        return a.friendPerson.id.length() < b.friendPerson.id.length();
    }
    return a.friendPerson.id < b.friendPerson.id;
}

bool cmp1(Query7Result a, Query7Result b) {
    if (a.friendId.length() != b.friendId.length()) {
        return a.friendId.length() < b.friendId.length();
    }
    return a.friendId < b.friendId;
}

bool cmp2(Query7Result a, Query7Result b) {
    if (a.xyCount != b.xyCount) {
        return a.xyCount > b.xyCount;
    }
    if (a.friendId.length() != b.friendId.length()) {
        return a.friendId.length() < b.friendId.length();
    }
    return a.friendId < b.friendId;
}

int main(int argc, char** argv) {
    auto start = std::chrono::high_resolution_clock::now();

    // vector<string> queries = {"MATCH (c:Comment)\n RETURN c\n LIMIT 2"};
    vector<string> servers = {"http://122.9.162.58:7474/", "http://116.63.178.28:7474/", "http://122.9.132.101:7474/", "http://139.9.233.77:7474/", "http://122.9.144.74:7474/", "http://116.63.188.96:7474/", "http://139.9.246.15:7474/", "http://116.63.183.28:7474/"};
    // vector<string> servers = {"http://122.9.162.58:7474/", "http://116.63.178.28:7474/"};
    // vector<string> servers = {"http://122.9.162.58:7474/", "http://116.63.178.28:7474/", "http://122.9.132.101:7474/", "http://139.9.233.77:7474/"};
    // vector<string> servers = {"http://122.9.162.58:7474/", "http://116.63.178.28:7474/", "http://122.9.132.101:7474/", "http://139.9.233.77:7474/", "http://122.9.144.74:7474/", "http://116.63.188.96:7474/"};

    string file_path = "./queries/interactive-complex-3.txt";
    string neo4jResult;
    string tid = " ";
    string tid1 = " ";
    string cities_id = " ";
    string cid = " ";
    string comid = " ";
    string pid = " ";
    int tid_length = 0;
    int tid1_length = 0;
    int cities_id_length = 0;
    int cid_length = 0;
    int comid_length = 0;
    int pid_length = 0;
    vector<Query1Result> query1Results;
    vector<Query2Result> query2Results;
    vector<Query3Result> query3Results;
    vector<Query4Result> query4Results;
    vector<Query5Result> query5Results;
    vector<Query6Result> query6Results;
    vector<Query7Result> query7Results;

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
                    char buffer[8192];
                    MPI_Recv(buffer, 8192, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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

                cout << "query1_ans_list: " << query1_ans_list.size() << endl;

                cid = "WHERE NOT country.id IN [";

                for (int j = 0; j < 1; j++) {
                    Query1Result q1;
                    q1 = parseQuery1Results(query1_ans_list[j]);
                    query1Results.push_back(q1);
                    cid = cid + q1.countryX.id + ", " + q1.countryY.id;
                }
                cid = cid + "]\n";
                // cout << "cid: " << cid << endl;
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

            cid_length = cid.size();
            // cout << "节点0的cid_length: " << cid_length << endl;
            MPI_Bcast(&cid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            cid.resize(cid_length);
            MPI_Bcast(&cid[0], cid_length, MPI_CHAR, 0, MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 到达i0同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i0同步墙" << endl;

        }
        if (i == 1) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            MPI_Bcast(&cid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            // cout << "cid_length: " << cid_length << endl;
            if (world_rank != 0) {
                // cout << "这不是0号节点" << endl;
                // 调整字符串大小以适应接收的数据
                cid.resize(cid_length);
            }
            MPI_Bcast(&cid[0], cid_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            if (world_rank != 0) {
                string new_query = str1 + cid + str2;

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
                string new_query = str1 + cid + str2;
                // cout << "节点 " << world_rank << endl;
                // cout << "new_query: " << new_query << endl;
                // cout << "tid: " << tid << endl;

                vector<string> results(world_size - 1);
                // vector<string> results_1(8);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[163840];
                    MPI_Recv(buffer, 163840, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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
                    if (j == 0) {
                        cities_id = cities_id + q2.city.id;
                    }
                    else {
                        cities_id = cities_id + ", " + q2.city.id;
                    }
                }
                // cout << "cities_id: " << cities_id << endl;
            }

            cities_id_length = cities_id.size();
            // cout << "节点0的cid_length: " << cid_length << endl;
            MPI_Bcast(&cities_id_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            cities_id.resize(cities_id_length);
            MPI_Bcast(&cities_id[0], cities_id_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
        }
        if (i == 2) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            MPI_Bcast(&cities_id_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            // cout << "cities_id_length: " << cities_id_length << endl;
            if (world_rank != 0) {
                // cout << "这不是0号节点" << endl;
                // 调整字符串大小以适应接收的数据
                cities_id.resize(cities_id_length);
            }
            MPI_Bcast(&cities_id[0], cities_id_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            if (world_rank != 0) {
                auto now = chrono::system_clock::now();
                auto duration_since_epoch = now.time_since_epoch();
                auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                time_t now_time_t = chrono::system_clock::to_time_t(now);
                tm* now_tm = localtime(&now_time_t);
                fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                string result = connect2Neo4j(query_list[i], servers[world_rank - 1]);
                // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
                MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
            }
            else {
                string new_query = query_list[i];
                // cout << "节点 " << world_rank << endl;

                vector<string> results(world_size - 1);
                // vector<string> results_1(8);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[1344123];
                    MPI_Recv(buffer, 1344123, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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

                tid = "WHERE friend.id IN [";

                for (int j = 0; j < query3_ans_list.size(); j++) {
                    Query3Result q3;
                    q3 = parseQuery3Results(query3_ans_list[j]);
                    query3Results.push_back(q3);
                    if (j == 0) {
                        tid = tid + q3.friendPerson.id;
                    }
                    else {
                        tid = tid + ", " + q3.friendPerson.id;
                    }
                }
                tid = tid + "] ";
                // cout << "tid: " << tid << endl;
            }

            tid_length = tid.size();
            // cout << "节点0的tid_length: " << tid_length << endl;
            MPI_Bcast(&tid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            tid.resize(tid_length);
            MPI_Bcast(&tid[0], tid_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
        }
        if (i == 3) {
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
                string new_query = str1 + tid + "AND city.id IN [" + cities_id + "]\n" + str2;

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
                string new_query = str1 + tid + "AND city.id IN [" + cities_id + "]\n" + str2;
                // cout << "节点 " << world_rank << endl;
                // cout << "new_query: " << new_query << endl;
                // cout << "tid: " << tid << endl;

                vector<string> results(world_size - 1);
                // vector<string> results_1(8);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[4500000];
                    MPI_Recv(buffer, 4500000, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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

                // cout << "query4_ans_list: " << query4_ans_list.size() << endl;

                tid1 = "WHERE friend.id IN [";

                for (int j = 0; j < query4_ans_list.size(); j++) {
                    Query4Result q4;
                    // cout << query4_ans_list[j] << endl;
                    q4 = parseQuery4Results(query4_ans_list[j]);
                    query4Results.push_back(q4);
                    if (j < 20) {
                        if (j == 0) {
                            tid1 = tid1 + q4.friendPerson.id;
                        }
                        else {
                            tid1 = tid1 + ", " + q4.friendPerson.id;
                        }
                    }
                }
                tid1 = tid1 + "]\n";
                // cout << "tid1: " << tid1 << endl;
            }

            tid1_length = tid1.size();
            // cout << "节点0的tid1_length: " << tid1_length << endl;
            MPI_Bcast(&tid1_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            tid1.resize(tid1_length);
            MPI_Bcast(&tid1[0], tid1_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
        }
        if (i == 4) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            MPI_Bcast(&tid1_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            // cout << "tid1_length: " << tid1_length << endl;
            if (world_rank != 0) {
                // cout << "这不是0号节点" << endl;
                // 调整字符串大小以适应接收的数据
                tid1.resize(tid1_length);
            }
            MPI_Bcast(&tid1[0], tid1_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            if (world_rank != 0) {
                string new_query = str1 + tid1 + str2;
                // cout << world_rank << " " << new_query << endl;

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
                string new_query = str1 + tid1 + str2;
                // cout << "节点 " << world_rank << endl;
                // cout << "new_query: " << new_query << endl;
                // cout << "tid: " << tid << endl;

                vector<string> results(world_size - 1);
                // vector<string> results_1(8);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[4000000];
                    MPI_Recv(buffer, 4000000, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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
                            query7_ans_list.push_back(now_ans[k]);
                        }
                    }
                }

                // cout << "query7_ans_list: " << query7_ans_list.size() << endl;
                pid = "WHERE message.id IN [";

                for (int j = 0; j < query7_ans_list.size(); j++) {
                    Query7Result q7;
                    q7 = parseQuery7Results(query7_ans_list[j]);
                    for (int k = 0; k < q7.message_list.size(); k++) {
                        if (j == 0 && k == 0) {
                            pid = pid + q7.message_list[k];
                        }
                        else {
                            pid = pid + ", " + q7.message_list[k];
                        }
                    }
                    q7.xyCount = 0;
                    q7.xCount = 0;
                    q7.yCount = 0;
                    query7Results.push_back(q7);
                }
                pid = pid + "]";
                // cout << "pid: " << pid << endl;
                // cout << "q7.size(): " << query7Results.size() << endl;
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
        if (i == 5) {
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
                string new_query = str1 + tid1 + str2;

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
                string new_query = str1 + tid1 + str2;
                // cout << "节点 " << world_rank << endl;
                // cout << "new_query: " << new_query << endl;
                // cout << "tid: " << tid << endl;

                vector<string> results(world_size - 1);
                vector<string> query8_ans_list;
                // vector<string> results_1(8);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[363840];
                    MPI_Recv(buffer, 363840, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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
                            query8_ans_list.push_back(now_ans[k]);
                        }
                    }
                }

                // cout << "query8_ans_list: " << query8_ans_list.size() << endl;

                comid = "WHERE message.id IN [";

                for (int j = 0; j < query7_ans_list.size(); j++) {
                    Query7Result q7;
                    q7 = parseQuery7Results(query7_ans_list[j]);
                    for (int k = 0; k < q7.message_list.size(); k++) {
                        if (j == 0 && k == 0) {
                            comid = comid + q7.message_list[k];
                        }
                        else {
                            comid = comid + ", " + q7.message_list[k];
                        }
                    }
                    q7.xyCount = 0;
                    q7.xCount = 0;
                    q7.yCount = 0;
                    query7Results.push_back(q7);
                }
                comid = comid + "]\n";
                // cout << "comid: " << comid << endl;
            }

            comid_length = comid.size();
            // cout << "节点0的comid_length: " << comid_length << endl;
            MPI_Bcast(&comid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            comid.resize(comid_length);
            MPI_Bcast(&comid[0], comid_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
        }
        if (i == 6) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            MPI_Bcast(&comid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            // cout << "comid_length: " << comid_length << endl;
            if (world_rank != 0) {
                // cout << "这不是0号节点" << endl;
                // 调整字符串大小以适应接收的数据
                comid.resize(comid_length);
            }
            MPI_Bcast(&comid[0], comid_length, MPI_CHAR, 0, MPI_COMM_WORLD);

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
                // cout << "tid: " << tid << endl;

                vector<string> results(world_size - 1);
                // vector<string> results_1(8);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[926384];
                    MPI_Recv(buffer, 926384, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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
                            query5_ans_list.push_back(now_ans[k]);
                        }
                    }
                }

                // cout << "query5_ans_list: " << query5_ans_list.size() << endl;

                for (int j = 0; j < query5_ans_list.size(); j++) {
                    Query5Result q5;
                    q5 = parseQuery5Results(query5_ans_list[j]);
                    string ttime = q5.message.creationDate.substr(1);
                    ttime = ttime.substr(0, ttime.length() - 1);
                    // cout << "ttime: " << ttime << endl;
                    int64_t timestamp = convertToTimestamp(ttime);
                    // if (j < 10) {
                        // cout << "timestamp: " << timestamp << endl;
                    // }
                    // cout << "timestamp: " << timestamp << endl;
                    if (timestamp >= 1311281784409 && timestamp <= 1347462600454) {
                        query5Results.push_back(q5);
                    }
                }
                // cout << "q5.size(): " << query5Results.size() << endl;
            }

            // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
        }
        if (i == 7) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            if (world_rank != 0) {
                string new_query = str1 + comid + str2;

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
                string new_query = str1 + comid + str2;
                // cout << "节点 " << world_rank << endl;
                // cout << "new_query: " << new_query << endl;
                // cout << "tid: " << tid << endl;

                vector<string> results(world_size - 1);
                // vector<string> results_1(8);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[16384];
                    MPI_Recv(buffer, 16384, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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
                            query6_ans_list.push_back(now_ans[k]);
                        }
                    }
                }

                // cout << "query6_ans_list: " << query6_ans_list.size() << endl;

                for (int j = 0; j < query6_ans_list.size(); j++) {
                    Query6Result q6;
                    q6 = parseQuery6Results(query6_ans_list[j]);
                    string ttime = q6.message.creationDate.substr(1);
                    ttime = ttime.substr(0, ttime.length() - 1);
                    // cout << "ttime: " << ttime << endl;
                    int64_t timestamp = convertToTimestamp(ttime);
                    // if (j < 10) {
                        // cout << "timestamp: " << timestamp << endl;
                    // }
                    // cout << "timestamp: " << timestamp << endl;
                    if (timestamp >= 1347423577314 && timestamp <= 1347462600454) {
                        query6Results.push_back(q6);
                    }
                }
                // cout << "q5.size(): " << query5Results.size() << endl;
                // cout << "q6.size(): " << query6Results.size() << endl;

                for (int j = 0; j < query7Results.size(); j++) {
                    for (int k = 0; k < query7Results[j].message_list.size(); k++) {
                        for (int a = 0; a < query5Results.size(); a++) {
                            // cout << "query5Results[a].message.id:" << query5Results[a].message.id << endl;
                            if (query7Results[j].message_list[k] == query5Results[a].message.id) {
                                query7Results[j].xCount += query5Results[a].messageX;
                                query7Results[j].yCount += query5Results[a].messageY;
                                break;
                            }
                        }
                    }
                    query7Results[j].xyCount = query7Results[j].xCount + query7Results[j].yCount;
                }
                for (int j = 0; j < query7Results.size(); j++) {
                    for (int k = 0; k < query7Results[j].message_list.size(); k++) {
                        for (int a = 0; a < query6Results.size(); a++) {
                            // cout << "query5Results[a].message.id:" << query5Results[a].message.id << endl;
                            if (query7Results[j].message_list[k] == query6Results[a].message.id) {
                                query7Results[j].xCount += query6Results[a].messageX;
                                query7Results[j].yCount += query6Results[a].messageY;
                                break;
                            }
                        }
                    }
                    query7Results[j].xyCount = query7Results[j].xCount + query7Results[j].yCount;
                }
                // cout << "q7.size(): " << query7Results.size() << endl;

                sort(query7Results.begin(), query7Results.end(), cmp1);
                for (int i = 1; i < query7Results.size(); i++) {
                    if (query7Results[i].friendId == query7Results[i - 1].friendId) {
                        query7Results[i].xCount += query7Results[i - 1].xCount;
                        query7Results[i].yCount += query7Results[i - 1].yCount;
                        query7Results[i].xyCount += query7Results[i - 1].xyCount;
                        query7Results[i - 1].xCount = -1;
                        query7Results[i - 1].yCount = -1;
                        query7Results[i - 1].xyCount = -1;
                    }
                }

                sort(query7Results.begin(), query7Results.end(), cmp2);
                auto now = chrono::system_clock::now();
                auto duration_since_epoch = now.time_since_epoch();
                auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                time_t now_time_t = chrono::system_clock::to_time_t(now);
                tm* now_tm = localtime(&now_time_t);
                fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                for (int i = 0; i < query7Results.size(); i++) {
                    if (i > 9) {
                        break;
                    }
                    print(query7Results[i]);
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