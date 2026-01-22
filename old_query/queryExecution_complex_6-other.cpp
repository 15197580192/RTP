#include <iostream>
#include <cstring>
#include <algorithm>
#include <vector>
#include <set>
#include <map>
#include <fstream>
#include <sstream>
#include <optional>
#include <chrono>
#include <Python.h>
#include <iomanip>
using namespace std;

struct Comment{
    string id;
    string creationDate;
    string locationIP;
    string browserUsed;
    string content;
    string length;
};

struct Forum{
    string id;
    string title;
    string creationDate;
};

struct Organisation{
    string id;
    string type;
    string name;
    string url;
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

struct Tag{
    string id;
    string name;
    string url;
};

struct Tagclass{
    string id;
    string name;
    string url;
};

vector<string> query_list;
vector<string> query1_ans_list;
vector<string> query2_ans_list;
vector<string> query2Other_ans_list;
vector<string> query3_ans_list;
vector<string> query4_ans_list;

struct Query1Result {
    string knownTagId;
};

struct Query2Result {
    Person friendPerson;
};

struct Query3Result {
    Post friendPost;
};

struct Query4Result {
    Post friendPost;
    Tag postTag;
    int postCount;
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
        std::cerr << "无法打开文件：" << filePath << std::endl;
        return ;
    }

    // 打印所有查询
    // for (const auto& query : query_list) {
    //     std::cout << "Cypher Query:\n" << query << "---\n";
    // }
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
    cout << "data:" << data << endl;
    int pos = data.find(':');
    string tl = data.substr(pos + 2);
    cout << "tl:" << tl << endl;
    q1.knownTagId = tl;
    return q1;
}

Query2Result parseQuery2Results(string data) {
    Query2Result q2;
    // cout << "data:" << data << endl;
    Person p = parsePerson(data);
    q2.friendPerson = p;
    return q2;
}

Query3Result parseQuery3Results(string data) {
    Query3Result q3;
    // cout << "data:" << data << endl;
    Post p = parsePost(data);
    q3.friendPost = p;
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
            Post p = parsePost(sub_data[i]);
            q4.friendPost = p;
        }
        if (i == 1) {
            Tag t = parseTag(sub_data[i]);
            q4.postTag = t;
        }
    }
    return q4;
}

bool cmp(Query4Result a, Query4Result b) {
    if (a.postTag.id.length() != b.postTag.id.length()) {
        return a.postTag.id.length() < b.postTag.id.length();
    }
    return a.postTag.id < b.postTag.id;
}

bool cmp1(Query4Result a, Query4Result b) {
    if (a.postCount != b.postCount) {
        return a.postCount > b.postCount;
    }
    if (a.postTag.name.length() != b.postTag.name.length()) {
        return a.postTag.name.length() < b.postTag.name.length();
    }
    return a.postTag.name < b.postTag.name;
}

void print(Query4Result q4) {
    cout << "---------------------------------------" << endl;
    cout << "'tagName': " << q4.postTag.name << ", 'postCount': " << q4.postCount << endl;
}

int main() {
    auto start = std::chrono::high_resolution_clock::now();

    string file_path = "./queries/interactive-complex-6-other.txt";
    // string file_path = "./queries/try_query-1.txt";
    string nowIP = "bolt://139.9.250.196:7687/";
    // getIPList(ip_path);
    string neo4jResult, tid, tid1, pid;
    vector<Query1Result> query1Results;
    vector<Query2Result> query2Results;
    vector<Query3Result> query3Results;
    vector<Query4Result> query4Results;

    getQueryList(file_path);
    for (int i = 0; i < query_list.size(); i++) {
        cout << "now_query: " << query_list[i] << endl;
        if (i == 0) {
            neo4jResult = connect2Neo4j(query_list[i], nowIP);
            cout << "neo4jResult:" << neo4jResult << endl;
            query1_ans_list = getAnsList(neo4jResult);
            cout << "query1_ans_list: " << query1_ans_list.size() << endl;

            for (int j = 0; j < query1_ans_list.size(); j++) {
                Query1Result q1;
                q1 = parseQuery1Results(query1_ans_list[j]);
                query1Results.push_back(q1);
            }
        }
        if (i == 1) {
            tid = "WHERE p.id IN [";

            string new_query = query_list[i];
            cout << "new_query:" << new_query << endl;
            neo4jResult = connect2Neo4j(new_query, nowIP);
            // cout << "neo4jResult: " << neo4jResult << endl;
            query2_ans_list = getAnsList(neo4jResult);
            cout << "query2_ans_list: " << query2_ans_list.size() << endl;
            for (int j = 0; j < query2_ans_list.size(); j++) {
                Query2Result q2;
                q2 = parseQuery2Results(query2_ans_list[j]);
                if (j == 0) {
                    tid = tid + q2.friendPerson.id;
                }
                else {
                    tid = tid + ", " + q2.friendPerson.id;
                }
            }
            tid = tid + "]\n";
            // cout << "tid: " << tid << endl;
        }
        if (i == 2) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            tid1 = "WHERE friend.id IN [";

            string new_query = str1 + tid1 + str2;
            cout << "new_query:" << new_query << endl;
            neo4jResult = connect2Neo4j(new_query, nowIP);
            // cout << "neo4jResult: " << neo4jResult << endl;
            query2Other_ans_list = getAnsList(neo4jResult);
            cout << "query2Other_ans_list: " << query2Other_ans_list.size() << endl;
            for (int j = 0; j < query2Other_ans_list.size(); j++) {
                Query2Result q2;
                q2 = parseQuery2Results(query2Other_ans_list[j]);
                if (q2.friendPerson.id == "87095182") {
                    break;
                }
                bool flag = false;
                for (int k = 0; k < query2Results.size(); k++) {
                    if (query2Results[k].friendPerson.id == q2.friendPerson.id) {
                        flag = true;
                        break;
                    }
                }
                if (flag == false) {
                    query2Results.push_back(q2);
                }
            }
            for (int j = 0; j < query2Results.size(); j++) {
                if (j == 0) {
                    tid1 = tid1 + query2Results[j].friendPerson.id;
                }
                else {
                    tid1 = tid1 + ", " + query2Results[j].friendPerson.id;
                }
            }
            tid1 = tid1 + "]\n";
            // cout << "tid1: " << tid1 << endl;
        }
        if (i == 3) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            string new_query = str1 + tid1 + str2;
            cout << "new_query:" << new_query << endl;
            neo4jResult = connect2Neo4j(new_query, nowIP);
            // cout << "neo4jResult: " << neo4jResult << endl;
            query3_ans_list = getAnsList(neo4jResult);

            cout << "query3_ans_list: " << query3_ans_list.size() << endl;

            pid = "WHERE post.id IN [";
            for (int j = 0; j < query3_ans_list.size(); j++) {
                Query3Result q3;
                q3 = parseQuery3Results(query3_ans_list[j]);
                query3Results.push_back(q3);
                if (j == 0) {
                    pid = pid + q3.friendPost.id;
                }
                else {
                    pid = pid + ", " + q3.friendPost.id;
                }
            }
            pid = pid + "]\n";
            // cout << "pid: " << pid << endl;
        }
        if (i == 4) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            string new_query = str1 + pid + str2;
            cout << "new_query:" << new_query << endl;
            neo4jResult = connect2Neo4j(new_query, nowIP);
            // cout << "neo4jResult: " << neo4jResult << endl;
            query4_ans_list = getAnsList(neo4jResult);
            cout << "query4_ans_list: " << query4_ans_list.size() << endl;
            for (int j = 0; j < query4_ans_list.size(); j++) {
                Query4Result q4;
                q4 = parseQuery4Results(query4_ans_list[j]);
                if (q4.postTag.id != query1Results[0].knownTagId) {
                    query4Results.push_back(q4);
                }
            }
            cout << "query4Results.size(): " << query4Results.size() << endl;
        }

        auto end = std::chrono::high_resolution_clock::now();

        // 计算所经过的时间
        std::chrono::duration<double> elapsed = end - start;

        // 输出结果
        std::cout << "Elapsed time: " << elapsed.count() << " seconds." << std::endl;
    }

    sort(query4Results.begin(), query4Results.end(), cmp);

    string now_id = query4Results[0].postTag.id;
    query4Results[0].postCount = 0;
    int now_cnt = 0;
    for (int i = 1; i < query4Results.size(); i++) {
        if (query4Results[i].postTag.id == now_id) {
            query4Results[i].postCount = 0;
            now_cnt++;
        }
        else {
            query4Results[i - 1].postCount = now_cnt;
            now_id = query4Results[i].postTag.id;
            query4Results[i].postCount = 0;
            now_cnt = 0;
        }
    }
    query4Results[query4Results.size() - 1].postCount = now_cnt;

    sort(query4Results.begin(), query4Results.end(), cmp1);
    for (int i = 0; i < query4Results.size(); i++) {
        if (i > 9) {
            break;
        }
        print(query4Results[i]);
    }


    // 获取结束时间
    auto end = std::chrono::high_resolution_clock::now();

    // 计算所经过的时间
    std::chrono::duration<double> elapsed = end - start;

    // 输出结果
    std::cout << "Elapsed time: " << elapsed.count() << " seconds." << std::endl;

    return 0;
}