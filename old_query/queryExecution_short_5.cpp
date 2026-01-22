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

vector<string> query_list;
vector<string> query1_ans_list;

struct Query1Result {
    string personId;
    string firstName;
    string lastName;
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

Query1Result parseQuery1Results(string data) {
    Query1Result q1;
    // cout << "data:" << data << endl;
    vector<string> sub_data;
    stringSplit(data, ", '", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        // cout << sub_data[i] << endl;
        int pos = sub_data[i].find(':');
        string tdata = sub_data[i].substr(pos + 2);
        if (i == 0) {
            q1.personId = tdata;
        }
        if (i == 1) {
            q1.firstName = tdata;
        }
        if (i == 2) {
            q1.lastName = tdata;
        }
    }
    return q1;
}

void print(Query1Result q) {
    cout << "---------------------------------------" << endl;
    cout << "'personId': " << q.personId << ", 'firstName': " << q.firstName << ", 'lastName': " << q.lastName << endl;
}

int main() {
    auto start = std::chrono::high_resolution_clock::now();

    string file_path = "./queries/interactive-short-5.txt";
    // string file_path = "./queries/try_query-1.txt";
    string nowIP = "bolt://139.9.250.196:7687/";
    // getIPList(ip_path);
    string neo4jResult;
    vector<Query1Result> query1Results;

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
    }

    for (int i = 0; i < query1Results.size(); i++) {
        print(query1Results[i]);
    }

    // 获取结束时间
    auto end = std::chrono::high_resolution_clock::now();

    // 计算所经过的时间
    std::chrono::duration<double> elapsed = end - start;

    // 输出结果
    std::cout << "Elapsed time: " << elapsed.count() << " seconds." << std::endl;

    return 0;
}