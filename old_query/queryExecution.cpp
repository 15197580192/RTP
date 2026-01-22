#include <iostream>
#include <cstring>
#include <vector>
#include <map>
#include <fstream>
#include <sstream>
#include <Python.h>
using namespace std;

struct Comment{
    string id;
    string creationDate;
    string locationIP;
    string browserUsed;
    string content;
    string length;
}

struct Forum{
    string id;
    string title;
    string creationDate;
}

struct Organisation{
    string id;
    string type;
    string name;
    string url;
}

struct Person{
    string id;
    string firstName;
    string lastName;
    string gender;
    string birthday;
    string creationDate;
    string locationIP;
    string browserUsed;
}

struct Place{
    string id;
    string name;
    string url;
    string type;
}

struct Post{
    string id;
    string imageFile;
    string creationDate;
    string locationIP;
    string browserUsed;
    string language;
    string content;
    string length;
}

struct Tag{
    string id;
    string name;
    string url;
}

struct Tagclass{
    string id;
    string name;
    string url;
}

vector<string> query_list;

map<string, string> Mymap;
vector<Mymap> data;

void connect2Neo4j(string sentence, string nowIP) {
    // 初始化 Python 解释器
    Py_Initialize();

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

void stringSplit(string str, const char split,vector<string>& res)
{
	istringstream iss(str);	// 输入流
	string token;			// 接收缓冲区
	while (getline(iss, token, split))	// 以split为分隔符
	{
		res.push_back(token);
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

// std::pair<std::string, std::string> parseKeyValuePair(const std::string& kv) {
//     std::regex kvRegex(R"((\w+)='([^']*)'|(\w+): (\d+))");
//     std::smatch matches;
//     if (std::regex_search(kv, matches, kvRegex)) {
//         if (matches.size() == 5) { // 数字键值对
//             return {matches[3], matches[4]};
//         } else if (matches.size() == 3) { // 字符串键值对
//             return {matches[1], matches[2]};
//         }
//     }
//     return {"", ""};
// }

void getAnsList(string ans) {
    vector<string> ans_list;
    string tans = ans.substr(1);
    tans = tans.substr(0, tans.length() - 1);
    cout << "tans: " << tans << endl;
    stringSplit(tans, "}, {", ans_list);
    cout << "ans_list: " << endl;
    for (int i = 0; i < ans_list.size(); i++) {
        cout << ans_list[i] << endl;
    }
    cout << "-------------------------------------------" << endl;

    for (int i = 0; i < ans_list.size(); i++) {
        if (ans_list[i].find("Node") != -1) {

        }
        else {
            // parseKeyValuePair();
        }
    }
}

int main() {
    string file_path = "./queries/interactive-complex-1.txt";
    string nowIP = "http://116.63.183.28:7474/";
    // getIPList(ip_path);
    getQueryList(file_path);
    for (const auto& query : query_list) {
        cout << "now_query: " << query << endl;
        connect2Neo4j(query, nowIP);
        break;
    }

    // std::string neo4jResult = "[{'distance': 4, 'friend': Node('Person', birthday='1986-03-09', browserUsed='Internet Explorer', creationDate='2011-08-19T08:01:56.626+0000', firstName='Tom', gender='male', id='87096098', lastName='Aa', locationIP='79.171.64.2')}, {'distance': 3, 'friend': Node('Person', birthday='1980-10-23', browserUsed='Chrome', creationDate='2012-05-02T00:07:50.185+0000', firstName='Jan', gender='female', id='87096561', lastName='Aa', locationIP='204.231.231.108')}]";
    std::string neo4jResult = "[{'distance': 4, 'friend': Node('Person', birthday='1986-03-09', browserUsed='Internet Explorer', creationDate='2011-08-19T08:01:56.626+0000', firstName='Tom', gender='male', id='87096098', lastName='Aa', locationIP='79.171.64.2')}]";

    return 0;
}