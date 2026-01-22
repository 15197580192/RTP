#include <iostream>
#include <cstring>
#include <algorithm>
#include <vector>
#include <cctype>
#include <Python.h>

using namespace std;

vector<string> query_list;

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
            PyObject* pValue = PyUnicode_FromString("MATCH (n:Person) RETURN n LIMIT 25;"); // 将 C++ 字符串转换为 Python 字符串
            PyTuple_SetItem(pArgs, 0, pValue); // 将 Python 字符串放入元组
            pValue = PyUnicode_FromString("http://122.9.162.58:7474/");
            PyTuple_SetItem(pArgs, 1, pValue);
            
            // 调用函数
            // PyUnicode_AsUTF8(pArgs);
            // 新的调用，带参数
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

string trimTrailingWhitespace(string str) {
    // find_last_not_of 返回最后一个不在参数中的字符的位置
    size_t end = str.find_last_not_of(" \t\n\r\f\v");
    while (end != std::string::npos && !std::isalnum(str[end])) {
        end = str.find_last_not_of(" \t\n\r\f\v", end - 1);
    }
    if (end != std::string::npos) {
        str = str.substr(0, end + 1);
        return str;
    } else {
        str.clear(); // 字符串只包含空白字符或没有字母或数字
        return "";
    }
}

string addDynamicReturnStatement(string query) {
    // 检查是否已包含RETURN关键字
    if (query.find("RETURN") == string::npos) {
        // 查找最后一个WITH关键字的位置
        size_t withPos = query.rfind("WITH ");
        if (withPos != string::npos) {
            // 查找LIMIT关键字的位置
            size_t limitPos = query.find("LIMIT", withPos);
            string returnValues;

            // 提取WITH和LIMIT（如果存在）之间的字符串
            if (limitPos != string::npos) {
                returnValues = query.substr(withPos + 5, limitPos - (withPos + 5));
            } else {
                // 如果没有LIMIT，则提取WITH之后的所有内容
                returnValues = query.substr(withPos + 5);
            }
            cout << "returnValues: " << returnValues << endl;
            // 清理返回值字符串（例如，移除ORDER BY子句）
            size_t orderByPos = returnValues.find("ORDER BY");
            if (orderByPos != string::npos) {
                cout << "能找到order by" << endl;
                returnValues = returnValues.substr(0, orderByPos);
            }

            // 在LIMIT之前或查询末尾插入RETURN语句
            if (limitPos != string::npos) {
                query.insert(limitPos, "RETURN " + returnValues + " ");
            } else {
                query += " RETURN " + returnValues;
            }
        }
    }

    return query;
}

void getQueryList(string query, string delimiter1, string delimiter2) {
    size_t pos = 0, prev = 0;

    while (pos != std::string::npos) {
        size_t pos1 = query.find(delimiter1, pos);
        size_t pos2 = query.find(delimiter2, pos);

        // 找到最近的分隔符
        pos = (pos1 != std::string::npos && pos2 != std::string::npos) ? std::min(pos1, pos2) :
              (pos1 != std::string::npos) ? pos1 : pos2;

        if (pos != std::string::npos) {
            if (pos != prev) {
                string tquery = query.substr(prev, pos - prev);
                tquery = trimTrailingWhitespace(tquery);
                cout << "now_query: " << tquery << endl;
                tquery = addDynamicReturnStatement(tquery);
                cout << "处理完后: " << tquery << endl;
                query_list.push_back(tquery);
            }
            prev = pos;
            pos += (pos == pos1) ? delimiter1.length() : delimiter2.length();
        }
    }

    if (prev < query.length()) {
        string tquery = query.substr(prev);
        tquery = trimTrailingWhitespace(tquery);
        tquery = addDynamicReturnStatement(tquery);
        query_list.push_back(tquery);
    }
}

int main() {
    string query = R"(MATCH (p:Person {id: "87095182"}), (friend:Person)
                            WHERE NOT p=friend
                            WITH p, friend
                            MATCH path = shortestPath((p)-[:knows*1..]-(friend))
                            WITH min(length(path)) AS distance, friend
                    ORDER BY
                        friend.lastName ASC,
                        toInteger(friend.id) ASC
                    LIMIT 20
                    MATCH (friend)-[:isLocatedIn]->(friendCity:Place)
                    OPTIONAL MATCH (friend)-[studyAt:studyAt]->(uni:Organisation)-[:isLocatedIn]->(uniCity:Place)
                    WITH friend, collect(
                        CASE
                            WHEN uni IS NULL THEN null
                            ELSE [uni.name, studyAt.classYear, uniCity.name]
                        END ) AS unis, friendCity, distance
                    OPTIONAL MATCH (friend)-[workAt:workAt]->(company:Organisation)-[:isLocatedIn]->(companyCountry:Place)
                    WITH friend, collect(
                        CASE
                            WHEN company IS NULL then null
                            ELSE [company.name, workAt.workFrom, companyCountry.name]
                        END ) AS companies, unis, friendCity
                    RETURN
                        friend.id AS friendId,
                        friend.lastName AS friendLastName,
                        friend.birthday AS friendBirthday,
                        friend.creationDate AS friendCreationDate,
                        friend.gender AS friendGender,
                        friend.browserUsed AS friendBrowserUsed,
                        friend.locationIP AS friendLocationIp,
                        friend.email AS friendEmails,
                        friend.speaks AS friendLanguages,
                        friendCity.name AS friendCityName,
                        unis AS friendUniversities,
                        companies AS friendCompanies
                    ORDER BY
                        friendLastName ASC,
                        toInteger(friendId) ASC
                    LIMIT 20)";
    // cout << query << endl;
    getQueryList(query, "MATCH (", "OPTIONAL MATCH (");
    cout << query_list.size() << endl;
    for (int i = 0; i< query_list.size(); i++) {
        cout << query_list[i] << endl;
        cout << "---------------------------------------------" << endl;
    }
    return 0;
}