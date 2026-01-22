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
vector<string> query3_ans_list;
vector<string> query4_ans_list;
vector<string> query5_ans_list;
vector<string> query6_ans_list;

struct Query1Result {
    int distance;
    Person friendPerson;
};

struct Query2Result {
    Place friendCity;
};

struct Query3Result {
    vector<vector<string>> unis;
    string fid;
};

struct Query4Result {
    string uniCityName;
    string uniId;
};

struct Query5Result {
    string friendId;
    string friendLastName;
    string friendBirthday;
    string friendCreationDate;
    string friendGender;
    string friendBrowserUsed;
    string friendLocationIp;
    string friendEmails;
    string friendLanguages;
    string friendCityName;
    vector<vector<string>> friendUniversities;
    vector<vector<string>> friendCompanies;
    Organisation company;
};

struct Query6Result {
    Place companyCountry;
    string companyId;
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

Organisation parseOrganisation(string data) {
    istringstream dataStream(data);
    Organisation info;
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
            else if (key == "type") info.type = value;
            else if (key == "name") info.name = value;
            else if (key == "url") info.url = value;
        }
    }

    return info;
}

vector<vector<string>> parseVector(string data) {
    vector<vector<string>> total_v;
    vector<string> temp;
    int pos = data.find(':');
    string v_data = data.substr(pos + 4);
    // cout << "v_data: " << v_data << endl;
    v_data = v_data.substr(0, v_data.length() - 2);
    // cout << "v_data: " << v_data << endl;
    // cout << "v_data.length(): " << v_data.length() << endl;
    if (v_data.length() == 0) {
        cout << "到这了" << endl;
        return total_v;
    }
    else{
        stringSplit(v_data, "], [", temp);
        for (int i = 0; i < temp.size(); i++) {
            // cout << "temp[i]: " << temp[i] << endl;
            vector<string> temp_v;
            stringSplit(temp[i], ", ", temp_v);
            total_v.push_back(temp_v);
        }
        // cout << "循环结束" << endl;
    }
    return total_v;
}

Query1Result parseQuery1Results(string data) {
    Query1Result q1;
    // cout << "data:" << data << endl;
    vector<string> sub_data;
    stringSplit(data, ", '", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        // cout << sub_data[i] << endl;
        if (i == 0) {
            int pos = sub_data[i].find(':');
            int tnum = stoi(sub_data[i].substr(pos + 1));
            q1.distance = tnum;
        }
        if (i == 1) {
            Person p = parsePerson(sub_data[1]);
            // cout << p.id << " " << p.gender << endl;
            q1.friendPerson = p;
        }
    }
    return q1;
}

Query2Result parseQuery2Results(string data) {
    Query2Result q2;
    // cout << "data:" << data << endl;
    Place p = parsePlace(data);
    // cout << p.id << " " << p.gender << endl;
    q2.friendCity = p;
    return q2;
}

Query3Result parseQuery3Results(string data) {
        // cout << "data: " << data << endl;
    Query3Result q3;
    // cout << "data.length(): " << data.length() << endl;
    vector<string> sub_data;
    stringSplit(data, ", 'friend", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        // cout << "query3: " << sub_data[i] << endl;
        // cout << "len: " << sub_data[i].length() << endl;
        if (i == 0) {
            if (sub_data[i].length() <=10) {
                // cout << "到这了" << endl;
                vector<vector<string>> v;
                vector<string> t_v;
                t_v.push_back("null");
                v.push_back(t_v);
                q3.unis = v;
                // cout << "赋值完成" << endl;
            }
            else{
                // cout << "执行函数" << endl;
                vector<vector<string>> v = parseVector(sub_data[i]);
                // cout << "v: " << v[0][0] << endl;
                // cout << "v: " << v[0][2] << endl;
                q3.unis = v;
            }
        }
        if (i == 1) {
            int pos = sub_data[i].find(":");
            q3.fid = sub_data[i].substr(pos + 2);
        }
    }
    return q3;
}

Query4Result parseQuery4Results(string data) {
    Query4Result q4;

    vector<string> sub_data;
    stringSplit(data, ", '", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        cout << sub_data[i] << endl;
        if (i == 0) {
            int pos = sub_data[i].find(':');
            string td = sub_data[i].substr(pos + 2);
            q4.uniCityName = td;
        }
        if (i == 1) {
            int pos = sub_data[i].find(':');
            string td = sub_data[i].substr(pos + 2);
            q4.uniId = td;
        }
    }
    return q4;
}

Query5Result parseQuery5Results(string data) {
    // cout << "data: " << data << endl;
    Query5Result q5;
    string now_data = data;
    int now_num = 0;
    vector<vector<string>> v;
    while(1) {
        int pos = now_data.find(": ");
        if (pos == -1) {
            break;
        }
        else{
            now_data = now_data.substr(pos + 2);
            // cout << "now_data:" << now_data << endl;
            int tpos = now_data.find(',');
            string now_str = "";
            // cout << "now_data: " << now_data << endl;
            // cout << now_data[0] << " " << now_data[1] << " " << now_data[2] << endl;
            if (now_data[0] == 'N' && now_data[1] == 'o' && now_data[2] == 'd') {
                now_str = now_data;
            }
            else {
                if (now_data[0] == '[') {
                    tpos = now_data.find(", 'company");
                }
                now_str = now_data.substr(0, tpos);
            }
            // cout << "now_str: " << now_str << endl;
            switch (now_num) {
                case 0:
                    q5.friendId = now_str;
                    break;
                case 1:
                    q5.friendLastName = now_str;
                    break;
                case 2:
                    q5.friendBirthday = now_str;
                    break;
                case 3:
                    q5.friendCreationDate = now_str;
                    break;
                case 4:
                    q5.friendGender = now_str;
                    break;
                case 5:
                    q5.friendBrowserUsed = now_str;
                    break;
                case 6:
                    q5.friendLocationIp = now_str;
                    break;
                case 7:
                    q5.friendEmails = now_str;
                    break;
                case 8:
                    q5.friendLanguages = now_str;
                    break;
                case 9:
                    if (now_str.length() <= 2) {
                        vector<string> t_v;
                        t_v.push_back("null");
                        v.push_back(t_v);
                        q5.friendCompanies = v;
                    }
                    else{
                        v = parseVector(now_str);
                        q5.friendCompanies = v;
                    }
                    break;
                case 10:
                    // cout << "现在是10:" << now_str << endl;
                    Organisation o = parseOrganisation(now_str);
                    q5.company = o;
                    break;
            }
            now_num++;
        }
    }
    // cout << "q5生成结束" << endl;
    return q5;
}

Query6Result parseQuery6Results(string data) {
    Query6Result q6;
    vector<string> sub_data;
    stringSplit(data, ", '", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        // cout << sub_data[i] << endl;
        if (i == 0) {
            Place p = parsePlace(data);
            // cout << "v: " << v[0][0] << endl;
            q6.companyCountry = p;
            // cout << "companyCountry.name:" << q6.companyCountry.name << endl;
        }
        if (i == 1) {
            int pos = sub_data[i].find(":");
            q6.companyId = sub_data[i].substr(pos + 2);
            // cout << "companyId: " << q6.companyId << endl;
        }
    }
    return q6;
}

void print(Query5Result q5) {
    cout << "---------------------------------------" << endl;
    cout << "'friendId': " << q5.friendId << ", 'friendLastName': " << q5.friendLastName << ", 'friendBirthday': " << q5.friendBirthday;
    cout << ", 'friendCreationDate': " << q5.friendCreationDate << ", 'friendGender': " << q5.friendGender << ", 'friendBrowserUsed': ";
    cout << q5.friendBrowserUsed << ", 'friendLocationIp': " << q5.friendLocationIp << ", 'friendEmails': " << q5.friendEmails;
    cout << ", 'friendLanguages': " << q5.friendLanguages << ", 'friendCityName': " << q5.friendCityName << ", 'friendUniversities': [";
    for (int i = 0; i < q5.friendUniversities.size(); i++) {
        cout << "[";
        for (int j = 0; j < q5.friendUniversities[i].size(); j++) {
            cout << q5.friendUniversities[i][j];
            if (j < q5.friendUniversities[i].size() - 1) {
                cout << ", ";
            }
        }
        cout << "]";
        if (i < q5.friendUniversities.size() - 1) {
            cout << ", ";
        }
    }
    cout << "], 'friendCompanies': [";
    for (int i = 0; i < q5.friendCompanies.size(); i++) {
        cout << "[";
        for (int j = 0; j < q5.friendCompanies[i].size(); j++) {
            cout << q5.friendCompanies[i][j];
            if (j < q5.friendCompanies[i].size() - 1) {
                cout << ", ";
            }
        }
        cout << "]";
        if (i < q5.friendCompanies.size() - 1) {
            cout << ", ";
        }
    }
    cout << "]" << endl;
}

int main() {
    auto start = std::chrono::high_resolution_clock::now();

    string file_path = "./queries/interactive-complex-1.txt";
    // string file_path = "./queries/try_query-1.txt";
    string nowIP = "bolt://116.63.188.96:7687/";
    // getIPList(ip_path);
    string neo4jResult, tid, oid, woid;
    vector<Query1Result> query1Results;
    vector<Query2Result> query2Results;
    vector<Query3Result> query3Results;
    vector<Query4Result> query4Results;
    vector<Query5Result> query5Results;
    vector<Query6Result> query6Results;

    getQueryList(file_path);
    for (int i = 0; i < query_list.size(); i++) {
        cout << "now_query: " << query_list[i] << endl;
        if (i == 0) {
            neo4jResult = connect2Neo4j(query_list[i], nowIP);
            // neo4jResult = "[{'distance': 3, 'friend': Node('Person', birthday='1989-07-28', browserUsed='Opera', creationDate='2010-01-16T12:57:05.671+0000', firstName='Jan', gender='female', id='9796', lastName='Aa', locationIP='62.122.29.222')}, {'distance': 3, 'friend': Node('Person', birthday='1981-02-27', browserUsed='Firefox', creationDate='2010-02-22T23:09:40.861+0000', firstName='Karel', gender='female', id='15870', lastName='Aa', locationIP='79.132.232.167')}]";
            cout << "neo4jResult:" << neo4jResult << endl;
            query1_ans_list = getAnsList(neo4jResult);
            cout << "query1_ans_list: " << query1_ans_list.size() << endl;

            tid = "WHERE friend.id IN [";

            for (int j = 0; j < query1_ans_list.size(); j++) {
                Query1Result q1;
                q1 = parseQuery1Results(query1_ans_list[j]);
                query1Results.push_back(q1);
                if (j == 0) {
                    tid = tid + q1.friendPerson.id;
                }
                else {
                    tid = tid + ", " + q1.friendPerson.id;
                }
            }
            tid = tid + "]\n";
            // cout << "tid: " << tid << endl;
        }
        if (i == 1) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            string new_query = str1 + tid + str2;
            neo4jResult = connect2Neo4j(new_query, nowIP);
            // cout << "neo4jResult: " << neo4jResult << endl;
            query2_ans_list = getAnsList(neo4jResult);
            // cout << "query2_ans_list: " << query2_ans_list.size() << endl;
            for (int j = 0; j < query2_ans_list.size(); j++) {
                Query2Result q2;
                q2 = parseQuery2Results(query2_ans_list[j]);
                // cout << "q2.id: " << q2.friendCity.id << endl; 
                query2Results.push_back(q2);
            }
        }
        if (i == 2) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);
            string new_query = str1 + tid + str2;
            cout << "new_query: " << new_query << endl;
            neo4jResult = connect2Neo4j(new_query, nowIP);

            cout << "neo4jResult: " << neo4jResult << endl;

            query3_ans_list = getAnsList(neo4jResult);
            cout << "query3_ans_list.size(): " << query3_ans_list.size() << endl;
            oid = "WHERE uni.id IN [";
            for (int j = 0; j < query3_ans_list.size(); j++) {
                Query3Result q3;
                q3 = parseQuery3Results(query3_ans_list[j]);
                query3Results.push_back(q3);
                if (q3.unis[0][0] == "null") {
                    continue;
                }
                else{
                    if (j == 0) {
                        oid = oid + q3.unis[0][2];
                    }
                    else {
                        oid = oid + ", " + q3.unis[0][2];
                    }
                }
            }
            oid = oid + "]\n";
            cout << "oid: " << oid << endl;
        }

        if (i == 3) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);
            string new_query = str1 + oid + str2;
            cout << "new_query: " << new_query << endl;
            neo4jResult = connect2Neo4j(new_query, nowIP);

            cout << "neo4jResult: " << neo4jResult << endl;

            query4_ans_list = getAnsList(neo4jResult);
            cout << "query4_ans_list.size(): " << query4_ans_list.size() << endl; 

            for (int j = 0; j < query4_ans_list.size(); j++) {
                Query4Result q4;
                q4 = parseQuery4Results(query4_ans_list[j]);
                query4Results.push_back(q4);
            }

            for (int j = 0; j < query3Results.size(); j++) {
                Query3Result q3 = query3Results[j];
                if (q3.unis[0][0] == "null") {
                    continue;
                }
                else {
                    for (int k = 0; k < query4Results.size(); k++) {
                        if (query4Results[k].uniId == query3Results[j].unis[0][2]) {
                            query3Results[j].unis[0][2] = query4Results[k].uniCityName;
                        }
                    }
                }
            }
            for (int a = 0; a < query3Results.size(); a++) {
                Query3Result q3 = query3Results[a];
                cout << "a: " << a << endl;
                for (int b = 0; b < q3.unis[0].size(); b++) {
                    cout << q3.unis[0][b] << " ";
                }
                cout << endl;
            }
        }

        if (i == 4) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);
            string new_query = str1 + tid + str2;
            cout << "new_query: " << new_query << endl;
            neo4jResult = connect2Neo4j(new_query, nowIP);
            cout << "neo4jResult: " << neo4jResult << endl;

            query5_ans_list = getAnsList(neo4jResult);
            woid = "WHERE company.id IN [";
            cout << "query5_ans_list.size(): " << query5_ans_list.size() << endl;
            for (int j = 0; j < query5_ans_list.size(); j++) {
                // cout << query4_ans_list[j] << endl;
                Query5Result q5;
                q5 = parseQuery5Results(query5_ans_list[j]);
                for (int k = 0; k < query1Results.size(); k++) {
                    if (q5.friendId == query1Results[k].friendPerson.id) {
                        cout << "q5.friendId: " << q5.friendId << endl;
                        q5.friendCityName = query2Results[k].friendCity.name;
                        cout << "q5.friendCityName: " << q5.friendCityName << endl;
                        cout << "k: " << k << endl;
                        cout << "query3Result: " << query3Results.size() << endl;
                        q5.friendUniversities = query3Results[k].unis;
                        cout << "能到这" << endl;
                        break;
                    }
                }
                cout << "friendCityName: " << q5.friendCityName << endl;
                // cout << q5.company.id << endl;
                query5Results.push_back(q5);
                // cout << "friendCompanies: " << q5.friendCompanies[0][2] << endl;
                if (q5.friendCompanies[0][0] == "null") {
                    continue;
                }
                else {
                    for (int k = 0; k < q5.friendCompanies.size(); k++) {
                        if (j == 0 && k == 0) {
                            woid = woid + q5.friendCompanies[k][2];
                        }
                        else {
                            woid = woid + ", " + q5.friendCompanies[k][2];
                        }
                    }
                }
            }
            woid = woid + "]\n";
            cout << "woid: " << woid << endl;
        }
        if (i == 5) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);
            string new_query = str1 + woid + str2;
            cout << "new_query: " << new_query << endl;
            neo4jResult = connect2Neo4j(new_query, nowIP);

            cout << "neo4jResult: " << neo4jResult << endl;
            query6_ans_list = getAnsList(neo4jResult);
            
            for (int j = 0; j < query6_ans_list.size(); j++) {
                Query6Result q6;
                q6 = parseQuery6Results(query6_ans_list[j]);
                query6Results.push_back(q6);
            }
            
            cout << "query6Results.size()" << query6Results.size() << endl;

            for (int j = 0; j < query5Results.size(); j++) {
                if (query5Results[j].friendCompanies[0][0] == "null") {
                    continue;
                }
                for (int k = 0; k < query5Results[j].friendCompanies.size(); k++) {
                    // cout << "fc" << query5Results[j].friendCompanies[k][2] << endl;
                    for (int a = 0; a < query6Results.size(); a++) {
                        if (query6Results[a].companyId == query5Results[j].friendCompanies[k][2]) {
                            query5Results[j].friendCompanies[k][2] = query6Results[a].companyCountry.name;
                            break;
                        }
                    }
                }
            }
            cout << "6能执行完" << endl;
        }

        auto end = std::chrono::high_resolution_clock::now();

        // 计算所经过的时间
        std::chrono::duration<double> elapsed = end - start;

        // 输出结果
        std::cout << "Elapsed time: " << elapsed.count() << " seconds." << std::endl;
    }
    for (int i = 0; i < query5Results.size(); i++) {
        print(query5Results[i]);
    }
    // 获取结束时间
    auto end = std::chrono::high_resolution_clock::now();

    // 计算所经过的时间
    std::chrono::duration<double> elapsed = end - start;

    // 输出结果
    std::cout << "Elapsed time: " << elapsed.count() << " seconds." << std::endl;

    return 0;
}