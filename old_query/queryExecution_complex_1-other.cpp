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
vector<string> query7_ans_list;
vector<string> query8_ans_list;

struct Query1Result {
    int distance;
    Person friendPerson;
};

struct Query2Result {
    int distance;
    Person friendPerson;
};

struct Query3Result {
    int distance;
    Person friendPerson;
};

struct Query4Result {
    Place friendCity;
};

struct Query5Result {
    vector<vector<string>> unis;
    string fid;
};

struct Query6Result {
    string uniCityName;
    string uniId;
};

struct Query7Result {
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

struct Query8Result {
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
    cout << "v_data: " << v_data << endl;
    cout << "v_data.length(): " << v_data.length() << endl;
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
    Person p = parsePerson(data);
    q1.friendPerson = p;
    q1.distance = 1;
    return q1;
}

Query2Result parseQuery2Results(string data) {
    Query2Result q2;
    // cout << "data:" << data << endl;
    Person p = parsePerson(data);
    q2.friendPerson = p;
    q2.distance = 2;
    return q2;
}

Query3Result parseQuery3Results(string data) {
    Query3Result q3;
    // cout << "data:" << data << endl;
    Person p = parsePerson(data);
    q3.friendPerson = p;
    q3.distance = 3;
    return q3;
}

Query4Result parseQuery4Results(string data) {
    Query4Result q4;
    // cout << "data:" << data << endl;
    Place p = parsePlace(data);
    q4.friendCity = p;
    return q4;
}

Query5Result parseQuery5Results(string data) {
    // cout << "data: " << data << endl;
    Query5Result q5;
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
                q5.unis = v;
                // cout << "赋值完成" << endl;
            }
            else{
                // cout << "执行函数" << endl;
                vector<vector<string>> v = parseVector(sub_data[i]);
                // cout << "v: " << v[0][0] << endl;
                // cout << "v: " << v[0][2] << endl;
                q5.unis = v;
            }
        }
        if (i == 1) {
            int pos = sub_data[i].find(":");
            q5.fid = sub_data[i].substr(pos + 2);
        }
    }
    return q5;
}

Query6Result parseQuery6Results(string data) {
    Query6Result q6;

    vector<string> sub_data;
    stringSplit(data, ", '", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        // cout << sub_data[i] << endl;
        if (i == 0) {
            int pos = sub_data[i].find(':');
            string td = sub_data[i].substr(pos + 2);
            q6.uniCityName = td;
        }
        if (i == 1) {
            int pos = sub_data[i].find(':');
            string td = sub_data[i].substr(pos + 2);
            q6.uniId = td;
        }
    }
    return q6;
}

Query7Result parseQuery7Results(string data) {
    // cout << "data: " << data << endl;
    Query7Result q7;
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
                    q7.friendId = now_str;
                    break;
                case 1:
                    q7.friendLastName = now_str;
                    break;
                case 2:
                    q7.friendBirthday = now_str;
                    break;
                case 3:
                    q7.friendCreationDate = now_str;
                    break;
                case 4:
                    q7.friendGender = now_str;
                    break;
                case 5:
                    q7.friendBrowserUsed = now_str;
                    break;
                case 6:
                    q7.friendLocationIp = now_str;
                    break;
                case 7:
                    q7.friendEmails = now_str;
                    break;
                case 8:
                    q7.friendLanguages = now_str;
                    break;
                case 9:
                    if (now_str.length() <= 2) {
                        vector<string> t_v;
                        t_v.push_back("null");
                        v.push_back(t_v);
                        q7.friendCompanies = v;
                    }
                    else{
                        v = parseVector(now_str);
                        q7.friendCompanies = v;
                    }
                    break;
                case 10:
                    // cout << "现在是10:" << now_str << endl;
                    Organisation o = parseOrganisation(now_str);
                    q7.company = o;
                    break;
            }
            now_num++;
        }
    }
    // cout << "q5生成结束" << endl;
    return q7;
}

Query8Result parseQuery8Results(string data) {
    Query8Result q8;
    vector<string> sub_data;
    stringSplit(data, ", '", sub_data);
    for (int i = 0; i < sub_data.size(); i++) {
        // cout << sub_data[i] << endl;
        if (i == 0) {
            Place p = parsePlace(data);
            // cout << "v: " << v[0][0] << endl;
            q8.companyCountry = p;
            // cout << "companyCountry.name:" << q6.companyCountry.name << endl;
        }
        if (i == 1) {
            int pos = sub_data[i].find(":");
            q8.companyId = sub_data[i].substr(pos + 2);
            // cout << "companyId: " << q6.companyId << endl;
        }
    }
    return q8;
}

bool cmp(Query1Result a, Query1Result b) {
    if (a.friendPerson.lastName.length() != b.friendPerson.lastName.length()) {
        return a.friendPerson.lastName.length() < b.friendPerson.lastName.length();
    }
    if (a.friendPerson.lastName != b.friendPerson.lastName) {
        return a.friendPerson.lastName < b.friendPerson.lastName;
    }
    if (a.friendPerson.id.length() != b.friendPerson.id.length()) {
        return a.friendPerson.id.length() < b.friendPerson.id.length();
    }
    return a.friendPerson.id < b.friendPerson.id;
}

void print(Query7Result q7) {
    cout << "---------------------------------------" << endl;
    cout << "'friendId': " << q7.friendId << ", 'friendLastName': " << q7.friendLastName << ", 'friendBirthday': " << q7.friendBirthday;
    cout << ", 'friendCreationDate': " << q7.friendCreationDate << ", 'friendGender': " << q7.friendGender << ", 'friendBrowserUsed': ";
    cout << q7.friendBrowserUsed << ", 'friendLocationIp': " << q7.friendLocationIp << ", 'friendEmails': " << q7.friendEmails;
    cout << ", 'friendLanguages': " << q7.friendLanguages << ", 'friendCityName': " << q7.friendCityName << ", 'friendUniversities': [";
    for (int i = 0; i < q7.friendUniversities.size(); i++) {
        cout << "[";
        for (int j = 0; j < q7.friendUniversities[i].size(); j++) {
            cout << q7.friendUniversities[i][j];
            if (j < q7.friendUniversities[i].size() - 1) {
                cout << ", ";
            }
        }
        cout << "]";
        if (i < q7.friendUniversities.size() - 1) {
            cout << ", ";
        }
    }
    cout << "], 'friendCompanies': [";
    for (int i = 0; i < q7.friendCompanies.size(); i++) {
        cout << "[";
        for (int j = 0; j < q7.friendCompanies[i].size(); j++) {
            cout << q7.friendCompanies[i][j];
            if (j < q7.friendCompanies[i].size() - 1) {
                cout << ", ";
            }
        }
        cout << "]";
        if (i < q7.friendCompanies.size() - 1) {
            cout << ", ";
        }
    }
    cout << "]" << endl;
}

int main() {
    auto start = std::chrono::high_resolution_clock::now();

    string file_path = "./queries/interactive-complex-1-other.txt";
    // string file_path = "./queries/try_query-1.txt";
    string nowIP = "bolt://139.9.250.196:7687/";
    // getIPList(ip_path);
    string neo4jResult, tid, tid1, tid2, oid, woid;
    vector<Query1Result> query1Results;
    vector<Query2Result> query2Results;
    vector<Query3Result> query3Results;
    vector<Query4Result> query4Results;
    vector<Query5Result> query5Results;
    vector<Query6Result> query6Results;
    vector<Query7Result> query7Results;
    vector<Query8Result> query8Results;

    getQueryList(file_path);
    for (int i = 0; i < query_list.size(); i++) {
        cout << "now_query: " << query_list[i] << endl;
        if (i == 0) {
            neo4jResult = connect2Neo4j(query_list[i], nowIP);
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
            cout << "neo4jResult:" << neo4jResult << endl;
            query2_ans_list = getAnsList(neo4jResult);
            cout << "query2_ans_list: " << query2_ans_list.size() << endl;

            tid1 = "WHERE friend.id IN [";

            ofstream fout;
            fout.open("test_qc1_time.txt");

            auto now = chrono::system_clock::now();
            auto duration_since_epoch = now.time_since_epoch();
            auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
            auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
            time_t now_time_t = chrono::system_clock::to_time_t(now);
            tm* now_tm = localtime(&now_time_t);
            fout << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

            for (int j = 0; j < query2_ans_list.size(); j++) {
                Query2Result q2;
                q2 = parseQuery2Results(query2_ans_list[j]);
                query2Results.push_back(q2);
                if (j == 0) {
                    tid1 = tid1 + q2.friendPerson.id;
                }
                else {
                    tid1 = tid1 + ", " + q2.friendPerson.id;
                }
            }

            for (int j = 0; j < query2Results.size(); j++) {
                string now_id = query2Results[j].friendPerson.id;
                if (now_id == "933") {
                    continue;
                }
                int flag = 0;
                for (int k = 0; k < query1Results.size(); k++) {
                    if (now_id == query1Results[k].friendPerson.id) {
                        flag = 1;
                        break;
                    }
                }
                if (flag == 0) {
                    Query1Result q1;
                    q1.friendPerson = query2Results[j].friendPerson;
                    q1.distance = query2Results[j].distance;
                    query1Results.push_back(q1);
                }
            }

            auto now = chrono::system_clock::now();
            auto duration_since_epoch = now.time_since_epoch();
            auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
            auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
            time_t now_time_t = chrono::system_clock::to_time_t(now);
            tm* now_tm = localtime(&now_time_t);
            fout << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

            tid1 = tid1 + "]\n";
            // cout << "tid1: " << tid1 << endl;
        }
        if (i == 2) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            string new_query = str1 + tid1 + str2;

            neo4jResult = connect2Neo4j(new_query, nowIP);
            cout << "neo4jResult:" << neo4jResult << endl;
            query3_ans_list = getAnsList(neo4jResult);
            cout << "query3_ans_list: " << query3_ans_list.size() << endl;

            for (int j = 0; j < query3_ans_list.size(); j++) {
                Query3Result q3;
                q3 = parseQuery3Results(query3_ans_list[j]);
                query3Results.push_back(q3);
            }

            for (int j = 0; j < query2Results.size(); j++) {
                string now_id = query2Results[j].friendPerson.id;
                if (now_id == "933") {
                    continue;
                }
                int flag = 0;
                for (int k = 0; k < query1Results.size(); k++) {
                    if (now_id == query1Results[k].friendPerson.id) {
                        flag = 1;
                        break;
                    }
                }
                if (flag == 0) {
                    Query1Result q1;
                    q1.friendPerson = query2Results[j].friendPerson;
                    q1.distance = query2Results[j].distance;
                    query1Results.push_back(q1);
                }
            }

            for (int j = 0; j < query3Results.size(); j++) {
                string now_id = query3Results[j].friendPerson.id;
                if (now_id == "933") {
                    continue;
                }
                int flag = 0;
                for (int k = 0; k < query1Results.size(); k++) {
                    if (now_id == query1Results[k].friendPerson.id) {
                        flag = 1;
                        break;
                    }
                }
                if (flag == 0) {
                    Query1Result q1;
                    q1.friendPerson = query3Results[j].friendPerson;
                    q1.distance = query3Results[j].distance;
                    query1Results.push_back(q1);
                }
            }

            tid2 = "WHERE friend.id IN [";

            sort(query1Results.begin(), query1Results.end(), cmp);
            for (int j = 0; j < query1Results.size(); j++) {
                if (j > 19) {
                    break;
                }
                if (j == 0) {
                    tid2 = tid2 + query1Results[j].friendPerson.id;
                }
                else {
                    tid2 = tid2 + ", " + query1Results[j].friendPerson.id;
                }
            }
            tid2 = tid2 + "]\n";
            // cout << "tid2: " << tid2 << endl;
        }

        if (i == 3) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            string new_query = str1 + tid2 + str2;
            neo4jResult = connect2Neo4j(new_query, nowIP);
            // cout << "neo4jResult: " << neo4jResult << endl;
            query4_ans_list = getAnsList(neo4jResult);
            // cout << "query4_ans_list: " << query4_ans_list.size() << endl;
            for (int j = 0; j < query4_ans_list.size(); j++) {
                Query4Result q4;
                q4 = parseQuery4Results(query4_ans_list[j]);
                // cout << "q4.id: " << q4.friendCity.id << endl; 
                query4Results.push_back(q4);
            }
        }
        if (i == 4) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);
            string new_query = str1 + tid2 + str2;
            cout << "new_query: " << new_query << endl;
            neo4jResult = connect2Neo4j(new_query, nowIP);

            cout << "neo4jResult: " << neo4jResult << endl;

            query5_ans_list = getAnsList(neo4jResult);
            cout << "query5_ans_list.size(): " << query5_ans_list.size() << endl;
            oid = "WHERE uni.id IN [";
            for (int j = 0; j < query5_ans_list.size(); j++) {
                Query5Result q5;
                q5 = parseQuery5Results(query5_ans_list[j]);
                query5Results.push_back(q5);
                if (q5.unis[0][0] == "null") {
                    continue;
                }
                else{
                    if (j == 0) {
                        oid = oid + q5.unis[0][2];
                    }
                    else {
                        oid = oid + ", " + q5.unis[0][2];
                    }
                }
            }
            oid = oid + "]\n";
            cout << "oid: " << oid << endl;
        }

        if (i == 5) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);
            string new_query = str1 + oid + str2;
            cout << "new_query: " << new_query << endl;
            neo4jResult = connect2Neo4j(new_query, nowIP);

            cout << "neo4jResult: " << neo4jResult << endl;

            query6_ans_list = getAnsList(neo4jResult);
            cout << "query6_ans_list.size(): " << query6_ans_list.size() << endl; 

            for (int j = 0; j < query6_ans_list.size(); j++) {
                Query6Result q6;
                q6 = parseQuery6Results(query6_ans_list[j]);
                query6Results.push_back(q6);
            }

            for (int j = 0; j < query5Results.size(); j++) {
                Query5Result q5 = query5Results[j];
                if (q5.unis[0][0] == "null") {
                    continue;
                }
                else {
                    for (int k = 0; k < query6Results.size(); k++) {
                        if (query6Results[k].uniId == query5Results[j].unis[0][2]) {
                            query5Results[j].unis[0][2] = query6Results[k].uniCityName;
                        }
                    }
                }
            }
            for (int a = 0; a < query5Results.size(); a++) {
                Query5Result q5 = query5Results[a];
                cout << "a: " << a << endl;
                for (int b = 0; b < q5.unis[0].size(); b++) {
                    cout << q5.unis[0][b] << " ";
                }
                cout << endl;
            }
        }

        if (i == 6) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);
            string new_query = str1 + tid2 + str2;
            cout << "new_query: " << new_query << endl;
            neo4jResult = connect2Neo4j(new_query, nowIP);
            cout << "neo4jResult: " << neo4jResult << endl;

            query7_ans_list = getAnsList(neo4jResult);
            woid = "WHERE company.id IN [";
            cout << "query7_ans_list.size(): " << query7_ans_list.size() << endl;
            for (int j = 0; j < query7_ans_list.size(); j++) {
                // cout << query4_ans_list[j] << endl;
                Query7Result q7;
                q7 = parseQuery7Results(query7_ans_list[j]);
                for (int k = 0; k < 20; k++) {
                    if (q7.friendId == query1Results[k].friendPerson.id) {
                        cout << "q7.friendId: " << q7.friendId << endl;
                        q7.friendCityName = query4Results[k].friendCity.name;
                        cout << "q7.friendCityName: " << q7.friendCityName << endl;
                        cout << "k: " << k << endl;
                        cout << "query5Result: " << query5Results.size() << endl;
                        q7.friendUniversities = query5Results[k].unis;
                        cout << "能到这" << endl;
                        break;
                    }
                }
                cout << "friendCityName: " << q7.friendCityName << endl;
                // cout << q7.company.id << endl;
                query7Results.push_back(q7);
                // cout << "friendCompanies: " << q7.friendCompanies[0][2] << endl;
                if (q7.friendCompanies[0][0] == "null") {
                    continue;
                }
                else {
                    for (int k = 0; k < q7.friendCompanies.size(); k++) {
                        if (j == 0 && k == 0) {
                            woid = woid + q7.friendCompanies[k][2];
                        }
                        else {
                            woid = woid + ", " + q7.friendCompanies[k][2];
                        }
                    }
                }
            }
            woid = woid + "]\n";
            cout << "woid: " << woid << endl;
        }
        if (i == 7) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);
            string new_query = str1 + woid + str2;
            cout << "new_query: " << new_query << endl;
            neo4jResult = connect2Neo4j(new_query, nowIP);

            cout << "neo4jResult: " << neo4jResult << endl;
            query8_ans_list = getAnsList(neo4jResult);
            
            for (int j = 0; j < query8_ans_list.size(); j++) {
                Query8Result q8;
                q8 = parseQuery8Results(query8_ans_list[j]);
                query8Results.push_back(q8);
            }
            
            cout << "query8Results.size()" << query8Results.size() << endl;

            for (int j = 0; j < query7Results.size(); j++) {
                if (query7Results[j].friendCompanies[0][0] == "null") {
                    continue;
                }
                for (int k = 0; k < query7Results[j].friendCompanies.size(); k++) {
                    // cout << "fc" << query7Results[j].friendCompanies[k][2] << endl;
                    for (int a = 0; a < query8Results.size(); a++) {
                        if (query8Results[a].companyId == query7Results[j].friendCompanies[k][2]) {
                            query7Results[j].friendCompanies[k][2] = query8Results[a].companyCountry.name;
                            break;
                        }
                    }
                }
            }
            cout << "8能执行完" << endl;
        }

        auto end = std::chrono::high_resolution_clock::now();

        // 计算所经过的时间
        std::chrono::duration<double> elapsed = end - start;

        // 输出结果
        std::cout << "Elapsed time: " << elapsed.count() << " seconds." << std::endl;
    }
    for (int i = 0; i < query7Results.size(); i++) {
        print(query7Results[i]);
    }
    // 获取结束时间
    auto end = std::chrono::high_resolution_clock::now();

    // 计算所经过的时间
    std::chrono::duration<double> elapsed = end - start;

    // 输出结果
    std::cout << "Elapsed time: " << elapsed.count() << " seconds." << std::endl;

    return 0;
}