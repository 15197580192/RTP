#include <iostream>
#include <cmath>
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

bool cmp(Query7Result a, Query7Result b) {
    if (a.friendLastName != b.friendLastName) {
        return a.friendLastName < b.friendLastName;
    }
    return a.friendId < b.friendId;
}

bool cmp1(Query1Result a, Query1Result b) {
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

bool cmp2(Person a, Person b) {
    if (a.lastName != b.lastName) {
        return a.lastName < b.lastName;
    }
    if (a.id.length() != b.id.length()) {
        return a.id.length() < b.id.length();
    }
    return a.id < b.id;
}

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
        // cout << "到这了" << endl;
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
    Person p = parsePerson(data);
    q3.friendPerson = p;
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

int main(int argc, char** argv) {
    auto start = std::chrono::high_resolution_clock::now();

    // vector<string> queries = {"MATCH (c:Comment)\n RETURN c\n LIMIT 2"};
    vector<string> servers = {"http://122.9.162.58:7474/", "http://116.63.178.28:7474/", "http://122.9.132.101:7474/", "http://139.9.233.77:7474/", "http://122.9.144.74:7474/", "http://116.63.188.96:7474/", "http://139.9.246.15:7474/", "http://116.63.183.28:7474/"};

    string file_path = "./queries/interactive-complex-1-other.txt";
    string neo4jResult;
    string tid = " ";
    string tid1 = " ";
    string tid2 = " ";
    string oid = " ";
    string woid = " ";
    int tid_length = 0;
    int tid1_length = 0;
    int tid2_length = 0;
    int oid_length = 0;
    int woid_length = 0;
    vector<Query1Result> query1Results;
    vector<Query2Result> query2Results;
    vector<Query3Result> query3Results;
    vector<Query4Result> query4Results;
    vector<Query5Result> query5Results;
    vector<Query6Result> query6Results;
    vector<Query7Result> query7Results;
    vector<Query8Result> query8Results;

    getQueryList(file_path);

    MPI_Init(&argc, &argv);

    int world_rank, world_size;
    // world_rank = 9;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    ofstream fout;
    string outfile_name = "file_time_" + to_string(world_rank);
    fout.open(outfile_name);

    ifstream fin_id("know*1_3_id_list.txt");
    string line;
    vector<Person> person_list;
    while (getline(fin_id, line)) {
        // cout << "line: " << line << endl;
        vector<string> tlist;
        stringSplit(line, "' '", tlist);
        Person p;
        for (int i = 0; i < tlist.size(); i++) {
            if (i == 0) {
                p.id = tlist[i] + "'";
            }
            if (i == 1) {
                p.firstName = "'" + tlist[i] + "'";
            }
            if (i == 2) {
                p.lastName = "'" + tlist[i] + "'";
            }
            if (i == 3) {
                p.birthday = "'" + tlist[i] + "'";
            }
            if (i == 4) {
                // cout << tlist[i] << endl;
                p.creationDate = "'" + tlist[i];
            }
        }
        person_list.push_back(p);
        // person_list.push_back(line);
    }
    // cout << person_id_list[0] << endl;

    if (world_rank == 0) {
        auto now = chrono::system_clock::now();
        auto duration_since_epoch = now.time_since_epoch();
        auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
        auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
        time_t now_time_t = chrono::system_clock::to_time_t(now);
        tm* now_tm = localtime(&now_time_t);
        fout << 0 << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;
    }

    sort(person_list.begin(), person_list.end(), cmp2);
    tid2 = "WHERE friend.id IN [";
    for (int j = 0; j < person_list.size(); j++){
        if (j > 19) {
            break;
        }
        if (j == 0) {
            tid2 = tid2 + person_list[j].id;
        }
        else {
            tid2 = tid2 + ", " + person_list[j].id;
        }
    }
    tid2 = tid2 + "]\n";
    // cout << "tid2: " << tid2 << endl;

    for (int i = 3; i < query_list.size(); i++) {
        // cout << "当前节点: " << world_rank << endl;
        // cout << "now_query: " << query_list[i] << endl;
        if (i == 0) {
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

                // cout << "query1_ans_list: " << query1_ans_list.size() << endl;

                tid = "WHERE friend.id IN [";

                for (int j = 0; j < query1_ans_list.size(); j++) {
                    Query1Result q1;
                    q1 = parseQuery1Results(query1_ans_list[j]);
                    q1.distance = 1;
                    query1Results.push_back(q1);
                    if (j == 0) {
                        tid = tid + q1.friendPerson.id;
                    }
                    else {
                        tid = tid + ", " + q1.friendPerson.id;
                    }
                }
                tid = tid + "]\n";
                // tid = "WHERE c.id IN [1, 2]\n";
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
                // string result = connect2Neo4j(try_query, servers[world_rank - 1]);
                // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
                MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
            }
            else {
                string new_query = str1 + tid + str2;
                // cout << "new_query: " << new_query << endl;
                // cout << "tid: " << tid << endl;

                vector<string> results(world_size - 1);
                // vector<string> results_1(8);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[800000];
                    MPI_Recv(buffer, 800000, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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

                tid1 = "WHERE friend.id IN [";

                for (int j = 0; j < query2_ans_list.size(); j++) {
                    Query2Result q2;
                    q2 = parseQuery2Results(query2_ans_list[j]);
                    q2.distance = 2;
                    query2Results.push_back(q2);
                    if (j == 0) {
                        tid1 = tid1 + q2.friendPerson.id;
                    }
                    else {
                        tid1 = tid1 + ", " + q2.friendPerson.id;
                    }
                }
                tid1 = tid1 + "]\n";
                // cout << "tid1: " << tid1 << endl;

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
        if (i == 2) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            MPI_Bcast(&tid1_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            // cout << "tid1_length: " << tid1_length << endl;
            if (world_rank != 0) {
                // cout << "这不是0号节点" << endl;
                tid1.resize(tid1_length);
            }
            MPI_Bcast(&tid1[0], tid1_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            vector<string> tlist;
            pos = tid1.find("[");
            string ttid = tid1.substr(pos + 1);
            ttid = ttid.substr(0, ttid.length() - 2);
            // cout << "ttid: " << ttid << endl;
            stringSplit(ttid, ", ", tlist);
            // cout << tlist[0] << endl;
            // cout << tlist[tlist.size() - 1] << endl;
            int epoch = (tlist.size() / 400) + 1;

            for (int t = 0; t < epoch; t++) {
                query3_ans_list.clear();
                query3Results.clear();
                string now_tid = "WHERE friend.id IN [";
                for (int a = t * 400; a < min((t + 1) * 400, int(tlist.size())); a++) {
                    if (a % 400 == 0) {
                        now_tid = now_tid + tlist[a];
                    }
                    else {
                        now_tid = now_tid + ", " + tlist[a];
                    }
                }
                now_tid = now_tid + "]\n";
                // cout << "now_tid: " << now_tid << endl;
                

                if (world_rank != 0) {
                    string new_query = str1 + now_tid + str2;

                    auto now = chrono::system_clock::now();
                    auto duration_since_epoch = now.time_since_epoch();
                    auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                    auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                    time_t now_time_t = chrono::system_clock::to_time_t(now);
                    tm* now_tm = localtime(&now_time_t);
                    fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                    string result = connect2Neo4j(new_query, servers[world_rank - 1]);
                    // string result = connect2Neo4j(try_query, servers[world_rank - 1]);
                    // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
                    MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
                }
                else {
                    string new_query = str1 + now_tid + str2;
                    // cout << "new_query: " << new_query << endl;
                    // cout << "tid: " << tid << endl;

                    vector<string> results(world_size - 1);
                    // vector<string> results_1(8);
                    for (int j = 1; j < world_size; ++j) {
                        char buffer[8250000];
                        MPI_Recv(buffer, 8250000, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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

                    for (int j = 0; j < query3_ans_list.size(); j++) {
                        Query3Result q3;
                        q3 = parseQuery3Results(query3_ans_list[j]);
                        q3.distance = 3;
                        query3Results.push_back(q3);
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
                }

                MPI_Barrier(MPI_COMM_WORLD);

            }

            if (world_rank == 0) {
                tid2 = "WHERE friend.id IN [";

                vector<string> tempList;
                sort(query1Results.begin(), query1Results.end(), cmp1);
                for (int j = 0; j < query1Results.size(); j++) {
                    if (tempList.size() > 19) {
                        break;
                    }
                    for (int t = 0; t < tempList.size(); t++) {
                        if (query1Results[j].friendPerson.id == tempList[t]) {
                            continue;
                        }
                    }
                    tempList.push_back(query1Results[j].friendPerson.id);
                    if (j == 0) {
                        tid2 = tid2 + query1Results[j].friendPerson.id;
                    }
                    else {
                        tid2 = tid2 + ", " + query1Results[j].friendPerson.id;
                    }
                }
                tid2 = tid2 + "]\n";
            }

            tid2_length = tid2.size();
            // cout << "节点0的tid2_length: " << tid2_length << endl;
            MPI_Bcast(&tid2_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            tid2.resize(tid2_length);
            MPI_Bcast(&tid2[0], tid2_length, MPI_CHAR, 0, MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
        }
        if (i == 3) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            if (world_rank != 0) {
                string new_query = str1 + tid2 + str2;

                auto now = chrono::system_clock::now();
                auto duration_since_epoch = now.time_since_epoch();
                auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                time_t now_time_t = chrono::system_clock::to_time_t(now);
                tm* now_tm = localtime(&now_time_t);
                fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                string result = connect2Neo4j(new_query, servers[world_rank - 1]);
                // string result = connect2Neo4j(try_query, servers[world_rank - 1]);
                // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
                MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
            }
            else {
                string new_query = str1 + tid2 + str2;
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
                            query4_ans_list.push_back(now_ans[k]);
                        }
                    }
                }

                // cout << "query4_ans_list: " << query4_ans_list.size() << endl;

                for (int j = 0; j < query4_ans_list.size(); j++) {
                    Query4Result q4;
                    q4 = parseQuery4Results(query4_ans_list[j]);
                    // cout << "q4.id: " << q4.friendCity.id << endl;
                    query4Results.push_back(q4);
                }
            }

            // cout << "节点 " << world_rank << " 到达i1同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i1同步墙" << endl;
        }
        if (i == 4) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);
            string new_query = str1 + tid2 + str2;

            // cout <<  "当前节点 " << world_rank << " 的new_query为: " << new_query << endl;

            if (world_rank != 0) {
                // cout <<  "当前节点 " << world_rank << " 的new_query为: " << new_query << endl;
                auto now = chrono::system_clock::now();
                auto duration_since_epoch = now.time_since_epoch();
                auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                time_t now_time_t = chrono::system_clock::to_time_t(now);
                tm* now_tm = localtime(&now_time_t);
                fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                string result = connect2Neo4j(new_query, servers[world_rank - 1]);
                // string result = connect2Neo4j(try_query, servers[world_rank - 1]);
                // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
                MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
            }
            else {
                // cout << "节点: " << world_rank << endl;
                vector<string> results(world_size - 1);
                for (int j = 1; j < world_size; ++j) {
                    char buffer[800000];
                    MPI_Recv(buffer, 800000, MPI_CHAR, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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

                cout << "query5_ans_list: " << query5_ans_list.size() << endl;

                oid = "WHERE uni.id IN [";
                vector<Query5Result> tq5;
                int flag = 0;
                for (int j = 0; j < query5_ans_list.size(); j++) {
                    Query5Result q5;
                    q5 = parseQuery5Results(query5_ans_list[j]);
                    tq5.push_back(q5);
                }

                for (int j = 0; j < 20; j++) {
                    // cout << "j: " << j << " " << query1Results[j].friendPerson.id << endl;
                    Query5Result q5;
                    vector<vector<string>> v;
                    vector<string> t_v;
                    t_v.push_back("null");
                    v.push_back(t_v);
                    q5.unis = v;
                    q5.fid = person_list[j].id;
                    for (int k = 0; k < tq5.size(); k++) {
                        if (tq5[k].fid == q5.fid && tq5[k].unis[0][0] != "null") {
                            q5.unis = tq5[k].unis;
                            break;
                        }
                    }
                    query5Results.push_back(q5);
                    if (q5.unis[0][0] == "null") {
                        continue;
                    }
                    else{
                        if (flag == 0) {
                            oid = oid + q5.unis[0][2];
                            flag = 1;
                        }
                        else {
                            oid = oid + ", " + q5.unis[0][2];
                        }
                    }
                }
                oid = oid + "]\n";
                // cout << "oid: " << oid << endl;

                // oid_length = oid.size();
                // cout << "节点 " << world_rank << " 的oid_length: " << oid_length << endl;
                // MPI_Bcast(&oid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
                // MPI_Bcast(&oid[0], oid_length, MPI_CHAR, 0, MPI_COMM_WORLD);
            }
            // cout << "赋值前: " << world_rank << "length: " << oid_length << endl;
            oid_length = oid.size();
            // cout << "节点 " << world_rank << " 的oid_length: " << oid_length << endl;
            MPI_Bcast(&oid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);

            oid.resize(oid_length);

            // cout << "节点 " << world_rank << " 的oid_length: " << oid_length << endl;
            char *buffer = new char[oid_length];
            if (world_rank == 0) {
                strcpy(buffer, oid.c_str());
                // cout << "给Buffer赋值完毕";
            }
            MPI_Bcast(buffer, oid_length, MPI_CHAR, 0, MPI_COMM_WORLD);
            if (world_rank != 0) {
                oid = string(buffer);
            }
            // cout << "Process " << world_rank << " oid: " << oid << endl;

            // cout << "节点 " << world_rank << " 到达i2同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i2同步墙" << endl;
        }
        if (i == 5) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);
            
            MPI_Bcast(&oid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            // cout << "oid_length: " << oid_length << endl;
            if (world_rank != 0) {
                // cout << "这不是0号节点" << endl;
                // 调整字符串大小以适应接收的数据
                oid.resize(oid_length);
            }
            MPI_Bcast(&oid[0], oid_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            if (world_rank != 0) {
                // cout << "通信完后：" << oid.size() << endl;
                string new_query = str1 + oid + str2;

                auto now = chrono::system_clock::now();
                auto duration_since_epoch = now.time_since_epoch();
                auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                time_t now_time_t = chrono::system_clock::to_time_t(now);
                tm* now_tm = localtime(&now_time_t);
                fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                // cout <<  "当前节点 " << world_rank << " 的new_query为: " << new_query << endl;
                string result = connect2Neo4j(new_query, servers[world_rank - 1]);
                // string result = connect2Neo4j(try_query, servers[world_rank - 1]);
                // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
                MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
            }
            else {
                // cout << "节点: " << world_rank << endl;
                
                vector<string> results(world_size - 1);
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
            }
            // cout << "节点 " << world_rank << " 到达i3同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i3同步墙" << endl;
        }

        if (i == 6) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            if (world_rank != 0) {
                // cout << "节点: " << world_rank << endl;
                string new_query = str1 + tid2 + str2;

                auto now = chrono::system_clock::now();
                auto duration_since_epoch = now.time_since_epoch();
                auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                time_t now_time_t = chrono::system_clock::to_time_t(now);
                tm* now_tm = localtime(&now_time_t);
                fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                // cout << "节点 " << world_rank << endl;
                // cout << "new_query: " << new_query << endl;
                // cout << "tid: " << tid << endl;
                string result = connect2Neo4j(new_query, servers[world_rank - 1]);
                // string result = connect2Neo4j(try_query, servers[world_rank - 1]);
                // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
                MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
            }
            else {
                // cout << "节点: " << world_rank << endl;
                // string new_query = str1 + tid + str2;
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
                            query7_ans_list.push_back(now_ans[k]);
                        }
                    }
                }

                // cout << "query7_ans_list.size(): " << query7_ans_list.size() << endl;
                woid = "WHERE company.id IN [";
                int flag = 0;
                for (int j = 0; j < query7_ans_list.size(); j++) {
                    // cout << query4_ans_list[j] << endl;
                    Query7Result q7;
                    q7 = parseQuery7Results(query7_ans_list[j]);
                    for (int k = 0; k < 20; k++) {
                        if (q7.friendId == person_list[k].id) {
                            // cout << "q7.friendId: " << q7.friendId << endl;
                            q7.friendCityName = query4Results[k].friendCity.name;
                            // cout << "q5.friendCityName: " << q5.friendCityName << endl;
                            // cout << "k: " << k << endl;
                            // cout << "query3Result: " << query3Results.size() << endl;
                            q7.friendUniversities = query5Results[k].unis;
                            // cout << "能到这" << endl;
                            break;
                        }
                    }
                    // cout << "friendCityName: " << q5.friendCityName << endl;
                    // cout << q5.company.id << endl;
                    query7Results.push_back(q7);
                    // cout << "friendCompanies: " << q5.friendCompanies[0][2] << endl;
                }
                int q7size = query7Results.size();
                // cout << "q5size: " << q5size << endl;
                sort(query7Results.begin(), query7Results.end(), cmp);
                for (int j = 0; j < query7Results.size(); j++) {
                    if (j > 19) {
                        break;
                    }
                    Query7Result q7 = query7Results[j];
                    if (q7.friendCompanies[0][0] == "null") {
                        continue;
                    }
                    else {
                        for (int k = 0; k < q7.friendCompanies.size(); k++) {
                            if (flag == 0 && k == 0) {
                                woid = woid + q7.friendCompanies[k][2];
                                flag = 1;
                            }
                            else {
                                woid = woid + ", " + q7.friendCompanies[k][2];
                            }
                        }
                    }
                }
                woid = woid + "]\n";
                // cout << "woid: " << woid << endl;
            }
            woid_length = woid.size();
            // cout << "节点0的woid_length: " << woid_length << endl;
            MPI_Bcast(&woid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            woid.resize(woid_length);
            MPI_Bcast(&woid[0], woid_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            // cout << "节点 " << world_rank << " 到达i4同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i4同步墙" << endl;
        }
        if (i == 7) {
            int pos = query_list[i].find('\n');
            string str1 = query_list[i].substr(0, pos + 1);
            string str2 = query_list[i].substr(pos + 1);

            MPI_Bcast(&woid_length, 1, MPI_INT, 0, MPI_COMM_WORLD);
            // cout << "woid_length: " << woid_length << endl;
            if (world_rank != 0) {
                // cout << "这不是0号节点" << endl;
                // 调整字符串大小以适应接收的数据
                woid.resize(woid_length);
            }
            MPI_Bcast(&woid[0], woid_length, MPI_CHAR, 0, MPI_COMM_WORLD);

            if (world_rank != 0) {
                // cout << "节点: " << world_rank << endl;
                string new_query = str1 + woid + str2;

                auto now = chrono::system_clock::now();
                auto duration_since_epoch = now.time_since_epoch();
                auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                time_t now_time_t = chrono::system_clock::to_time_t(now);
                tm* now_tm = localtime(&now_time_t);
                fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                // cout << "节点 " << world_rank << endl;
                // cout << "new_query: " << new_query << endl;
                // cout << "woid: " << woid << endl;
                string result = connect2Neo4j(new_query, servers[world_rank - 1]);
                // string result = connect2Neo4j(try_query, servers[world_rank - 1]);
                // cout << "节点 " << world_rank << " 向服务器ip " << servers[world_rank - 1] << " 发送信息" << endl;
                MPI_Send(result.c_str(), result.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
            }
            else {
                // cout << "节点: " << world_rank << endl;
                // string new_query = str1 + woid + str2;
                // cout << "节点 " << world_rank << endl;
                // cout << "new_query: " << new_query << endl;
                // cout << "woid: " << woid << endl;

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
                            query8_ans_list.push_back(now_ans[k]);
                        }
                    }
                }

                // cout << "query8_ans_list.size(): " << query8_ans_list.size() << endl;
                
                for (int j = 0; j < query8_ans_list.size(); j++) {
                    Query8Result q8;
                    q8 = parseQuery8Results(query8_ans_list[j]);
                    query8Results.push_back(q8);
                }
                
                // cout << "query6Results.size()" << query6Results.size() << endl;

                for (int j = 0; j < query7Results.size(); j++) {
                    if (j > 19) {
                        break;
                    }
                    if (query7Results[j].friendCompanies[0][0] == "null") {
                        continue;
                    }
                    for (int k = 0; k < query7Results[j].friendCompanies.size(); k++) {
                        // cout << "fc" << query5Results[j].friendCompanies[k][2] << endl;
                        for (int a = 0; a < query8Results.size(); a++) {
                            if (query8Results[a].companyId == query7Results[j].friendCompanies[k][2]) {
                                query7Results[j].friendCompanies[k][2] = query8Results[a].companyCountry.name;
                                break;
                            }
                        }
                    }
                }
                // cout << "8能执行完" << endl;

                auto now = chrono::system_clock::now();
                auto duration_since_epoch = now.time_since_epoch();
                auto seconds = chrono::duration_cast<chrono::seconds>(duration_since_epoch);
                auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration_since_epoch - seconds);
                time_t now_time_t = chrono::system_clock::to_time_t(now);
                tm* now_tm = localtime(&now_time_t);
                fout << i << " " << world_rank << " " << put_time(now_tm, "%Y-%m-%d %H:%M:%S") << '.' << setfill('0') << setw(3) << milliseconds.count() << endl;

                for (int i = 0; i < query7Results.size(); i++) {
                    print(query7Results[i]);
                }                
            }
            // cout << "节点 " << world_rank << " 到达i5同步墙" << endl;
            MPI_Barrier(MPI_COMM_WORLD);
            // cout << "节点 " << world_rank << " 离开i5同步墙" << endl;
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