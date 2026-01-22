#include <iostream>
#include <cstring>
#include <algorithm>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <vector>
using namespace std;

vector<string> time_list;
vector<vector<string>> file_time_list;
vector<vector<string>> servers_time_list;
ofstream fout;

std::chrono::system_clock::time_point parseTimestamp(const std::string& timeStr) {
    std::tm tm = {};
    int milliseconds;
    std::istringstream ss(timeStr);
    char dot;

    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S") >> dot >> milliseconds;
    auto time = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    return time + std::chrono::milliseconds(milliseconds);
}

int getTime(string v_t1, string v_t2) {
    int pos1 = v_t1.find(" 2024");
    string time1 = v_t1.substr(pos1 + 1);
    int pos2 = v_t2.find(" 2024");
    string time2 = v_t2.substr(pos1 + 1);

    auto timePoint1 = parseTimestamp(time1);
    auto timePoint2 = parseTimestamp(time2);

    // Calculate difference
    auto duration = chrono::duration_cast<chrono::milliseconds>(timePoint2 - timePoint1);
    return duration.count();    
}

int getTotalTime() {
    int pos1 = time_list[0].find(" 2024");
    string time1 = time_list[0].substr(pos1 + 1);
    int pos2 = time_list[time_list.size() - 1].find(" 2024");
    string time2 = time_list[time_list.size() - 1].substr(pos1 + 1);

    auto timePoint1 = parseTimestamp(time1);
    auto timePoint2 = parseTimestamp(time2);

    // Calculate difference
    auto duration = chrono::duration_cast<chrono::milliseconds>(timePoint2 - timePoint1);

    return duration.count();
}

int main() {
    string now_query = "ER_RCP_20/qc3";
    string now_file = "qc3";
    int query_cnt = 0;
    vector<string> servers = {"_122_9_162_58_7474.txt", "_116_63_178_28_7474.txt", "_122_9_132_101_7474.txt", "_139_9_233_77_7474.txt", "_122_9_144_74_7474.txt", "_116_63_188_96_7474.txt", "_139_9_246_15_7474.txt", "_116_63_183_28_7474.txt"};
    // vector<string> servers = {"_122_9_162_58_7474.txt", "_116_63_178_28_7474.txt"};
    // vector<string> servers = {"_122_9_162_58_7474.txt", "_116_63_178_28_7474.txt", "_122_9_132_101_7474.txt", "_139_9_233_77_7474.txt"};
    // vector<string> servers = {"_122_9_162_58_7474.txt", "_116_63_178_28_7474.txt", "_122_9_132_101_7474.txt", "_139_9_233_77_7474.txt", "_122_9_144_74_7474.txt", "_116_63_188_96_7474.txt"};
    // vector<string> servers = {"_139_9_250_196_7474.txt"};

    string fin1_name = "./query_time/" + now_query + "/file_time_0";
    string fout_name = "./query_time/result/" + now_query + ".txt";
    fout.open(fout_name);

    string line;
    ifstream fin1(fin1_name);
    while (getline(fin1, line)) {
        time_list.push_back(line);
    }

    for (int i = 1; i < 9; i++) {
        string now_file_name = "./query_time/" + now_query + "/file_time_" + to_string(i);
        ifstream fin(now_file_name);
        string now_line;
        vector<string> now_vector;
        while (getline(fin, now_line)) {
            now_vector.push_back(now_line);
        }
        file_time_list.push_back(now_vector);
    }

    for (int i = 0; i < 8; i++) {
        string now_file_name = "./query_time/" + now_query + "/" + now_file + servers[i];
        // cout << "now_file_name: " << now_file_name << endl;
        ifstream fin(now_file_name);
        string now_line;
        vector<string> now_vector;
        while (getline(fin, now_line)) {
            if (i == 0) {
                query_cnt++;
            }
            now_vector.push_back(now_line);
        }
        servers_time_list.push_back(now_vector);
    }

    query_cnt /= 2;
    cout << "query_cnt: " << query_cnt << endl;
    int computation_time = 0;
    int communication_time = 0;

    for (int i = 0; i < query_cnt; i++) {
        int max_query_time = 0;
        for (int j = 0; j < 8; j++) {
            // cout << i << " " << j << endl;
            // cout << servers_time_list[j][2 * i] << endl;
            // cout << servers_time_list[j][2 * i + 1] << endl;
            int now_time = getTime(servers_time_list[j][2 * i], servers_time_list[j][2 * i + 1]);
            // cout << now_time << endl;
            if (max_query_time < now_time) {
                max_query_time = now_time;
            }
            // cout << i << " " << j << endl;
        }
        computation_time += max_query_time;
        // cout << "i: " << i << " max_query_time: " << max_query_time << endl;
    }

    for (int i = 0; i < query_cnt; i++) {
        int max_communication_time = 0;
        for (int j = 0; j < 8; j++) {
            // cout << i << " " << j << endl;
            // cout << file_time_list[j][i] << endl;
            // cout << time_list[(i + 1) * 8] << endl;
            int now_time = getTime(file_time_list[j][i] , time_list[(i + 1) * 8]);
            // cout << "now_time" << now_time << endl;
            if (max_communication_time < now_time) {
                max_communication_time = now_time;
            }
        }
        // cout << "max_communication_time: " << max_communication_time << endl;
        communication_time += max_communication_time;
    }
    communication_time = communication_time - computation_time;

    int total_time = getTotalTime();
    int join_time = total_time - computation_time - communication_time;

    fout << "ComputationTime: " << computation_time << " milliseconds" << endl;
    fout << "CommunicationTime: " << communication_time << " milliseconds" << endl;
    fout << "JoinTime: " << join_time << " milliseconds" << endl;

    return 0;
}
