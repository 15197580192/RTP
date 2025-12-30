// #pragma GCC optimize(3)
#include <iostream>
#include <algorithm>
#include <queue>
#include <fstream>
#include <sstream>
#include <chrono>
#include <dirent.h>
#include <climits>
#include <cstdlib> 
#include <ctime>
using namespace std;
typedef pair<int, int> PII;

using ll = long long;

#include <algorithm>  // 用于 std::sort、std::set_intersection、std::unique
#include <iterator>   // 用于 std::back_inserter

struct Con_Com {
	int bh, cnt;// 编号，cost
} cc[81477047];
// } cc[795784];

struct Region {
    int bh;
    long long cost;
    long long si;// 阈值
    long long delta;
    long long b;// benefit
} re[12];
// int partition_element[11][33001];
vector<vector<ll>> partition_component;
vector<vector<ll>> partition_node_component;
vector<vector<ll>> id_list;
ll partition_number_cnt[8][29987835];
int partition_bool[8][29987835];
int partition_cnt = 8;
int alpha=1;

// int new_property_num[] = {4, 3, 3, 4, 3, 6, 10, 8};
int new_property_num[] = {8, 3, 8, 6, 3, 3, 4, 4};//todo

ll judge_label(ll node_id) {
	if (node_id >= 0 && node_id <= 65644) {
		return 0;
	}
	if (node_id >= 65645 && node_id <= 661097) {
		return 1;
	}
	if (node_id >= 661098 && node_id <= 8096793) {
		return 2;
	}
	if (node_id >= 8096794 && node_id <= 29962268) {
		return 3;
	}
	if (node_id >= 29962269 && node_id <= 29978348) {
		return 4;
	}
	if (node_id >= 29978349 && node_id <= 29978419) {
		return 5;
	}
	if (node_id >= 29978420 && node_id <= 29979879) {
		return 6;
	}
	if (node_id >= 29979880&& node_id <= 29987834) {
		return 7;
	}
	return 0;
}
// ll judge_label(ll node_id) {
// 	if (node_id >= 0 && node_id <= 1459) {
// 		return 0;
// 	}
// 	if (node_id >= 1460 && node_id <= 15209) {
// 		return 1;
// 	}
// 	if (node_id >= 15210 && node_id <= 15280) {
// 		return 2;
// 	}
// 	if (node_id >= 15281 && node_id <= 23235) {
// 		return 3;
// 	}
// 	if (node_id >= 23236 && node_id <= 39315) {
// 		return 4;
// 	}
// 	if (node_id >= 39316 && node_id <= 190358) {
// 		return 5;
// 	}
// 	if (node_id >= 190359 && node_id <= 191886) {
// 		return 6;
// 	}
// 	if (node_id >= 191887&& node_id <= 327587) {
// 		return 7;
// 	}
// 	return 0;
// }

bool cmp(Con_Com a, Con_Com b) {
	return a.cnt > b.cnt;
}

int cmp1(Region a, Region b) {
    if (a.si != b.si) {
        return a.si > b.si;
    }
    else{
        if (a.cost != b.cost) {
            return a.cost < b.cost;
        }
    }
    return a.bh < b.bh;
}

int cmp2(Region a, Region b) {
    return a.bh < b.bh;
}

void Stringsplit(string str, const char split,vector<string>& res)
{
	istringstream iss(str);
	string token;
	while (getline(iss, token, split))
	{
		res.push_back(token);
	}
}

void get_need_file(const std::string& path, std::vector<std::string>& paths, const std::string& extension) {
    DIR* dir;
    struct dirent* entry;

	// cout<<"----finding in path: "<<path<<" to find extension: "<<extension<<"----"<<endl;

    if ((dir = opendir(path.c_str())) != nullptr) {
        while ((entry = readdir(dir)) != nullptr) {
            std::string filename = entry->d_name;
            if (filename == "." || filename == "..") {
                continue;
            }

            std::string full_path = path + "/" + filename;
            if (entry->d_type == DT_DIR) {
                get_need_file(full_path, paths, extension);
            } else if (entry->d_type == DT_REG) {
                if (filename.substr(filename.find_last_of(".") + 1) == extension) {
                    paths.push_back(full_path);
                }
            }
        }
        closedir(dir);
    }
}

/**
 * @brief 统计单个文件的行数
 * @param filePath 文件完整路径
 * @return 文件总行数（失败返回-1）
 */
int getFileLineCount(const std::string& filePath) {
    std::ifstream file(filePath, std::ios::in);
    if (!file.is_open()) {
        std::cerr << "无法打开文件：" << filePath << std::endl;
        return -1;
    }

    int lineCount = 0;
    std::string line;
    // 逐行读取，每读取一行行数+1（兼容\n、\r\n换行格式）
    while (std::getline(file, line)) {
        lineCount++;
    }

    file.close();
    return lineCount;
}
/**
 * @brief 获取边类型的属性个数
 * 
 * @param file_name 
 * @return long long 
 */
long long judgeEdgeCost(string file_name){
    if(file_name.find("hasMember")!=string::npos||file_name.find("knows")!=string::npos||file_name.find("likes")!=string::npos||file_name.find("studyAt")!=string::npos||file_name.find("workAt")!=string::npos){
        return 2;
    }
    else{
        return 1;
    }
}
/**
 * @brief Get Origin Graph Cost
 * 
 * @param nodedir 
 * @param edgedir 
 * @return long long 
 */
long long getGraphCost(string nodedir,string edgedir){
    long long cnt=0;
    ifstream fnode(nodedir);
    string node_line;
    int node_cnt = 0;
    while(getline(fnode, node_line)){
        cnt+=new_property_num[judge_label(node_cnt)];
        node_cnt++;
    }
    fnode.close();
    vector<string> edge_file;
	string need_extension = "txt";
	get_need_file(edgedir, edge_file, need_extension);
    for(auto i:edge_file){
        cnt+=getFileLineCount(i)*judgeEdgeCost(i);
    }
    return cnt;
}

int main() {
    // 原图的cost
    ll graph_cost=getGraphCost("./output/new_SF10/new_output_nodes.txt","./output/new_SF10/new_output_relationships");
    cout<<"graph cost:"<<graph_cost<<endl;

    auto start = std::chrono::high_resolution_clock::now();
	ifstream fin("./output/new_SF10/connected_size_all_v4.txt");
	int conn_cnt = 0, bh, num, tcnt;
    string lname;
	// fin >> conn_cnt;
    
	int k = 0;
    long long total_cost = 0;// 所有cc的总属性
	while (fin >> bh >> num >> lname) {
		cc[k].bh = bh;
		cc[k].cnt = num;
        total_cost += num;
		k++;
        conn_cnt++;
	}
    long long threshold = total_cost / partition_cnt;
	sort(cc, cc + conn_cnt, cmp);// 按cost倒序排序
    fin.close();
    cout<<"cc total cost:"<<total_cost<<endl;

    ll cap = alpha * (graph_cost / partition_cnt);// 初始阈值
    ll cap0=cap;

    ifstream fin1("./output/new_SF10/region_component_v4.txt");
	string line;
	int pcnt = 0;
    vector<ll> tini;
    tini.push_back(0L);
    id_list.push_back(tini);
	while (getline(fin1, line)) {
		vector<ll> l_id;
		int pos = line.find(":");
		string tline = line.substr(pos + 1);
        	pcnt++;
        	int now_loop = 0;

        	vector<string> strList;
        	Stringsplit(tline, ',', strList);
        	for (int i = 0; i < strList.size(); i++) {
            		ll tn = stoll(strList[i]);
            		l_id.push_back(tn);
        	}
		id_list.push_back(l_id);
	}
    // 分区初始化
    for (int i = 0; i < partition_cnt; i++) {
        re[i].bh = i;
        re[i].cost = 0;
        re[i].delta=0;
        re[i].si = cap;
        re[i].b=0;
    }
    string origin_str = "";
    for (int i = 0; i < partition_cnt; i++) {
        for (int j = 0; j < 29987835; j++) {
            partition_number_cnt[i][j] = 0;
            partition_bool[i][j] = 0;
        }
    }

    ofstream fout, fout1, fout2;

    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double, std::milli> duration = end - start;

    start = std::chrono::high_resolution_clock::now();
	tcnt = conn_cnt;
    vector<ll> t_node_ini;
    t_node_ini.push_back(-1);
    for (int i = 0; i < partition_cnt; i++) {
        partition_node_component.push_back(t_node_ini);
    }
    long long eth=0;// 放缩因子
    // 将n个最大的LTCs种子到n个不同的分区
    for (int i = 0; i < partition_cnt; i++) {
        for (int k = 0; k < id_list[cc[i].bh].size(); k++){
            int node_id = id_list[cc[i].bh][k];
            partition_node_component[i].push_back(node_id);
            partition_bool[i][node_id] = 1;
            partition_number_cnt[i][node_id]++;
        }
        re[i].cost += cc[i].cnt;
        vector<ll> tb;
        tb.push_back(cc[i].bh);
        partition_component.push_back(tb);
        // 更新阈值
        re[i].delta=cc[i].cnt;
        if(re[i].delta>re[i].si){
            eth=re[i].delta-re[i].si;
            cap=cap+eth;
            for (int ii = 0; ii < partition_cnt; ii++) {
                re[ii].si+=eth;
            }
        }
        re[i].si-=re[i].delta;
    }
    cout<<"seed done"<<endl;
    srand((unsigned int)time(0));
    for (int i = partition_cnt; i < conn_cnt; i++) { // for each cc
        // region相似度
        for (int j = 0; j < partition_cnt; j++) {
            ll tsum = 0;
            // cout << "size_of_id_list:" << id_list[cc[i].bh].size() << endl;
            for (int k = 0; k < id_list[cc[i].bh].size(); k++){
                ll node_id = id_list[cc[i].bh][k];
                ll tn = partition_number_cnt[j][node_id];
                tsum += tn*new_property_num[judge_label(node_id)];
            }
            re[j].b = tsum;
        }
        if(i%10000==0) {
            cout<<"placed "<<i/10000<<" *10000 cc"<<endl;
        }
        // find best
        int best=-1;
        ll maxBenefit=LLONG_MIN;
        eth=LLONG_MAX;
        sort(re, re + partition_cnt, cmp1);// si倒序，cost升序
        for (int j = 0; j < partition_cnt; j++) {
            re[j].delta=cc[i].cnt-re[j].b;
            if(re[j].delta<=re[j].si){
                if(re[j].b>maxBenefit){
                    maxBenefit=re[j].b;
                    best=j;
                }
            } else {
                eth=std::min(eth,re[j].delta-re[j].si);
            }
        }
        if(best==-1){
            cap=cap+eth;
            for(int t=0;t<partition_cnt;t++){
                re[t].si+=eth;
            }
            for(int j=0;j<partition_cnt;j++){
                if(re[j].delta<=re[j].si&&re[j].b>maxBenefit){
                    maxBenefit=re[j].b;
                    best=j;
                }
            }
        }
        // place
        for (int k = 0; k < id_list[cc[i].bh].size(); k++){
            ll node_id = id_list[cc[i].bh][k];
            if (partition_bool[re[best].bh][node_id] == 0) {
                partition_node_component[re[best].bh].push_back(node_id);
                partition_bool[re[best].bh][node_id] = 1;
            }
            else {
                int temp = new_property_num[judge_label(node_id)];
                re[best].cost -= temp;
            }
            partition_number_cnt[best][node_id]++;
        }
        re[best].cost += cc[i].cnt;
        re[best].si-=re[best].delta;
        partition_component[re[best].bh].push_back(cc[i].bh);
    }
    cout<<"alpha:"<<double(1.0*cap/cap0)<<" "<<cap<<" "<<cap0<<endl;

    end = std::chrono::high_resolution_clock::now();

    duration = end - start;
    long long res_cost=0;
    start = std::chrono::high_resolution_clock::now();
    string fout_name = "./output/new_SF10/partition_result_all_" + to_string(partition_cnt);
    fout_name = fout_name + "_v2.txt";
    string fout1_name = "./output/new_SF10/region_node_component_" + to_string(partition_cnt);
    fout1_name = fout1_name + "_v2.txt";
	fout.open(fout_name);
    fout1.open(fout1_name);
    sort(re, re + partition_cnt, cmp2);
    for (int i = 0; i < partition_cnt; i++){
        fout << "partition_id: " << i << endl;
		fout << "partition_size: " << re[i].cost << endl;
		fout << "partition_element: ";
        for (int j = 0; j < partition_component[i].size(); j++) {
            fout << partition_component[i][j] << ", ";
        }
        fout << endl;
        res_cost+=re[i].cost;
    }
    for (int i = 0; i < partition_cnt; i++) {
        fout1 << "partition_id: " << i << endl;
        fout1 << "partition_size: " << partition_node_component[i].size() - 1 << endl;
        fout1 << "partition_node_element: ";
        for (int j = 1; j < partition_node_component[i].size(); j++) {
            fout1 << partition_node_component[i][j] << ",";
        }
        fout1 << endl;
    }
    cout<<"partition cost: "<<res_cost<<endl;

    end = std::chrono::high_resolution_clock::now();

    duration = end - start;

    cout << "Time taken by the code: " << duration.count() << " ms" << endl;

	return 0;
}
