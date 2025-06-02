#define _CRT_SECURE_NO_WARNINGS
#include <iostream>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include <sys/io.h>
#include <dirent.h>
#include <chrono>

using namespace std;

// 节点类
class Node {
public:
	Node(){}
    Node(long long& id, const std::string& label)
        : id_(id), label_(label){}

    // 获取节点ID
    long long getId() { return id_; }
    
    // 获取节点标签
    std::string getLabel() const { return label_; }
    
    // 打印节点信息
    void printInfo() const {
        std::cout << "Node ID: " << id_ << ", Label: " << label_;
        std::cout << std::endl;
    }

private:
    long long id_;        // 节点ID
    std::string label_;     // 节点标签
};

// 边类
class Edge {
public:
	Edge(){}
    Edge(long long& id, const std::string& label, 
         long long& fromId, long long& toId)
        : id_(id), label_(label), fromId_(fromId), toId_(toId){}

    // 获取边ID
    long long getId() const { return id_; }
    
    // 获取边标签
    std::string getLabel() const { return label_; }
    
    // 获取起点ID
    long long getFromId() const { return fromId_; }
    
    // 获取终点ID
    long long getToId() const { return toId_; }
    
    // 打印边信息
    void printInfo() const {
        std::cout << "Edge ID: " << id_ << ", Label: " << label_;
        std::cout << ", From: " << fromId_ << ", To: " << toId_;
        std::cout << std::endl;
    }

private:
    long long id_;        // 边ID
    std::string label_;     // 边标签
    long long fromId_;    // 起点ID
    long long toId_;      // 终点ID
};

map<long long,Edge> id2edge;

class cc{
private:
	long long id_;
	set<long long> nodes_;
	set<long long> edges_;
public:
	cc(){}
	cc(long long id, set<long long>& nodes, set<long long>& edges)
		: id_(id), nodes_(nodes), edges_(edges) {}

	long long getId() const { return id_; }
	set<long long> getNodes() const { return nodes_; }
	set<long long> getEdges() const { return edges_; }

	void printInfo() const {
		cout << "CC ID: " << id_ << ", Nodes: ";
		for (const auto& node : nodes_) {
			cout << node << " ";
		}
		cout << ", Edges: ";
		for (const auto& edge : edges_) {
			cout << edge << " ";
		}
		cout << endl;
	}

};

vector<int>parent;
vector<int>rank1;
ofstream fout;//./output/new_SF0.1/connected_size_all_v4.txt
ofstream fout1;//"./output/new_SF0.1/region_component_v4.txt
ofstream fout2;//./output/new_SF0.1/done_file_name_v4.txt
ofstream fout3;//./output/new_SF0.1/connected_label_v4.txt
typedef pair<int, vector<int> > PII;

int node_cnt[327588];//点的出度
int node_in_cnt[327588];//点的入度

int ccnt = 1;// cc编号

int new_property_num[] = {4, 3, 3, 4, 3, 6, 10, 8};

int judge_label(int node_id) {
	if (node_id >= 0 && node_id <= 1459) {
		return 0;
	}
	if (node_id >= 1460 && node_id <= 15209) {
		return 1;
	}
	if (node_id >= 15210 && node_id <= 15280) {
		return 2;
	}
	if (node_id >= 15281 && node_id <= 23235) {
		return 3;
	}
	if (node_id >= 23236 && node_id <= 39315) {
		return 4;
	}
	if (node_id >= 39316 && node_id <= 190358) {
		return 5;
	}
	if (node_id >= 190359 && node_id <= 191886) {
		return 6;
	}
	if (node_id >= 191887&& node_id <= 327587) {
		return 7;
	}
	return 0;
}

void get_need_file(const std::string& path, std::vector<std::string>& paths, const std::string& extension) {
    DIR* dir;
    struct dirent* entry;

	cout<<"----finding in path: "<<path<<"to find extension: "<<extension<<"----"<<endl;

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

void DisJointSetUnion(int n) {
	for (int i = 0; i < n; i++) {
		parent.push_back(i);
		rank1.push_back(1);
	}
}

int find(int x) {
	if (x == parent[x])
		return x;
	else {
		parent[x] = find(parent[x]);
		return parent[x];
	}
}

void Union(int x, int y) {
	int rx = find(x);
	int ry = find(y);

	if (rx != ry) {
		if (rank1[rx] < rank1[ry]) {
			swap(rx, ry);
		}

		parent[ry] = rx;

		if (rank1[rx] == rank1[ry])
			rank1[rx] += 1;
	}
}


void stringSplit1(const string &source, const string &delimiter, vector<string> &vect) {
	string::size_type pos1;
	string::size_type pos2;

	pos2 = source.find(delimiter);
	pos1 = 0;
	while (string::npos != pos2) {
		vect.push_back(source.substr(pos1, pos2 - pos1));

		pos1 = pos2 + delimiter.size();
		pos2 = source.find(delimiter, pos1);
	}
	if (pos1 != source.length()) {
		vect.push_back(source.substr(pos1));
	}
}

long long readline(string strPath, long long &line) {
	ifstream is(strPath);
	char c;
	while (is.get(c)) {
		if (c == '\n')
			line++;
	}
	is.close();
	return line;
}

string getFileName(string origin_name) {
    string tline = origin_name;

	int pos = tline.find("relationships");
	tline = tline.substr(pos + 14);

    pos = tline.find(".");
    tline = tline.substr(0, pos);
    return tline;
}
/**
 * @brief 获取以node_id为起点的边的总属性数目
 * 
 * @param node_id 
 * @param file_name 
 * @return int 
 */
int getRelationshipsCost(int node_id, string file_name) {
	int res=0;
    if (file_name.find("hasMember")!=string::npos)
	{
        res+=2 * node_cnt[node_id];
    }
	if(file_name.find("knows")!=string::npos)
	{
		res+=2 * node_cnt[node_id];
	}
	if(file_name.find("likes")!=string::npos)
	{
		res+=2 * node_cnt[node_id];
	}
	if(file_name.find("studyAt")!=string::npos)
	{
		res+=2 * node_cnt[node_id];
	}
	if(file_name.find("workAt")!=string::npos)
	{
		res+=2 * node_cnt[node_id];
	}
	if(file_name.find("hasMember")==string::npos&&file_name.find("knows")==string::npos&&file_name.find("likes")==string::npos&&file_name.find("studyAt")==string::npos&&file_name.find("workAt")==string::npos){
        return node_cnt[node_id];
    }
	else return res;
}

/**
 * @brief dfs遍历以node_id为起点的链，记录链上的点和边
 * 
 * @param node_id 
 * @param chain_nodes 
 * @param chain_edges 
 * @param edges 
 */
void dfs(long long node_id, set<long long>& chain_nodes, set<long long>& chain_edges, vector<vector<long long>>& edges,vector<cc> & chain) {
	chain_nodes.insert(node_id);
	if(edges[node_id].size()==0){
		chain.push_back(cc(node_id, chain_nodes, chain_edges));
	}
	for (long long edge_id : edges[node_id]) {
		chain_edges.insert(edge_id);
		Edge edge = id2edge[edge_id];
		long long next_node = edge.getToId();
		if (chain_nodes.find(next_node) == chain_nodes.end()) {
			dfs(next_node, chain_nodes, chain_edges, edges,chain);
		}
	}
}

void create_Connected_transitive(vector<string> filenames) {
    memset(node_cnt, 0 ,sizeof(node_cnt));// 点出度归零
	memset(node_in_cnt, 0 ,sizeof(node_in_cnt));// 点出度归零
	// fout2 << filename << endl;
	string filename="";
	for(int i=0;i<filenames.size();i++){
		if(i!=0) filename+=filenames[i]+"|";
		else filename+=filenames[i];
	}
	cout<<"path: "<<filename<<"----"<<endl;
	vector<PII> nodes_parent;
	vector<long long> nodes;
	vector<vector<long long>> edges(327588);
	long long line = 0;
	long long n_num = 0;
	long long r_num = 0;
	string s;

	for(string file_name:filenames){
		ifstream fin_relationships(file_name);
		r_num = readline(file_name, line);// line递增

		while (getline(fin_relationships, s)) {
			vector<string> vec;
			vector<string> edge_id;
			vector<string> node_u;
			vector<string> node_v;
			stringSplit1(s, " ", vec);
			stringSplit1(vec[0], ":", edge_id);
			long long uid= atoll(edge_id[1].c_str());
			stringSplit1(vec[1], ":", node_u);
			long long u = atoll(node_u[1].c_str());
			stringSplit1(vec[2], ":", node_v);
			long long v = atoll(node_v[1].c_str());
			nodes.push_back(u);
			nodes.push_back(v);
				node_cnt[u]++;
				node_in_cnt[v]++;
			edges[u].push_back(uid);
			id2edge[uid] = Edge(uid, getFileName(file_name), u, v);
		}
		fin_relationships.close();

		sort(nodes.begin(), nodes.end());//点id递增排列
		nodes.erase(unique(nodes.begin(), nodes.end()),  nodes.end());//去重
		n_num = nodes.size();//点数目

	}
	cout<<"nodes size: "<<n_num<<endl;
	cout<<"edges size: "<<id2edge.size()<<endl;

	// 没有入度的为起点
	set<long long> chain_tail;
	for (long long i = 0; i < n_num; i++) {
		if(node_in_cnt[nodes[i]] == 0) {
			chain_tail.insert(nodes[i]);
		}
	}
	cout<<"chain_tail size: "<<chain_tail.size()<<endl;
	// 求每一个起点开始的链
	vector<cc> chain;// 起点id, <链的点集合, 链的边集合>
	long long cc_cnt=1;
	for(auto i: chain_tail) {
		set<long long> chain_nodes;
		set<long long> chain_edges;
		dfs(i, chain_nodes, chain_edges, edges, chain);
	}
	cout<<"chain size: "<<chain.size()<<endl;
	// if(filename.find("replyOf")!=string::npos){
	// 	// 两个链的边集有交集就合并
	// 	for(auto it1 = chain.begin(); it1 != chain.end(); it1++) {
	// 		for(auto it2 = next(it1); it2 != chain.end();) {
	// 			set<long long> intersection_edges;
	// 			set_intersection(it1->getEdges().begin(), it1->getEdges().end(),
	// 				it2->getEdges().begin(), it2->getEdges().end(),
	// 				inserter(intersection_edges, intersection_edges.begin()));
	// 			if (!intersection_edges.empty()) {
	// 				// 合并链
	// 				it1->getNodes().insert(it2->getNodes().begin(), it2->getNodes().end());
	// 				it1->getEdges().insert(it2->getEdges().begin(), it2->getEdges().end());
	// 				it2 = chain.erase(it2); // 删除it2指向的元素，并返回下一个元素的迭代器
	// 			} else {
	// 				++it2; // 继续遍历下一个元素
	// 			}
	// 		}
	// 	}
	// }
	
	fout2 << "connected components number:" << chain.size() << endl;

	string file_name_clear = filename;
	// 遍历chain，输出每个连通分量的信息
	for (const auto& chain_info : chain) {
		fout << ccnt << " ";
		fout1 << ccnt << ":";
		fout3 << ccnt << ":" << filename << endl;
		long long cnt = 0;
		for (const auto& node : chain_info.getNodes()) {
			fout1 << node << ",";
			cnt += new_property_num[judge_label(node)];// 点属性数目
			cnt += getRelationshipsCost(node, file_name_clear);// 边属性数目
		}
		fout << cnt <<" "<< getFileName(filename)<< endl;
		fout1 << endl;
		ccnt++;// CC编号
	}
	fout2 << filename << endl;
}

void create_Connected(vector<string> filenames) {
    memset(node_cnt, 0 ,sizeof(node_cnt));
	// fout2 << filename << endl;
	string filename="";
	for(int i=0;i<filenames.size();i++){
		filename+=filenames[i];
	}
	vector<PII> nodes_parent;
	vector<int> nodes;
	long long line = 0;
	long long n_num = 0;
	long long r_num = 0;
	DisJointSetUnion(327588);
	string s;

	for(string file_name:filenames){
		// string file_name = filename;
		ifstream fin_relationships(file_name);
		ifstream fin_relationships2(file_name);
		r_num = readline(file_name, line);

		while (getline(fin_relationships, s)) {
			vector<string> vec;
			vector<string> node_u;
			vector<string> node_v;
			stringSplit1(s, " ", vec);
			stringSplit1(vec[1], ":", node_u);
			int u = atoi(node_u[1].c_str());
			stringSplit1(vec[2], ":", node_v);
			int v = atoi(node_v[1].c_str());
			nodes.push_back(u);
			nodes.push_back(v);
				node_cnt[u]++;
		}
		fin_relationships.close();

		sort(nodes.begin(), nodes.end());
		nodes.erase(unique(nodes.begin(), nodes.end()),  nodes.end());
		n_num = nodes.size();


		while (getline(fin_relationships2, s)) {
			vector<string> vec;
			vector<string> node_u;
			vector<string> node_v;

			stringSplit1(s, " ", vec);
			stringSplit1(vec[1], ":", node_u);
			int u = atoi(node_u[1].c_str());
			stringSplit1(vec[2], ":", node_v);
			int v = atoi(node_v[1].c_str());
			Union(u, v);
		}

		fin_relationships2.close();
	}
	

	vector<int> t;
    t.push_back(11);
    for (int i = 0; i < 327588; i++){
        nodes_parent.push_back({i, t});
    }

	int count = 0;
	for (int i = 0; i < n_num; i++) {
		int fx = find(nodes[i]);
		if (fx == nodes[i]) {
			count++;
		}
	}

	fout2 << "connected components number:" << count << endl;

	int cnt_has_find_father = 0;
	for (int i = 0; i < n_num; i++) {
		int fx = find(nodes[i]);
		if (i % 100000 == 0) {
			cout << i << endl;
		}
        	nodes_parent[fx].second.push_back(nodes[i]);
	}

    // string file_name_clear = getFileName(file_name);
	string file_name_clear = filename;
    for (int i = 0; i < 327588; i++) {
        if (nodes_parent[i].second.size() > 1) {
		    fout << ccnt << " ";
		    fout1 << ccnt << ":";
			fout3 << ccnt << ":" << filename << endl;
            long long cnt = 0;
            for (int j = 1; j < nodes_parent[i].second.size(); j++){
			    fout1 << nodes_parent[i].second[j] << ",";
			    cnt += new_property_num[judge_label(nodes_parent[i].second[j])];
                cnt += getRelationshipsCost(nodes_parent[i].second[j], file_name_clear);
		    }
            fout << cnt << endl;
		    fout1 << endl;
		    ccnt++;
        }
    }
	fout2 << filename << endl;
	nodes_parent.clear();
}

void create_Connected(string filename) {
    memset(node_cnt, 0 ,sizeof(node_cnt));
	fout2 << filename << endl;
	vector<PII> nodes_parent;

	string file_name = filename;
	ifstream fin_relationships(file_name);
	ifstream fin_relationships2(file_name);
	vector<int> nodes;
	
	long long line = 0;
	long long r_num = readline(file_name, line);

	long long n_num = 0;
	string s;
	while (getline(fin_relationships, s)) {
		vector<string> vec;
		vector<string> node_u;
		vector<string> node_v;
		stringSplit1(s, " ", vec);
		stringSplit1(vec[1], ":", node_u);
		int u = atoi(node_u[1].c_str());
		stringSplit1(vec[2], ":", node_v);
		int v = atoi(node_v[1].c_str());
		nodes.push_back(u);
		nodes.push_back(v);
        	node_cnt[u]++;
	}
	fin_relationships.close();

	sort(nodes.begin(), nodes.end());
	nodes.erase(unique(nodes.begin(), nodes.end()),  nodes.end());
	n_num = nodes.size();

	DisJointSetUnion(327588);

	while (getline(fin_relationships2, s)) {
		vector<string> vec;
		vector<string> node_u;
		vector<string> node_v;

		stringSplit1(s, " ", vec);
		stringSplit1(vec[1], ":", node_u);
		int u = atoi(node_u[1].c_str());
		stringSplit1(vec[2], ":", node_v);
		int v = atoi(node_v[1].c_str());
		Union(u, v);
	}

	fin_relationships2.close();
	vector<int> t;
    t.push_back(11);
    for (int i = 0; i < 327588; i++){
        nodes_parent.push_back({i, t});
    }

	int count = 0;
	for (int i = 0; i < n_num; i++) {
		int fx = find(nodes[i]);
		if (fx == nodes[i]) {
			count++;
		}
	}

	fout2 << "connected components number:" << count << endl;

	int cnt_has_find_father = 0;
	for (int i = 0; i < n_num; i++) {
		int fx = find(nodes[i]);
		if (i % 100000 == 0) {
			cout << i << endl;
		}
        	nodes_parent[fx].second.push_back(nodes[i]);
	}

    string file_name_clear = getFileName(file_name);
    for (int i = 0; i < 327588; i++) {
        if (nodes_parent[i].second.size() > 1) {
		    fout << ccnt << " ";
		    fout1 << ccnt << ":";
			fout3 << ccnt << ":" << filename << endl;
            long long cnt = 0;
            for (int j = 1; j < nodes_parent[i].second.size(); j++){
			    fout1 << nodes_parent[i].second[j] << ",";
			    cnt += new_property_num[judge_label(nodes_parent[i].second[j])];
                cnt += getRelationshipsCost(nodes_parent[i].second[j], file_name_clear);
		    }
            fout << cnt<< " "<<file_name_clear << endl;
		    fout1 << endl;
		    ccnt++;
        }
    }
	fout2 << filename << endl;
	nodes_parent.clear();
}

int main() {
    auto start = std::chrono::high_resolution_clock::now();

	fout.open("./output/new_SF0.1/connected_size_all_v4.txt");
	fout1.open("./output/new_SF0.1/region_component_v4.txt");
	fout2.open("./output/new_SF0.1/done_file_name_v4.txt");
	fout3.open("./output/new_SF0.1/connected_label_v4.txt");
	string file_path = "./output/new_SF0.1/new_output_relationships";
	vector<string> my_file;//./output/new_SF0.1/new_output_relationships/label.txt
	string need_extension = "txt";
	get_need_file(file_path, my_file, need_extension);
	for (int i = 0; i < my_file.size(); i++) {
		// create_Connected(my_file[i]);
		if(my_file[i].find("knows")!=string::npos) create_Connected(my_file[i]);
		else create_Connected_transitive({my_file[i]});
		parent.clear();
		rank1.clear();
	}

    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double, std::milli> duration = end - start;

    std::cout << "Time taken by the code: " << duration.count() << " ms" << std::endl;
	return 0;
}
