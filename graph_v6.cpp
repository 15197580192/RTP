#define _CRT_SECURE_NO_WARNINGS
#include <iostream>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <vector>
#include <map>
#include <unordered_map>
#include <set>
#include <algorithm>
#include <sys/io.h>
#include <dirent.h>
#include <chrono>
#include <queue>

using namespace std;
using ll = long long;

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

map<long long,Edge> id2edge;// 原边ID -> 边对象
unordered_map<ll, set<ll>> out_edges;  // 出边邻接表，原节点 -> 出边ID列表
unordered_map<ll, set<ll>> in_edges;   // 入边邻接表，原节点 -> 入边ID列表

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

// 自定义 CC 类型（无需重载 < 运算符，完全自定义排序规则）
struct CC {
    ll id;
    std::string label;
    long long cost;
	set<long long> nodes;
	// 重载 < 运算符：定义默认排序规则（按 cost 降序）
    bool operator<(const CC& other) const {
        // 优先队列默认使用 std::less<CC>，会调用此运算符，确保 cost 大的优先出队
        return this->cost < other.cost;
    }
};

vector<ll>parent;
vector<ll>rank1;
ofstream fout;//./output/new_SF0.1/connected_size_all_v4.txt
ofstream fout1;//"./output/new_SF0.1/region_component_v4.txt
ofstream fout2;//./output/new_SF0.1/done_file_name_v4.txt
ofstream fout3;//./output/new_SF0.1/connected_label_v4.txt
typedef pair<ll, vector<ll> > PII;

map<string,set<long long>> labelNodes;
map<string,map<long long,long long>> labelNodesCount;

map<string,vector<ll>> node_num;// L的点的出度
map<string,priority_queue<CC>> all_cc;
ll node_cnt[29987835];//todo 点的出度
ll node_in_cnt[29987835];//todo 点的入度

ll ccnt = 1;// cc编号

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

void get_need_file(const std::string& path, std::vector<std::string>& paths, const std::string& extension) {
    DIR* dir;
    struct dirent* entry;

	cout<<"----finding in path: "<<path<<" to find extension: "<<extension<<"----"<<endl;

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

void DisJointSetUnion(long long n) {
	for (long long i = 0; i < n; i++) {
		parent.push_back(i);
		rank1.push_back(1);
	}
}

long long find(long long x) {
	if (x == parent[x])
		return x;
	else {
		parent[x] = find(parent[x]);
		return parent[x];
	}
}

void Union(long long x, long long y) {
	long long rx = find(x);
	long long ry = find(y);

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
ll getRelationshipsCost(ll node_id, string file_name) {
	ll res=0;
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
        res+= node_cnt[node_id];
    }
	return res;
}
// 字符串按|拆分的函数
std::vector<std::string> splitStringByPipe(const std::string& str) {
    std::vector<std::string> result;
    size_t start = 0;  // 记录每个子串的起始索引
    size_t len = str.length();

    for (size_t i = 0; i < len; ++i) {
        // 遇到分隔符|时，截取[start, i)区间的子串
        if (str[i] == '|') {
            // 截取子串：从start开始，长度为i - start
            result.push_back(str.substr(start, i - start));
            start = i + 1;  // 更新下一个子串的起始索引（跳过分隔符）
        }
    }

    // 处理最后一个子串（遍历结束后，start到字符串末尾的部分）
    if (start < len) {
        result.push_back(str.substr(start));
    }

    return result;
}
ll getRelationshipsCost1(ll node_id, string file_name) {
	std::vector<std::string> vec = splitStringByPipe(file_name);
	ll res=0;
	for(auto file:vec){
		if(file_name.find("hasMember")==string::npos&&file_name.find("knows")==string::npos&&file_name.find("likes")==string::npos&&file_name.find("studyAt")==string::npos&&file_name.find("workAt")==string::npos){
			res+= node_num[file][node_id];
		} else {
			res+=2 * node_num[file][node_id];
		}
	}
	return res;
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
	memset(node_in_cnt, 0 ,sizeof(node_in_cnt));// 点入度归零
	// fout2 << filename << endl;
	string filename="";
	for(int i=0;i<filenames.size();i++){
		if(i!=0) filename+=filenames[i]+"|";
		else filename+=filenames[i];
	}
	cout<<"path: "<<filename<<"----"<<endl;
	vector<PII> nodes_parent;
	vector<long long> nodes;
	vector<long long> totalEdges;
	vector<vector<long long>> edges(29987835);// todo 点的邻居
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
			totalEdges.push_back(uid);
			id2edge[uid] = Edge(uid, getFileName(file_name), u, v);

			//统计label的点
			set<long long>& nodesOfLabel = labelNodes[file_name];
			nodesOfLabel.insert(u);
			nodesOfLabel.insert(v);

		}
		fin_relationships.close();

		sort(nodes.begin(), nodes.end());//点id递增排列
		nodes.erase(unique(nodes.begin(), nodes.end()),  nodes.end());//去重
		n_num = nodes.size();//点数目

	}
	cout<<"nodes size: "<<n_num<<endl;
	cout<<"edges size: "<<id2edge.size()<<endl;

	DisJointSetUnion(176623433);// todo 边大小


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
	for(auto i: chain_tail) {// 对于每个起点
		set<long long> chain_nodes;
		set<long long> chain_edges;
		dfs(i, chain_nodes, chain_edges, edges, chain);// 找可达的边和点
		auto e = chain_edges.begin();
		// 边并查集，合并可达的边
		if(e!=chain_edges.end()){
			for(auto it1= next(e);it1!=chain_edges.end();it1++){
				Union(*e,*it1);
			}
		}
	}
	// 遍历每一个边，找到父边
	vector<set<long long>> trans_nodes(176623433);
	vector<set<long long>> trans_edges(176623433);// todo 边大小
	for(auto i:totalEdges){
		long long x = find(i);
		Edge ee = id2edge[i];
		trans_nodes[x].insert(ee.getFromId());
		trans_nodes[x].insert(ee.getToId());
		trans_edges[x].insert(i);

	}
	cout<<"chain size: "<<chain.size()<<endl;
	fout2 << "connected components number:" << chain.size() << endl;

	string file_name_clear = filename;
	// 遍历chain，输出每个连通分量的信息
	// for (const auto& chain_info : chains_nodes) {
	for (const auto& chain_info : trans_nodes) {
		if(chain_info.size()==0) continue;
		fout << ccnt << " ";
		fout1 << ccnt << ":";
		fout3 << ccnt << ":" << filename << endl;
		long long cnt = 0;
		// for (const auto& node : chain_info.getNodes()) {
		for (const auto& node : chain_info) {
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

// void create_Connected_transitive_v1(vector<string> filenames) {
void create_Connected_transitive_v1(string filename) {
    // memset(node_cnt, 0 ,sizeof(node_cnt));
	node_num.clear();
	out_edges.clear();
    in_edges.clear();
	vector<string> filenames=splitStringByPipe(filename);
	bool flag_single_label=true;
	if(filenames.size()>1) flag_single_label=false;
	fout2 << filename << endl;
	cout << filename << endl;
	vector<PII> nodes_parent;// 根点（原图的边）
	vector<ll> nodes;// label的点
	vector<ll> edges;// label的边
	long long line = 0;
	long long n_num = 0;
	long long r_num = 0;
	DisJointSetUnion(176623433);// todo 边
	string s;
	for(string file_name:filenames){
		node_num[file_name] = std::vector<long long>(29987835, 0LL);
	}

	for(string file_name:filenames){
		// string file_name = filename;
		ifstream fin_relationships(file_name);
		r_num = readline(file_name, line);
		// build graph
		while (getline(fin_relationships, s)) {
			vector<string> vec;
			vector<string> edge_id;
			vector<string> node_u;
			vector<string> node_v;
			stringSplit1(s, " ", vec);
			stringSplit1(vec[0], ":", edge_id);
			ll eid=atoll(edge_id[1].c_str());
			stringSplit1(vec[1], ":", node_u);
			ll u = atoll(node_u[1].c_str());
			stringSplit1(vec[2], ":", node_v);
			ll v = atoll(node_v[1].c_str());
			nodes.push_back(u);
			nodes.push_back(v);
			edges.push_back(eid);
				// node_cnt[u]++;
				node_num[file_name][u]++;

			
			// 记录边信息
            id2edge[eid] = Edge(eid, getFileName(file_name), u, v);
			// 构建原图的入/出边索引：u的出边是eid，v的入边是eid
            out_edges[u].insert(eid);
            in_edges[v].insert(eid);
			// 统计文件对应的节点（原逻辑保留）
			labelNodes[filename].insert(u);
			labelNodes[filename].insert(v);
		}
		fin_relationships.close();
		// 点去重
		sort(nodes.begin(), nodes.end());
		nodes.erase(unique(nodes.begin(), nodes.end()),  nodes.end());
		n_num = nodes.size();

		// 构建并查集
		for(ll u:nodes){
			// step1：当前节点的入边 与 出边 两两相连（辅助图的边）
			set<ll>& ins = in_edges[u];   // u的入边列表（辅助图节点）
			set<ll>& outs = out_edges[u]; // u的出边列表（辅助图节点）
			for (ll in_eid : ins) {
				for (ll out_eid : outs) {
					Union(in_eid, out_eid);// 入边-出边相连
				}
			}

			// step2：当前节点的出边之间 两两相连（辅助图的边）
			for (auto it1 = outs.begin(); it1 != outs.end(); ++it1) {
				auto it2 = next(it1); 
				for (; it2 != outs.end(); ++it2) {
					ll eid1 = *it1;
					ll eid2 = *it2;
					Union(eid1, eid2); // 出边之间两两合并
				}
			}
		}
	}
	

	vector<ll> t;
    t.push_back(11);
    for (ll i = 0; i < 176623433; i++){// todo 边
        nodes_parent.push_back({i, t});
    }

	ll count = 0;
	// for (ll i = 0; i < r_num; i++) {
	// 	ll fx = find(edges[i]);
	// 	if (fx == edges[i]) {
	// 		count++;
	// 	}
	// }

	// fout2 << "connected components number:" << count << endl;

	cout << "nodes num:" << n_num << endl;
	cout << "edges num:" << r_num << endl;
	// cout << "connected components number:" << count << endl;
	ll cnt_has_find_father = 0;
	for (ll i = 0; i < r_num; i++) {
		ll fx = find(edges[i]);
		// if (i % 100000 == 0) {
		// 	cout << i << endl;
		// }
        	nodes_parent[fx].second.push_back(edges[i]);
	}

    for (ll i = 0; i < 176623433; i++) {// todo 边
        if (nodes_parent[i].second.size() > 1) {
			count++;
		    // fout << ccnt << " ";
		    // fout1 << ccnt << ":";
			// fout3 << ccnt << ":" << filename << endl;
            long long cnt = 0;
			set<ll> cc_nodes;
            for (ll j = 1; j < nodes_parent[i].second.size(); j++){
				Edge ee = id2edge[nodes_parent[i].second[j]];
				cc_nodes.insert(ee.getFromId());
				cc_nodes.insert(ee.getToId());
		    }
			for(ll j:cc_nodes){
				// fout1 << j << ",";
			    cnt += new_property_num[judge_label(j)];
                cnt += getRelationshipsCost1(j, filename);
			}
            // fout << cnt << " "<<getFileName(filename) << endl;
		    // fout1 << endl;
			all_cc[filename].push(CC{ccnt,filename,cnt,cc_nodes});
		    ccnt++;
        }
    }
	cout << "connected components number:" << count << endl;
	fout2 << "connected components number:" << count << endl;
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
	vector<ll> nodes;
	
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
		ll u = atoi(node_u[1].c_str());
		stringSplit1(vec[2], ":", node_v);
		ll v = atoi(node_v[1].c_str());
		nodes.push_back(u);
		nodes.push_back(v);
        	node_cnt[u]++;

		//统计label的点
		set<long long>& nodesOfLabel = labelNodes[file_name];
		nodesOfLabel.insert(u);
		nodesOfLabel.insert(v);

	}
	fin_relationships.close();

	sort(nodes.begin(), nodes.end());
	nodes.erase(unique(nodes.begin(), nodes.end()),  nodes.end());
	n_num = nodes.size();

	DisJointSetUnion(29987835);// todo 点

	while (getline(fin_relationships2, s)) {
		vector<string> vec;
		vector<string> node_u;
		vector<string> node_v;

		stringSplit1(s, " ", vec);
		stringSplit1(vec[1], ":", node_u);
		ll u = atoi(node_u[1].c_str());
		stringSplit1(vec[2], ":", node_v);
		ll v = atoi(node_v[1].c_str());
		Union(u, v);
	}

	fin_relationships2.close();
	vector<ll> t;
    t.push_back(11);
    for (ll i = 0; i < 29987835; i++){// todo 点
        nodes_parent.push_back({i, t});
    }

	ll count = 0;
	for (ll i = 0; i < n_num; i++) {
		ll fx = find(nodes[i]);
		if (fx == nodes[i]) {
			count++;
		}
	}

	fout2 << "connected components number:" << count << endl;

	ll cnt_has_find_father = 0;
	for (ll i = 0; i < n_num; i++) {
		ll fx = find(nodes[i]);
		if (i % 100000 == 0) {
			cout << i << endl;
		}
        	nodes_parent[fx].second.push_back(nodes[i]);
	}

    string file_name_clear = getFileName(file_name);
    for (ll i = 0; i < 29987835; i++) {// todo 点
        if (nodes_parent[i].second.size() > 1) {
		    // fout << ccnt << " ";
		    // fout1 << ccnt << ":";
			// fout3 << ccnt << ":" << filename << endl;
            long long cnt = 0;
			set<ll> cc_nodes;
            for (ll j = 1; j < nodes_parent[i].second.size(); j++){
			    // fout1 << nodes_parent[i].second[j] << ",";
				cc_nodes.insert(nodes_parent[i].second[j]);
			    cnt += new_property_num[judge_label(nodes_parent[i].second[j])];
                cnt += getRelationshipsCost(nodes_parent[i].second[j], file_name_clear);
		    }
			all_cc[filename].push(CC{ccnt,filename,cnt,cc_nodes});
            // fout << cnt<< " "<<file_name_clear << endl;
		    // fout1 << endl;
		    ccnt++;
        }
    }
	fout2 << filename << endl;
	nodes_parent.clear();
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

struct lpair{
	string l1;
	string l2;
	long long w;
	// 重载 < 运算符：定义默认排序规则（按 aff 降序）
    bool operator<(const lpair& other) const {
        // 优先队列默认使用 std::less<CC>，会调用此运算符，确保 cost 大的优先出队
        return this->w < other.w;
    }
};

ll getAff(string l1,string l2){
	set<ll> &n1=labelNodes[l1];
	set<ll> &n2=labelNodes[l2];
	ll res=0;
	for(auto node:n1){
		if(n2.find(node)!=n2.end()){
			res+=new_property_num[judge_label(node)];
		}
	}
	return res;
}

int main() {
	// 原图的cost
    ll graph_cost=getGraphCost("./output/new_SF10/new_output_nodes.txt","./output/new_SF10/new_output_relationships");
    cout<<"graph cost:"<<graph_cost<<endl;
	int partition_cnt=8;
	double alpha=1.0;
	double beta=1.0;
	double cap0 = alpha * (graph_cost / partition_cnt);// 初始阈值
	double B=beta*cap0;

    auto start = std::chrono::high_resolution_clock::now();

	fout.open("./output/new_SF10/connected_size_all_v4.txt");
	fout1.open("./output/new_SF10/region_component_v4.txt");
	fout2.open("./output/new_SF10/done_file_name_v4.txt");
	fout3.open("./output/new_SF10/connected_label_v4.txt");
	// string file_path = "./output/new_SF0.1/new_output_relationships";
	string file_path = "./output/new_SF10/new_output_relationships";// todo // /data1/lq/RCP
	vector<string> my_file;//./output/new_SF0.1/new_output_relationships/label.txt
	string need_extension = "txt";
	get_need_file(file_path, my_file, need_extension);

	for (int i = 0; i < my_file.size(); i++) {
		// v1
		// create_Connected(my_file[i]);
		// v4
		// if(my_file[i].find("knows")!=string::npos) create_Connected(my_file[i]);
		// else create_Connected_transitive({my_file[i]});
		// v5
		if(my_file[i].find("knows")!=string::npos) create_Connected(my_file[i]);
		else create_Connected_transitive_v1(my_file[i]);
	}
	set<string> res_label;
	priority_queue<lpair> pq;
	for(int i=0;i < my_file.size(); i++)
	{	
		string l1=my_file[i];
		res_label.insert(l1);
		for(int j=i+1;j < my_file.size(); j++){
			string l2=my_file[j];
			ll w=getAff(l1,l2);
			if(w>0) pq.push(lpair{l1,l2,w});
		}
	}
	while(!pq.empty()){
		auto &p=pq.top();
		pq.pop();
		string l1=p.l1;
		string l2=p.l2;
		if(res_label.find(l1)==res_label.end()||res_label.find(l2)==res_label.end()) continue;
		if(p.w==0) continue;
		if(all_cc[l1].top().cost>B||all_cc[l2].top().cost>B) continue;
		string new_l=l1+"|"+l2;
		create_Connected_transitive_v1(new_l);
		if(all_cc[new_l].top().cost>B) continue;
		res_label.erase(l1);
		res_label.erase(l2);
		all_cc.erase(l1);
		all_cc.erase(l2);
		res_label.insert(new_l);
		for(auto l:res_label){
			if(l!=new_l){
				ll w=getAff(l,new_l);
				if(w>0) pq.push(lpair{l,new_l,w});
			}
		}
	}
	cout<<"------------------------------------------------------------"<<endl;
	cout<<"final label:"<<endl;
	ccnt=1;
	for(auto label:res_label){
		cout<<label<<endl;
		auto q=all_cc[label];
		while(!q.empty()){
			CC c = q.top();
			q.pop();
			fout << ccnt << " ";
		    fout1 << ccnt << ":";
			fout3 << ccnt << ":" << label << endl;
			for(auto j:c.nodes){
				fout1 << j << ",";
			}
			fout << c.cost << " "<< label << endl;
		    fout1 << endl;
			ccnt++;
		}	
	}
	cout<<"cc count:"<<ccnt<<endl;

    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double, std::milli> duration = end - start;

    std::cout << "Time taken by the code: " << duration.count() << " ms" << std::endl;
	return 0;
}
