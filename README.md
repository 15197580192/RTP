# Connectivity-Oriented Property Graph Partitioning for Distributed Graph Pattern Query Processing

# Overview
relationship connectivity partitioning(RCP) is a edge-centric property graph partitioning approach, where the objective function is to minimize the cuts to the number of distinct crossing properties. This approach can be used to avoid inter-partition joins in a wider set of Cypher/Gremlin workloads in the context of distributed Cypher/Gremlin query evaluation.

# Preprocessing
This section outlines the steps and methodologies involved in preprocessing the data.
```
# input preprocessing
cp *.csv ./import
# output preprocessing,please look to the file `output_dir.sh` and replace the `directory` with the exact directory name 
cd output 
bash output_dir.sh
```

## csv2txt.cpp
This file is used to divide the edge csv files in the original data set into the corresponding txt files according to the relationship.
```
ofstream fout1;//nodes
fout1.open("./output/new_SF1000/new_output_nodes.txt");// nodepath
ofstream fout2;//relationship
fout2.open("./output/new_SF1000/new_output_relationships.txt");// relationpath
string output = "./output/new_SF1000/new_output_relationships/"; // relaitons path divided in parts


g++ csv2txt.cpp -o csv2txt
./csv2txt >res
```
## graph_v4.cpp
This file is used to process the txt files obtained in the previous step into the corresponding LCC set. The set of interconnected nodes is divided into an LCC by connecting different nodes by edges.
```
// node cnt (wc -l ./output/new_SF1000/new_output_nodes.txt),here is 327588
int node_cnt[327588];
DisJointSetUnion(327588);
for (int i = 0; i < 327588; i++){
    for (int i = 0; i < 327588; i++) {
// nodes properties num(the order correspond to those in judege_label) 
int new_property_num[] = {4, 3, 3, 4, 3, 6, 10, 8};
// nodes type number,must config the range according to new_output_nodes.txt

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

g++ graph_v4.cpp -o graph_v4
./graph_v4 >res2
```

# Partitioning
This section provides an overview of the steps and methods involved in partitioning.

## partition_v4.cpp
This file is used to divide the LCC obtained in the previous step into different partitions considering both cost and benefit.
```
// cc num
wc -l ./output/new_SF1000/connected_size_all_v4.txt
struct Con_Com {
	int bh, cnt;
} cc[32254];
// partition parts num,nodes_num
int partition_number_cnt[7][327588];
int partition_bool[7][327588];
int partition_cnt = 6;

g++ partition_v4.cpp -o partition_v4
./partition_v4 >res3
```

## distributeData_nodes.cpp
This file is used to divide the total node table into sub-node tables of each partition based on the set of nodes belonging to each partition obtained in the previous step.
```
struct Node {
    string label;
    int id;
    string comment;
}node[327588];

while (now_partition_id < 6){

mkdir -p ./output/new_SF1000/partition_txt/partition_0
mkdir -p ./output/new_SF1000/partition_txt/partition_1
mkdir -p ./output/new_SF1000/partition_txt/partition_2
mkdir -p ./output/new_SF1000/partition_txt/partition_3
mkdir -p ./output/new_SF1000/partition_txt/partition_4
mkdir -p ./output/new_SF1000/partition_txt/partition_5

g++ distributeData_nodes.cpp -o distributeData_nodes
./distributeData_nodes >res4
```

## distributeData_relationships.cpp
This file is used to partition the total edge table into sub-edge tables of each partition based on the set of LCCS belonging to each partition obtained in the previous step.
```
    while (now_partition_id < 8){
    
g++ distributeData_relationships.cpp -o distributeData_relationships
./distributeData_relationships >res5
```

## txt2csv.cpp
This file is used to convert the sub-node table and sub-edge table obtained in the previous step into a csv file.
```
mkdir -p ./output/new_SF1000/partition_csv/partition_0
mkdir -p ./output/new_SF1000/partition_csv/partition_1
mkdir -p ./output/new_SF1000/partition_csv/partition_2
mkdir -p ./output/new_SF1000/partition_csv/partition_3
mkdir -p ./output/new_SF1000/partition_csv/partition_4
mkdir -p ./output/new_SF1000/partition_csv/partition_5
g++ txt2csv.cpp -o txt2csv
./txt2csv >res6
```

# Benchmark Queries
The benchmark queries used in our experimental evaluation exists in #queries# folder.

If you encounter any problems, please send emails to me (email address: shimin22@hnu.edu.cn).
