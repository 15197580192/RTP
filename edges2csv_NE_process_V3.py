import csv
import multiprocessing
import os
import time


#每种点id的所在范围
csvVexBelong = {'comment_0_0.csv': [],
                'forum_0_0.csv': [],
                'organisation_0_0.csv': [],
                'person_0_0.csv': [],
                'place_0_0.csv': [],
                'post_0_0.csv': [],
                'tag_0_0.csv': [],
                'tagclass_0_0.csv': []}

#每个点新id与原id的对应关系
csvVexKey = {'comment_0_0.csv': {},
              'forum_0_0.csv': {},
              'organisation_0_0.csv': {},
              'person_0_0.csv': {},
              'place_0_0.csv': {},
              'post_0_0.csv': {},
              'tag_0_0.csv': {},
              'tagclass_0_0.csv': {}}

#所有分区的边信息
csvEdges = {}

#边文件与对应的点文件
csvEdgesFile = {'comment_hasCreator_person_0_0.csv': ['comment_0_0.csv','person_0_0.csv'],
                'comment_hasTag_tag_0_0.csv': ['comment_0_0.csv','tag_0_0.csv'], 
                'comment_isLocatedIn_place_0_0.csv': ['comment_0_0.csv','place_0_0.csv'],  
                'comment_replyOf_comment_0_0.csv': ['comment_0_0.csv','comment_0_0.csv'],  
                'comment_replyOf_post_0_0.csv': ['comment_0_0.csv','post_0_0.csv'],  
                'forum_containerOf_post_0_0.csv': ['forum_0_0.csv','post_0_0.csv'],  
                'forum_hasMember_person_0_0.csv': ['forum_0_0.csv','person_0_0.csv'],  
                'forum_hasModerator_person_0_0.csv': ['forum_0_0.csv','person_0_0.csv'], 
                'forum_hasTag_tag_0_0.csv': ['forum_0_0.csv','tag_0_0.csv'], 
                'organisation_isLocatedIn_place_0_0.csv': ['organisation_0_0.csv','place_0_0.csv'], 
                'person_hasInterest_tag_0_0.csv': ['person_0_0.csv','tag_0_0.csv'], 
                'person_isLocatedIn_place_0_0.csv': ['person_0_0.csv','place_0_0.csv'], 
                'person_knows_person_0_0.csv': ['person_0_0.csv','person_0_0.csv'], 
                'person_likes_comment_0_0.csv': ['person_0_0.csv','comment_0_0.csv'], 
                'person_likes_post_0_0.csv': ['person_0_0.csv','post_0_0.csv'], 
                'person_studyAt_organisation_0_0.csv': ['person_0_0.csv','organisation_0_0.csv'], 
                'person_workAt_organisation_0_0.csv': ['person_0_0.csv','organisation_0_0.csv'], 
                'place_isPartOf_place_0_0.csv': ['place_0_0.csv','place_0_0.csv'], 
                'post_hasCreator_person_0_0.csv': ['post_0_0.csv','person_0_0.csv'], 
                'post_hasTag_tag_0_0.csv': ['post_0_0.csv','tag_0_0.csv'],  
                'post_isLocatedIn_place_0_0.csv': ['post_0_0.csv','place_0_0.csv'], 
                'tag_hasType_tagclass_0_0.csv': ['tag_0_0.csv','tagclass_0_0.csv'], 
                'tagclass_isSubclassOf_tagclass_0_0.csv': ['tagclass_0_0.csv','tagclass_0_0.csv']}


def mkdir(path):
	folder = os.path.exists(path)
	if not folder:                   #判断是否存在文件夹如果不存在则创建为文件夹
		os.makedirs(path)            #makedirs 创建文件时如果路径不存在会创建这个路径
		
#获取点信息，需要传入点信息文件存放目录路径
def getVex(inPath):
    with open(inPath+'vexKey_NE.txt', 'r', encoding='utf-8') as txtfile:
        for line in  txtfile:
            temp = line.replace('\n','').split(' ')
            temp1 = temp[0].split('@')
            csvVexKey[temp1[1]][temp[1]] = temp1[0]
    
    with open(inPath+'vexBelong_NE.txt', 'r', encoding='utf-8') as txtfile:
        for line in  txtfile:
            temp = line.replace('\n','').split(' ')
            csvVexBelong[temp[0]].append(int(temp[1]))
            csvVexBelong[temp[0]].append(int(temp[2]))
    print('---点信息读取完成---')

#获取边信息，需要传入边集文件路径和分区数num
def getEdges(inPath, num):
    for i in range(num):
        csvEdges[str(i)] = {}
        temp = []
        for key in csvEdgesFile:
            flag = True
            for j in temp:
                if(csvEdgesFile[key][0] in j and csvEdgesFile[key][1] in j and csvEdgesFile[key][0] != csvEdgesFile[key][1]):
                    flag = False
                    break
            if(flag):
                temp.append({csvEdgesFile[key][0], csvEdgesFile[key][1]})
                csvEdges[str(i)][csvEdgesFile[key][0]+'@'+csvEdgesFile[key][1]] = {}
    
    print('分区边存放字典构造完毕')

    with open(inPath, 'r', encoding='utf-8') as txtfile:
        for i,line in  enumerate(txtfile):
            if(line.startswith('#')):
                continue
            # if(i%10000 == 0):
            #     print('已读取'+str(i)+'条边')
            temp = line.replace('\n','').split(' ')
            # 边关键词
            key1 = ''
            key2 = ''
            for key in csvVexBelong:
                if(int(temp[0]) >= csvVexBelong[key][0] and int(temp[0]) <= csvVexBelong[key][1]):
                    key1 = key
                if(int(temp[1]) >= csvVexBelong[key][0] and int(temp[1]) <= csvVexBelong[key][1]):
                    key2 = key
                if(key1 != '' and key2 != ''):
                    break
            
            if((key1+'@'+key2) in csvEdges[temp[2]]):
                csvEdges[temp[2]][key1+'@'+key2][csvVexKey[key1][temp[0]]+'@'+csvVexKey[key2][temp[1]]] = True
            elif((key2+'@'+key1) in csvEdges[temp[2]]):
                csvEdges[temp[2]][key2+'@'+key1][csvVexKey[key2][temp[1]]+'@'+csvVexKey[key1][temp[0]]] = True
            else:
                print('出错')
                quit()
    
    print('---边信息读取完成---')
    sum1 = 0
    for i in csvEdges:
        sum = 0
        for j in csvEdges[i]:
            sum = sum + len(csvEdges[i][j])
        print('分区'+i+'共：'+str(sum)+'条边')
        sum1 += sum
    print(sum1)


#点信息输出csv，参数依次为：原csv文件存放目录，分区后的新csv文件存放目录，所有分区的边信息，分区索引（代表当前处理第几个分区）
def vexToCsv(inPath, outPath, csvEdges, idx):
    mkdir(outPath+str(idx))
    print('正在处理分区'+str(idx)+'的点集')
    vertexID = {}
    for i in csvEdges[str(idx)]:
        print('正在读取'+i+'的所有点id')
        vertexID[i] = {}
        for j in csvEdges[str(idx)][i]:
            temp = j.split('@')
            vertexID[i][temp[0]] = True
            vertexID[i][temp[1]] = True

    for key in csvVexBelong:
        # print('正在处理'+key+'点集')
        with open(inPath+key, 'r', encoding='utf-8') as oldcsvfile:
            # 写入关系文件 模式“w”表示覆盖写入
            with open(outPath+str(idx)+'/'+key, "w", encoding="utf-8", newline="") as newcsvfile:
                oldcsv_reader = csv.reader(oldcsvfile, delimiter='|') 
                newcsv_writer = csv.writer(newcsvfile, delimiter='|')
                for j,row in enumerate(oldcsv_reader): #遍历旧csv
                    # row = row[0].split('|')
                    # if(j%1000==0 and j != 0):
                    #     print('已处理分区'+str(idx)+'的'+key+'文件中的'+str(j)+'个点')
                    if(j == 0):
                        newcsv_writer.writerow(row) #写入表头
                    else:
                        for k in csvEdgesFile:
                            if(key in csvEdgesFile[k] and (csvEdgesFile[k][0]+'@'+csvEdgesFile[k][1] in csvEdges[str(idx)])):
                                if(row[0] in vertexID[csvEdgesFile[k][0]+'@'+csvEdgesFile[k][1]]):
                                    newcsv_writer.writerow(row) #写入
                                    break


#边信息输出csv，参数依次为：原csv文件存放目录，分区后的新csv文件存放目录，所有分区的边信息，分区索引（代表当前处理第几个分区）
def edgeaToCsv(inPath, outPath, csvEdges, idx):
    mkdir(outPath+str(idx))
    print('正在处理分区'+str(idx)+'的边集')
    for key in csvEdgesFile:
        # print('正在处理'+key+'边集')
        with open(inPath+key, 'r', encoding='utf-8') as oldcsvfile:
            # 写入关系文件 模式“w”表示覆盖写入
            with open(outPath+str(idx)+'/'+key, "w", encoding="utf-8", newline="") as newcsvfile:
                oldcsv_reader = csv.reader(oldcsvfile, delimiter='|') 
                newcsv_writer = csv.writer(newcsvfile, delimiter='|')  
                for j,row in enumerate(oldcsv_reader): #遍历旧csv
                    # row = row[0].split('|')
                    # if(j%1000==0 and j != 0):
                    #     print('已处理分区'+str(idx)+'的'+key+'文件中的'+str(j)+'条边')
                    if(j == 0):
                        newcsv_writer.writerow(row) #写入表头
                    else:
                        if(csvEdgesFile[key][0]+'@'+csvEdgesFile[key][1] in csvEdges[str(idx)]):
                            if(row[0]+'@'+row[1] in csvEdges[str(idx)][csvEdgesFile[key][0]+'@'+csvEdgesFile[key][1]]):
                                newcsv_writer.writerow(row) #写入
                                # del csvEdges[str(idx)][csvEdgesFile[key][0]+'@'+csvEdgesFile[key][1]][row[0]+'@'+row[1]]
                        if(csvEdgesFile[key][1]+'@'+csvEdgesFile[key][0] in csvEdges[str(idx)]):
                            if(row[1]+'@'+row[0] in csvEdges[str(idx)][csvEdgesFile[key][1]+'@'+csvEdgesFile[key][0]]):
                                newcsv_writer.writerow(row) #写入
                                # del csvEdges[str(idx)][csvEdgesFile[key][1]+'@'+csvEdgesFile[key][0]][row[1]+'@'+row[0]]


# nohup python3 -u edges2csv_NE_process_V3.py >edges2csv_NE_process.log 2>&1 &
if __name__ == "__main__": 
    start = time.perf_counter()
    inPath = '/data1/lq/RCP/import/' # 原csv文件地址
    outPath = '/data1/hzy/neo4j/partition_code/result/result_NE/csv/' # 新分区后的csv文件保存地址
    getVex('/data1/hzy/neo4j/partition_code/result/result_NE/') # 获取点信息
    getEdges('/data1/hzy/neo4j/partition_code/result/result_NE/result_NE.edges.8.pedges', 8) # 获取8个分区的边信息
    end = time.perf_counter()
    t1 = str(end-start)

    pVex0 = multiprocessing.Process(target=vexToCsv, args=(inPath, outPath, csvEdges, 0))
    pVex1 = multiprocessing.Process(target=vexToCsv, args=(inPath, outPath, csvEdges, 1))
    pVex2 = multiprocessing.Process(target=vexToCsv, args=(inPath, outPath, csvEdges, 2))
    pVex3 = multiprocessing.Process(target=vexToCsv, args=(inPath, outPath, csvEdges, 3))
    pVex4 = multiprocessing.Process(target=vexToCsv, args=(inPath, outPath, csvEdges, 4))
    pVex5 = multiprocessing.Process(target=vexToCsv, args=(inPath, outPath, csvEdges, 5))
    pVex6 = multiprocessing.Process(target=vexToCsv, args=(inPath, outPath, csvEdges, 6))
    pVex7 = multiprocessing.Process(target=vexToCsv, args=(inPath, outPath, csvEdges, 7))

    pEdg0 = multiprocessing.Process(target=edgeaToCsv, args=(inPath, outPath, csvEdges, 0))
    pEdg1 = multiprocessing.Process(target=edgeaToCsv, args=(inPath, outPath, csvEdges, 1))
    pEdg2 = multiprocessing.Process(target=edgeaToCsv, args=(inPath, outPath, csvEdges, 2))
    pEdg3 = multiprocessing.Process(target=edgeaToCsv, args=(inPath, outPath, csvEdges, 3))
    pEdg4 = multiprocessing.Process(target=edgeaToCsv, args=(inPath, outPath, csvEdges, 4))
    pEdg5 = multiprocessing.Process(target=edgeaToCsv, args=(inPath, outPath, csvEdges, 5))
    pEdg6 = multiprocessing.Process(target=edgeaToCsv, args=(inPath, outPath, csvEdges, 6))
    pEdg7 = multiprocessing.Process(target=edgeaToCsv, args=(inPath, outPath, csvEdges, 7))
    
    start = time.perf_counter()
    pVex0.start()
    pVex1.start()
    pVex2.start()
    pVex3.start()
    pVex0.join()
    pVex1.join()
    pVex2.join()
    pVex3.join()

    pVex4.start()
    pVex5.start()
    pVex6.start()
    pVex7.start()
    pVex4.join()
    pVex5.join()
    pVex6.join()
    pVex7.join()

    pEdg0.start()
    pEdg1.start()
    pEdg2.start()
    pEdg3.start()
    pEdg0.join()
    pEdg1.join()
    pEdg2.join()
    pEdg3.join()

    pEdg4.start()
    pEdg5.start()
    pEdg6.start()
    pEdg7.start()
    pEdg4.join()
    pEdg5.join()
    pEdg6.join()
    pEdg7.join()

    end = time.perf_counter()
    t2 = str(end-start)

    print(t1)
    print(t2)
    print('处理完成！！！')