import csv
import time


csvVexFile = {'comment_0_0.csv': [],
              'forum_0_0.csv': [],
              'organisation_0_0.csv': [],
              'person_0_0.csv': [],
              'place_0_0.csv': [],
              'post_0_0.csv': [],
              'tag_0_0.csv': [],
              'tagclass_0_0.csv': []}

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


def toVex(inPath, outPath):
    startIndex = 0
    endIndex = 0
    resVexID = {}

    for key in csvVexFile:
        print('正在处理'+key+'点集')
        startIndex = endIndex
        with open(inPath+key,encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile) 
            next(csv_reader)
            with open(outPath,"a") as txtfile:
                for raw in csv_reader:
                    raw = raw[0].split('|')
                    txtfile.write(raw[0] + '@' + key + ' ' + str(endIndex) + '\n')
                    resVexID[raw[0] + '@' + key] = endIndex
                    endIndex = endIndex + 1
                csvVexFile[key].append(startIndex)
                csvVexFile[key].append(endIndex-1)
    return resVexID

def toEdges(inPath, outPath, resVexID):
    for key in csvEdgesFile:
        print('正在处理'+key+'边集')
        with open(inPath+key,encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile) 
            next(csv_reader)
            with open(outPath,"a") as txtfile:
                for raw in csv_reader:
                    raw = raw[0].split('|')
                    txtfile.write(str(resVexID[raw[0]+'@'+csvEdgesFile[key][0]]) + ' ' + str(resVexID[raw[1]+'@'+csvEdgesFile[key][1]]) + '\n')

def saveVexBelong(outPath):
    with open(outPath,"a") as txtfile:
        for key in csvVexFile:
            txtfile.write(key + ' ' + str(csvVexFile[key][0]) + ' ' + str(csvVexFile[key][1]) + '\n')

# nohup python3 csv2net_sheep.py >csv2net_sheep.log &
inPath = '/data1/lq/RCP/import/' # csv文件目录地址
start = time.perf_counter()
resVexID = toVex(inPath,'/data1/hzy/neo4j/partition_code/result/result_sheep/vexKey_sheep.txt') # 第二个参数为点原ID与新ID对应关系的保存文件
toEdges(inPath,'/data1/hzy/neo4j/partition_code/result/result_sheep/result_sheep.net',resVexID) # 第二个参数为边集文件保存地址
saveVexBelong('/data1/hzy/neo4j/partition_code/result/result_sheep/vexBelong_sheep.txt') # 参数为每类点id范围保存文件地址
end = time.perf_counter()
print(str(end-start))