def mergeVertex(inPath, outPath, num): # num是分区数
    with open(outPath, "w", encoding='utf-8') as newTxtfile:
        for i in range(num):
            with open(inPath+'/000'+str(i), "r", encoding='utf-8') as oldTxtfile:
                for index,line in enumerate(oldTxtfile):
                    if(index%10000 == 0):
                        print('已合并分区'+str(i)+'的'+str(index)+'条边')
                    temp = line.replace('\n','').split(' ')
                    newTxtfile.write(temp[0]+' '+temp[1]+' '+str(i)+'\n')


inPath = '/data1/hzy/neo4j/partition_code/result/result_sheep/sheep' # 分区文件保存地址
outPath = '/data1/hzy/neo4j/partition_code/result/result_sheep/result.txt' # 处理完成后的文件保存地址
mergeVertex(inPath,outPath,8)


            
