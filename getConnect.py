from py2neo import Graph

from datetime import datetime

def get_queryans(sentence, nowIP):
    # print("连接服务器")
    graph = Graph(nowIP, auth=("neo4j", "Neo4j@123456"))

    # print("sentence", sentence)

    # 去除前缀
    prefix_removed = nowIP.replace("http://", "")
    # prefix_removed = nowIP.replace("bolt://", "")

    # 替换点号和冒号为下划线
    formatted_str = prefix_removed.replace(".", "_").replace(":", "_")

    # 移除斜杠
    final_IP = formatted_str.rstrip('/')

    # 获取当前日期和时间
    now = datetime.now()

    # 格式化日期和时间
    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    # outfilename = "./query_time/SF100_NE/temp/temp_" + final_IP + ".txt"
    # outfilename = "./query_time/SF10_RCP_6/qc5/qc5_" + final_IP + ".txt"
    
    # 写入到文件
    # with open(outfilename, "a") as file:
    #     file.write(formatted_time + '\n')

    # print("开始查询")
    res = graph.run(sentence).data()
    # print("结束查询")

    # 获取当前日期和时间
    now = datetime.now()

    # 格式化日期和时间
    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    # 写入到文件
    # with open(outfilename, "a") as file:
    #     file.write(formatted_time + '\n')

    # print(nowIP)
    return res


def get_result(sentence, nowIP):
    # print("sentence:", sentence)
    
    # print("nowIP:", nowIP)
    ansList = get_queryans(sentence, nowIP)
    # print(ansList)
    resList = []
    for ans in ansList:
        resList.append(ans)
        # print(ans['n'])
    # for res in resList:
    #     print(res)
    # print("reList: ", resList)
    # print("reList's size: ", len(resList));
    return str(resList)

if __name__ == "__main__":
    # sentence = "MATCH (m:Comment{id:\"19968660\"})-[:hasCreator]->(p:Person) RETURN p"
    # sentence = "MATCH (m:Comment)-[:hasCreator]->(p:Person) RETURN p"
    sentence = "MATCH (m) RETURN m limit 10"
    # sentence = 'MATCH (person:Person {id: "13778"})-[:knows*2..3]-(friend) WHERE NOT friend=person AND      NOT (friend)-[:knows]-(person)	WITH friend	ORDER BY friend.creationDate DESC	RETURN DISTINCT friend	LIMIT 100'
    # sentence = "MATCH (m:Post{id:'6054624'}) SET m.content='About Lleyton Hewitt,  Masters Cup titles (2001 and 2002). In 2005, About Terry Pratchett' return m"
    print(get_result(sentence, "bolt://10.157.197.82:16200/"))
    print(get_result(sentence, "bolt://10.157.197.82:16201/"))
    print(get_result(sentence, "bolt://10.157.197.82:16202/"))
    print(get_result(sentence, "bolt://10.157.197.82:16203/"))
    print(get_result(sentence, "bolt://10.157.197.82:16204/"))
    print(get_result(sentence, "bolt://10.157.197.82:16205/"))
    print(get_result(sentence, "bolt://10.157.197.82:16206/"))
    print(get_result(sentence, "bolt://10.157.197.82:16207/"))
    
