from py2neo import Graph

from datetime import datetime

def get_queryans(sentence, nowIP):
    # print("连接服务器")
    graph = Graph(nowIP, auth=("neo4j", "neo4j"))

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
    sentence = "match (p:Person {id: \"933\"}) return p"
    get_result(sentence, "bolt://139.9.250.196:7687/")
