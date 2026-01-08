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
    try:
        # print("sentence:", sentence)
        
        print("nowIP:", nowIP)
        ansList = get_queryans(sentence, nowIP)
        # print(ansList)
        resList = []
        for ans in ansList:
            resList.append(ans)
            # print(ans['n'])
        # for res in resList:
        #     print(res)
        print("reList: ", resList)
        # print("reList's size: ", len(resList));
        return str(resList)
    except Exception as e:
        print(f"查询时发生错误：{str(e)}")

def read_and_split_file(file_path, separator=";"):
    """
    读取文件内容并按指定分隔符分割
    
    Args:
        file_path (str): 要读取的文件路径
        separator (str): 分隔符，默认是分号;
    
    Returns:
        list: 分割后的字符串列表（自动过滤空字符串）
    """
    try:
        # 打开文件并读取内容（使用 utf-8 编码，兼容中文）
        with open(file_path, 'r', encoding='utf-8') as f:
            # 读取全部内容并去除首尾空白字符（换行、空格、制表符等）
            content = f.read().strip()
            
            # 如果文件内容为空，返回空列表
            if not content:
                print(f"提示：文件 {file_path} 内容为空")
                return []
            
            # 按分隔符分割内容
            split_result = content.split(separator)
            
            # 过滤分割后可能出现的空字符串（比如末尾有分号的情况）
            # 同时去除每个元素首尾的空白字符
            clean_result = [item.strip() for item in split_result if item.strip()]
            
            return clean_result
    
    # 处理文件不存在的情况
    except FileNotFoundError:
        print(f"错误：文件 {file_path} 不存在，请检查路径是否正确")
        return []
    # 处理权限不足的情况
    except PermissionError:
        print(f"错误：没有权限读取文件 {file_path}")
        return []
    # 处理其他异常
    except Exception as e:
        print(f"读取文件时发生未知错误：{str(e)}")
        return []

# nohup python -u neo4j-load.py >>neo4j-load.log 2>&1 &
if __name__ == "__main__":
    # 按分号分割文件内容
    result = read_and_split_file('/data1/hzy/RTP/scrips/build-ldbc.cypher')
    
    # 打印结果
    print("分割后的内容：")
    for i in range(0,len(result)):
        # 通过 result[i] 获取对应索引的元素
        sentence = result[i]
        print(f"{i}：{sentence}")
        get_result(sentence, "bolt://10.157.197.82:6200/")
        get_result(sentence, "bolt://10.157.197.82:6201/")
        get_result(sentence, "bolt://10.157.197.82:6202/")
        get_result(sentence, "bolt://10.157.197.82:6203/")
        get_result(sentence, "bolt://10.157.197.82:6204/")
        get_result(sentence, "bolt://10.157.197.82:6205/")
        get_result(sentence, "bolt://10.157.197.82:6206/")
        get_result(sentence, "bolt://10.157.197.82:6207/")
        print()
