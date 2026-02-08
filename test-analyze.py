import re
import csv
import json

def parse_log_file(log_file_path, csv_file_path):
    """
    解析日志文件（支持多条记录）并将结果逐条写入CSV
    
    Args:
        log_file_path: 日志文件路径
        csv_file_path: 输出CSV文件路径
    """
    # 初始化存储变量 - 改为列表存储多条记录
    log_records = []
    # 临时存储单条记录的信息
    current_record = {
        'exec_program': "",
        'query_result': "",
        'total_time': ""
    }
    # 标记是否进入查询结果区域
    in_query_result = False
    
    # 读取日志文件
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
            for line in lines:
                line = line.strip()  # 去除首尾空格和换行符
                
                # 匹配执行程序
                if line.startswith("执行程序："):
                    # 如果当前临时记录已有内容，先保存再更新（处理连续记录）
                    if current_record['exec_program'] or current_record['query_result'] or current_record['total_time']:
                        log_records.append(current_record.copy())
                    # 更新执行程序信息
                    current_record['exec_program'] = line.replace("执行程序：", "").strip()
                
                # 匹配查询结果的分隔线，标记开始提取查询结果
                elif line.startswith("---------------------------------------"):
                    in_query_result = True
                    continue
                
                # 提取查询结果（JSON格式行）
                elif in_query_result and line and line.startswith("'"):
                    # 替换单引号为双引号，使其符合JSON格式
                    json_str = line.replace("'", "\"")
                    try:
                        # 解析为JSON对象，再转回字符串保证格式正确
                        query_data = json.loads(json_str)
                        current_record['query_result'] = json.dumps(query_data, ensure_ascii=False, indent=2)
                    except:
                        # 解析失败则保留原始字符串
                        current_record['query_result'] = line
                    in_query_result = False  # 提取完成后取消标记
                
                # 匹配总共耗时（找到耗时代表单条记录结束）
                elif line.startswith("总共耗时:"):
                    # 使用正则提取数字（支持整数和小数）
                    time_match = re.search(r'总共耗时:\s*(\d+\.?\d*)\s*秒', line)
                    if time_match:
                        current_record['total_time'] = time_match.group(1)
                    # 单条记录提取完成，保存到列表并重置临时记录
                    log_records.append(current_record.copy())
                    # 重置临时记录
                    current_record = {
                        'exec_program': "",
                        'query_result': "",
                        'total_time': ""
                    }
        
        # 检查是否有未保存的最后一条记录
        if current_record['exec_program'] or current_record['query_result'] or current_record['total_time']:
            log_records.append(current_record)
    
    except FileNotFoundError:
        print(f"错误：未找到日志文件 {log_file_path}")
        return
    except Exception as e:
        print(f"读取日志文件时出错：{str(e)}")
        return
    
    # 过滤空记录（避免写入无效数据）
    log_records = [record for record in log_records if any(record.values())]
    
    # 写入CSV文件（逐条写入）
    try:
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            # 定义CSV表头
            fieldnames = ['执行程序', '查询结果', '总共耗时(秒)']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # 写入表头
            writer.writeheader()
            
            # 逐条写入记录
            for idx, record in enumerate(log_records, 1):
                writer.writerow({
                    '执行程序': record['exec_program'],
                    '查询结果': record['query_result'],
                    '总共耗时(秒)': record['total_time']
                })
        
        print(f"解析完成！共提取到 {len(log_records)} 条记录")
        print(f"结果已写入：{csv_file_path}")
        
        # 打印前3条记录预览（避免输出过多）
        print("\n前3条记录预览：")
        for idx, record in enumerate(log_records[:3], 1):
            print(f"\n第{idx}条：")
            print(f"执行程序：{record['exec_program']}")
            print(f"查询结果：{record['query_result'][:50]}..." if len(record['query_result'])>50 else f"查询结果：{record['query_result']}")
            print(f"总共耗时：{record['total_time']} 秒")
    
    except Exception as e:
        print(f"写入CSV文件时出错：{str(e)}")

# 示例调用
if __name__ == "__main__":
    # 替换为你的日志文件路径和输出CSV路径
    # parse_log_file("/data1/hzy/neo4j/RTP-main/test-rcp.log", "rcp_res.csv")
    # parse_log_file("/data1/hzy/neo4j/RTP-main/test-rtp.log", "rtp_res.csv")
    # parse_log_file("/data1/hzy/neo4j/RTP-main/test-rtpplus.log", "rtpplus_res.csv")
    parse_log_file("/data1/hzy/neo4j/RTP-main/test-sheep.log", "sheep_res.csv")
    parse_log_file("/data1/hzy/neo4j/RTP-main/test-vgp.log", "vgp_res.csv")
    parse_log_file("/data1/hzy/neo4j/RTP-main/test-ne.log", "ne_res.csv")