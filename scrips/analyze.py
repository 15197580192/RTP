import csv
import statistics
from collections import defaultdict

def process_data(file_path):
    type_costs = defaultdict(list)
    
    with open(file_path, 'r') as f:
        # # 读取第一行作为表头
        # header = f.readline().strip().split()
        
        # # 检查表头是否符合要求
        # if header != ['id', 'cost', 'type']:
        #     raise ValueError("文件列名必须为 'id cost type'（空格分隔）")
        
        # 逐行处理数据
        for line in f:
            line = line.strip()
            if not line:  # 跳过空行
                continue
                
            try:
                # 使用空格分割行数据（支持多个空格）
                parts = line.split()
                if len(parts) != 3:
                    raise ValueError(f"数据行格式错误：{line}")
                
                _id, cost_str, type_ = parts
                cost = float(cost_str)  # 转换为浮点数
                type_costs[type_].append(cost)
            except (ValueError, IndexError) as e:
                print(f"警告：跳过无效行 '{line}'，原因：{str(e)}")
                continue
    
    # 计算统计指标（与之前相同）
    results = {}
    for type_, costs in type_costs.items():
        if not costs:
            continue
        
        results[type_] = {
            "平均值": statistics.mean(costs),
            "最小值": min(costs),
            "最大值": max(costs),
            "中位数": statistics.median(costs)
        }
    
    return results

# 打印结果函数保持不变...
def print_results(results):
    # 格式化输出结果
    print("类型,平均值,最小值,最大值,中位数")
    for type_, stats in results.items():
        print(f"{type_},{stats['平均值']:.2f},{stats['最小值']:.2f},{stats['最大值']:.2f},{stats['中位数']:.2f}")


if __name__ == "__main__":
    # python3 analyze.py /data1/hzy/RTP/output/new_SF0.1/connected_size_all_v4.txt
    import sys
    if len(sys.argv) != 2:
        print(f"用法: python {sys.argv[0]} <数据文件路径>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    try:
        results = process_data(file_path)
        if results:
            print_results(results)
        else:
            print("警告：数据中没有有效记录")
    except FileNotFoundError:
        print(f"错误：文件 {file_path} 未找到")
    except Exception as e:
        print(f"错误：处理数据时发生异常 - {str(e)}")