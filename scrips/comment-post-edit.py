def replace_content_commas(input_file, output_file=None):
    """
    读取管道符分隔的文件，只替换content列中的逗号为短下划线
    其他列完全保持不变，生成新文件
    
    参数:
        input_file: 输入文件路径
        output_file: 输出文件路径（可选，默认在原文件名后加_modified）
    """
    # 设置输出文件名
    if output_file is None:
        import os
        base, ext = os.path.splitext(input_file)
        output_file = f"{base}_modified{ext}"
    
    try:
        print(f"读取文件: {input_file}")
        
        with open(input_file, 'r', encoding='utf-8') as f_in:
            lines = f_in.readlines()
        
        if not lines:
            print("错误: 文件为空")
            return
        
        # 处理标题行
        header_line = lines[0].strip()
        headers = header_line.split('|')
        
        # 查找content列的索引
        try:
            content_idx = headers.index('content')
            print(f"✅ 找到content列，位于第{content_idx + 1}列")
        except ValueError:
            # 尝试不区分大小写查找
            content_idx = -1
            for i, col in enumerate(headers):
                if col.lower() == 'content':
                    content_idx = i
                    break
            
            if content_idx == -1:
                print("❌ 错误: 未找到名为'content'的列")
                print(f"   文件中的列名: {headers}")
                return
        
        print(f"处理中...")
        
        total_lines = 0
        total_commas_replaced = 0
        
        with open(output_file, 'w', encoding='utf-8') as f_out:
            # 写入标题行（完全不变）
            f_out.write(header_line + '\n')
            
            # 处理数据行
            for line_num, line in enumerate(lines[1:], start=2):
                line = line.strip()
                if not line:  # 跳过空行
                    f_out.write('\n')
                    continue
                
                parts = line.split('|')
                
                # 如果列数不够，直接写入原行
                if len(parts) <= content_idx:
                    f_out.write(line + '\n')
                    continue
                
                # 只修改content列
                original_content = parts[content_idx]
                if ',' in original_content:
                    # 计算并替换逗号
                    comma_count = original_content.count(',')
                    new_content = original_content.replace(',', '_')
                    parts[content_idx] = new_content
                    
                    total_commas_replaced += comma_count
                
                # 重新组合为完整行
                new_line = '|'.join(parts)
                f_out.write(new_line + '\n')
                total_lines += 1
        
        print(f"\n✅ 处理完成！")
        print(f"   处理行数: {total_lines}")
        print(f"   替换的逗号数: {total_commas_replaced}")
        print(f"   新文件: {output_file}")
        
        # 显示修改前后的对比
        print("\n📊 修改前后对比示例:")
        if len(lines) > 1:
            original_first_line = lines[1].strip()
            original_parts = original_first_line.split('|')
            if len(original_parts) > content_idx:
                print(f"   原始content: {original_parts[content_idx][:50]}...")
                new_parts = original_first_line.split('|')
                new_parts[content_idx] = new_parts[content_idx].replace(',', '_')
                print(f"   修改后content: {new_parts[content_idx][:50]}...")
        
    except FileNotFoundError:
        print(f"❌ 错误: 找不到文件 '{input_file}'")
    except Exception as e:
        print(f"❌ 处理过程中出错: {e}")
        import traceback
        traceback.print_exc()


# 使用方法
# python3 comment-post-edit.py
if __name__ == "__main__":
    import os
    # 设置你的输入文件路径
    # input_file_path = "/data1/hzy/neo4j/neo4j0/import/post_0_0.csv"  # 请修改为你的实际文件路径
    
    # 方法1：使用默认输出文件名
    # replace_content_commas(input_file_path)
    
    # 方法2：指定输出文件名
    # replace_content_commas(input_file_path, "/data1/hzy/neo4j/neo4j0/import/post_0_0_new.csv")
    
    file_names = ["comment_0_0.csv", "post_0_0.csv"]
    for i in range(8):
        for file_name in file_names:
            # input_file = f"/data1/hzy/neo4j/neo4j{i}/rtpplus/import/{file_name}"
            input_file = f"/data1/hzy/neo4j/partition_code/result/result_NE/csv/{i}/{file_name}"
            output_file = f"/data1/hzy/neo4j/partition_code/result/result_NE/csv/{i}/{file_name.replace('.csv', '_new.csv')}"
            if os.path.exists(input_file):
                replace_content_commas(input_file, output_file)
                print(f"✅ 处理完成：{output_file}")
