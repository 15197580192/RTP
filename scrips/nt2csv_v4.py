from rdflib import Graph
import re
import sys

# nt文件地址
nt_file_path = 'C:/Users/e-Liang.Wang4/Desktop/dbpedia2014.nt'
# 顶点文件保存地址
vertex_file_path = 'C:/Users/e-Liang.Wang4/Desktop/vertex.txt'
# 关系文件保存地址
relation_file_path = 'C:/Users/e-Liang.Wang4/Desktop/relation.txt'

# 顶点id（记录了程序分配给各顶点的id）
vertex_id = {}

# 正则匹配
def check_string(re_exp, str):
    res = re.search(re_exp, str)
    if res:
        return True
    else:
        return False

pattern = 'http://.*?'
vex_idx = 0

print('开始处理点，解析nt文件')
with open(nt_file_path, "r", encoding="utf-8") as nt_file:
    # 写入顶点文件 模式“w”表示覆盖写入
    with open(vertex_file_path, "w", encoding="utf-8") as f_vertex:
        currentItem = ''
        for i,line in enumerate(nt_file):
            if(i%10000 == 0):
                print('点已处理'+str(i)+'个三元组')
            g = Graph()
            # 解析n-triple文件
            g.parse(data=line, format='nt')
            # 遍历三元组
            for s, p, o in g:
                if(i == 0):
                    currentItem = str(s)
                elif(str(s) != currentItem):
                    f_vertex.write('\n')
                    currentItem = str(s)

                if(check_string(pattern, str(s)) and str(s) not in vertex_id):
                    vertex_id[str(s)] = vex_idx
                    f_vertex.write(str(vex_idx)+'^_^'+str(s))
                    vex_idx += 1
                if(not check_string(pattern, str(o))):
                    f_vertex.write('^_^'+str(p)+'-_-'+str(o).replace('\n','|_|')) # 换行符替换成其他字符

        print('点写入完成！') # 点文件格式：id^_^uri^_^key1-_-value1^_^key2-_-value2^_^......

        print('开始处理边，解析nt文件')
        f_vertex.write('\n')
        # 写入关系文件 模式“w”表示覆盖写入
        with open(relation_file_path, "w", encoding="utf-8") as f_relation:
            nt_file.seek(0) # 调整nt文件指针重新指到第一行
            for i,line in enumerate(nt_file):
                if(i%10000 == 0):
                    print('边已处理'+str(i)+'个三元组')
                g = Graph()
                # 解析n-triple文件
                g.parse(data=line, format='nt')
                # 遍历三元组
                for s, p, o in g:
                    if(check_string(pattern, str(o)) and str(o) not in vertex_id):
                        vertex_id[str(o)] = vex_idx
                        f_vertex.write(str(vex_idx)+'^_^'+str(o)+'\n')
                        f_relation.write(str(vertex_id[str(s)])+','+str(vertex_id[str(o)])+','+str(p)+'\n')
                        vex_idx += 1
                    elif(check_string(pattern, str(o)) and str(o) in vertex_id):
                        f_relation.write(str(vertex_id[str(s)])+','+str(vertex_id[str(o)])+','+str(p)+'\n')

            print('边写入完成！') # 边文件格式：startID,endID,label
        
