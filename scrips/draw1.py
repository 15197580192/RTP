import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from io import StringIO

# 文本格式的数据（模拟CSV文件内容）
data_text = """类型,平均值,最小值,最大值,中位数
containerOf,100.72,13.00,1968.00,96.00
hasCreator,13.48,10.00,17.00,13.00
hasInterest,214.95,15.00,726.00,132.00
hasMember,95.03,14.00,2726.00,54.00
hasModerator,13.00,13.00,13.00,13.00
hasTag,38.73,12.00,726.00,26.00
hasType,17.00,17.00,17.00,17.00
isLocatedIn,13.46,10.00,17.00,13.00
isPartOf,97.69,20.00,1406.00,76.00
isSubclassOf,218.00,35.00,548.00,71.00
knows,36288.00,36288.00,36288.00,36288.00
likes,647.99,11.00,9695.00,439.00
replyOf,20.61,10.00,148.00,15.00
studyAt,14.00,14.00,14.00,14.00
workAt,27.96,14.00,46.00,30.00"""

# 读取文本数据为DataFrame
df = pd.read_csv(StringIO(data_text))

# 从DataFrame中提取数据
categories = df['类型'].tolist()
avg = df['平均值'].tolist()
min_val = df['最小值'].tolist()
max_val = df['最大值'].tolist()
median = df['中位数'].tolist()

# 设置图形
plt.figure(figsize=(16, 9))
plt.yscale('log')  # 使用对数刻度
plt.title('the property number of transitive path', fontsize=16)
plt.xlabel('relation', fontsize=12)
plt.ylabel('property num', fontsize=12)
plt.grid(axis='y', linestyle='--', alpha=0.7)

# 设置柱状图位置和宽度
x = np.arange(len(categories))
width = 0.2

# 绘制柱状图（修复了中位数的label错误）
plt.bar(x - 1.5*width, avg, width, label='avg', alpha=0.8)
plt.bar(x - 0.5*width, min_val, width, label='min', alpha=0.8)
plt.bar(x + 0.5*width, max_val, width, label='max', alpha=0.8)
plt.bar(x + 1.5*width, median, width, label='median', alpha=0.8)  # 修复label为median

# 添加数值标签（只显示大于1000的值）
for i, v in enumerate(avg):
    if v > 1000:
        plt.text(i - 1.5*width, v*1.1, f'{v:.0f}', ha='center', fontsize=8)
        
for i, v in enumerate(max_val):
    if v > 1000:
        plt.text(i + 0.5*width, v*1.1, f'{v:.0f}', ha='center', fontsize=8)

# 添加特殊标记（找到knows的索引位置，避免硬编码）
knows_idx = categories.index('knows')
plt.text(knows_idx - 1.5*width, 36288, '36288', ha='center', fontsize=9, color='red')
plt.text(knows_idx + 0.5*width, 36288, '36288', ha='center', fontsize=9, color='red')

# 设置x轴
plt.xticks(x, categories, rotation=45, ha='right', fontsize=10)
plt.tight_layout()

# 添加图例
plt.legend(fontsize=10)

# 先保存图像再显示（修复保存空白图的问题）
plt.savefig('relationship_stats_v5.png', dpi=300, bbox_inches='tight')
plt.show()