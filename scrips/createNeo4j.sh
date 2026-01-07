# 1. 先在宿主机创建需要映射的目录（建议统一放在一个文件夹下，方便管理）,并添加权限
mkdir -p /data1/hzy/neo4j/neo4j0/{data,import,logs,plugins}
chmod 777 /data1/hzy/neo4j -R

# 2. 启动 Neo4j 容器（替换其中的占位符）
# docker run -itd \
#   --name neo4j-rtp0 \  # 给容器起一个易识别的名字
#   -p 6100:7474 \          # 映射 HTTP 访问端口（宿主机6100 -> 容器7474）
#   -p 6200:7687 \          # 映射 Bolt 协议端口（宿主机6200 -> 容器7687）
#   -v /data1/hzy/neo4j/neo4j0/data:/data \          # 映射数据目录（核心，数据持久化）
#   -v /data1/hzy/neo4j/neo4j0/import:/import \      # 映射导入目录（导入数据用）
#   -v /data1/hzy/neo4j/neo4j0/logs:/logs \          # 映射日志目录（查看运行日志）
#   -v /data1/hzy/neo4j/neo4j0/plugins:/plugins \    # 映射插件目录（放扩展插件如APOC）
#   -e NEO4J_AUTH=neo4j/你的密码 \      # 设置 Neo4j 初始密码（必须）
#   -e NEO4J_dbms_connector_http_listen__address=0.0.0.0:7474 \  # 允许外部访问 HTTP 端口
#   -e NEO4J_dbms_connector_bolt_listen__address=0.0.0.0:7687 \  # 允许外部访问 Bolt 端口
#   swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/neo4j:3.5.26
docker run -itd \
  --name neo4j-rtp7 \
  -p 6107:7474 \
  -p 6207:7687 \
  -v /data1/hzy/neo4j/neo4j7/data:/data \
  -v /data1/hzy/neo4j/neo4j7/import:/import \
  -v /data1/hzy/neo4j/neo4j7/logs:/logs \
  -v /data1/hzy/neo4j/neo4j7/plugins:/plugins \
  -e NEO4J_AUTH=neo4j/Neo4j@123456 \
  -e NEO4J_dbms_connector_http_listen__address=0.0.0.0:7474 \
  -e NEO4J_dbms_connector_bolt_listen__address=0.0.0.0:7687 \
  swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/neo4j:3.5.26

# 查看docker日志
docker logs containerid

#连接
http://10.157.197.82:6100/
bolt://10.157.197.82:6200/

#导数据
cp /data1/hzy/RTP/output/new_SF10/partition_csv-v1/partition_0/* /data1/hzy/neo4j/neo4j0/import
cp /data1/hzy/RTP/output/new_SF10/partition_csv-v1/partition_1/* /data1/hzy/neo4j/neo4j1/import
cp /data1/hzy/RTP/output/new_SF10/partition_csv-v1/partition_2/* /data1/hzy/neo4j/neo4j2/import
cp /data1/hzy/RTP/output/new_SF10/partition_csv-v1/partition_3/* /data1/hzy/neo4j/neo4j3/import
cp /data1/hzy/RTP/output/new_SF10/partition_csv-v1/partition_4/* /data1/hzy/neo4j/neo4j4/import
cp /data1/hzy/RTP/output/new_SF10/partition_csv-v1/partition_5/* /data1/hzy/neo4j/neo4j5/import
cp /data1/hzy/RTP/output/new_SF10/partition_csv-v1/partition_6/* /data1/hzy/neo4j/neo4j6/import
cp /data1/hzy/RTP/output/new_SF10/partition_csv-v1/partition_7/* /data1/hzy/neo4j/neo4j7/import
