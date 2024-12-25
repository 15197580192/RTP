import requests
import json

url = 'http://127.0.0.1:9010/grpc/api'  # 替换为你的 ghttp 服务的地址和端口
# 设置请求头
headers = {'Content-Type': 'application/json'}

# 要发送的 JSON 数据
db_name = 'finbench-sf10'   # 请改为当前数据库名称
cypher = '''CALL ic8(2199023256816)
YIELD personId, personFirstName, personLastName, commentCreationDate, commentId, commentContent
RETURN personId, personFirstName, personLastName, commentCreationDate, commentId, commentContent'''

data = {'operation': 'query', 'username': 'root', 'password': '123456', 'db_name': db_name, 'cypher': cypher}

# 发送 POST 请求
response = requests.post(url, json=data, headers=headers)
# 输出响应信息
print("POST 请求响应状态码:", response.status_code)
print("POST 请求响应内容:", response.text)
