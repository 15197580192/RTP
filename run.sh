#!/bin/bash

# 设置进程数(1*主进程+节点个数*从进程)
ulimit -s unlimited #解决栈溢出
export MPICH_MAX_MSG_SIZE=4294967296  # ✅ MPICH专属：设置MPI最大消息大小为1GB
export MPICH_TCP_BUFFER_SIZE=4294967296 # ✅ MPICH专属：设置MPI TCP缓冲区为1GB
NP=9

# 检查是否提供了程序路径作为参数
if [ -z "$1" ]; then
  echo "错误：未指定 MPI 程序路径！"
  echo "用法：$0 <MPI_PROGRAM_PATH>"
  exit 1
fi

# 从命令行参数中获取 MPI 程序路径
MPI_PROGRAM="$1"

# 统计并输出执行时间
echo "执行程序：$MPI_PROGRAM"
start_time=$(date +%s%3N)  # 获取当前时间戳，精确到毫秒（毫秒表示）

# 捕获 mpirun 输出（标准输出和标准错误都捕获）
mpi_output=$(mpirun -np $NP $MPI_PROGRAM 2>&1)

end_time=$(date +%s%3N)  # 获取当前时间戳，精确到毫秒

# 计算时间差（毫秒为单位）
execution_time=$((end_time - start_time))

# 转换为秒并保留三位小数，使用 printf 格式化输出
execution_time_sec=$(echo "scale=3; $execution_time / 1000" | bc)

# 使用 printf 格式化输出，总是保留三位小数
# printf "总共耗时: %.3f 秒\n" $execution_time_sec

# MPI 程序输出
# ========== 控制输出行数（最多前10行） ==========
echo "查询结果："
echo "$mpi_output"
printf "总共耗时: %.3f 秒\n" $execution_time_sec
# # 统计输出的行数
# line_count=$(echo "$mpi_output" | wc -l)

# if [ "$line_count" -gt 10 ]; then
#     # 超过10行：输出前10行 + 提示省略行数
#     echo "$mpi_output" | head -n 10
#     echo "（省略后续 $((line_count - 10)) 行）"
# else
#     # 不超过10行：输出全部内容
#     echo "$mpi_output"
# fi
