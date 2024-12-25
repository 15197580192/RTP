#!/bin/bash

# 设置进程数
NP=7

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
mpi_output=$(mpirun -np $NP --allow-run-as-root --oversubscribe $MPI_PROGRAM 2>&1)

end_time=$(date +%s%3N)  # 获取当前时间戳，精确到毫秒

# 计算时间差（毫秒为单位）
execution_time=$((end_time - start_time))

# 转换为秒并保留三位小数，使用 printf 格式化输出
execution_time_sec=$(echo "scale=3; $execution_time / 1000" | bc)

# 使用 printf 格式化输出，总是保留三位小数
printf "总共耗时: %.3f 秒\n" $execution_time_sec

# 去除 mpi_output 的第一行(No protocol specified)
mpi_output_no_first_line=$(echo "$mpi_output" | sed '1d')

# 输出去除第一行后的 MPI 程序输出
echo "查询结果："
echo "$mpi_output_no_first_line"
