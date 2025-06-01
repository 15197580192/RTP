directory="new_SF0.1"
partition_num=6
mkdir -p $directory/new_output_relationships
for ((i=0;i<$partition_num;i++));do
    mkdir -p $directory/partition_txt/partition_$i
    mkdir -p $directory/partition_csv/partition_$i
done