# for i in {1..3}
# do
#   for j in {1..4}
#   do
#       # bash -u run.sh /data1/hzy/neo4j/RTP-main/build/is${i} >> test-rtp.log
#       # bash -u run.sh /data1/hzy/neo4j/RTP-main/build/is${i}p >> test-rtpplus.log
#       # bash -u run.sh /data1/lq/RTP-main/build/is${i} >> test-rcp.log
#       bash -u run.sh /data1/hzy/neo4j/RTP-main/build/is${i}o 262 >> test-ne.log
#       bash -u run.sh /data1/hzy/neo4j/RTP-main/build/is${i}o 362 >> test-sheep.log
#       bash -u run.sh /data1/hzy/neo4j/RTP-main/build/is${i}o 462 >> test-vgp.log
#   done
# done
# for i in {1..14}
for i in {5..5}
do
  for j in {1..4}
  do
      # bash -u run.sh /data1/hzy/neo4j/RTP-main/build/ic${i} >> test-rtp.log
      # bash -u run.sh /data1/hzy/neo4j/RTP-main/build/ic${i}p >> test-rtpplus.log
      # bash -u run.sh /data1/lq/RTP-main/build/ic${i} >> test-rcp.log
      bash -u run.sh /data1/hzy/neo4j/RTP-main/build/ic${i}o 262 >> test-ne.log
      bash -u run.sh /data1/hzy/neo4j/RTP-main/build/ic${i}o 362 >> test-sheep.log
      bash -u run.sh /data1/hzy/neo4j/RTP-main/build/ic${i}o 462 >> test-vgp.log
  done
done