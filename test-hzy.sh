for ((i=1; i<=7; i++))
do
    for j in {0..3}
    do
    bash run.sh ./build/is${i}o
    done
done
for ((i=1; i<=14; i++))
do
    for j in {0..3}
    do
    bash run.sh ./build/ic${i}o
    done
done