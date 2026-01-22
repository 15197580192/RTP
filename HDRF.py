

inPath = '/data/wangliang/result_HDRF/result.edges'
outPath = '/data/wangliang/result_HDRF/result.txt'
result = {}
finalData = []

with open(inPath, 'r', encoding='utf-8') as txtfile:
    for i,line in  enumerate(txtfile):
        temp = line.replace('\n','').split(',')
        temp1 = temp[1].split(': ')
        temp = [temp[0], temp1[0], temp1[1]]
        if((temp[0] + '@' + temp[1] not in result) and (temp[1] + '@' + temp[0] not in result)):
            result[temp[0] + '@' + temp[1]] = temp[2]
            finalData.append([temp[0], temp[1], temp[2]])
        elif((temp[0] + '@' + temp[1] not in result) and (temp[1] + '@' + temp[0] in result)):
            result[temp[0] + '@' + temp[1]] = result[temp[1] + '@' + temp[0]]
            finalData.append([temp[0], temp[1], result[temp[0] + '@' + temp[1]]])
        else:
            finalData.append([temp[0], temp[1], result[temp[0] + '@' + temp[1]]])

with open(outPath, 'w', encoding='utf-8') as txtfile:
    for i in finalData:
        txtfile.write(i[0] + ' ' + i[1] + ' ' + i[2] + '\n')

print(len(finalData))