#coding=utf-8
__author__ = 'xiyuanbupt'

from collections import defaultdict

goodidPrefixWithCount = defaultdict(int)
buyeridPrefixWithCount = defaultdict(int)


orderRecordsCount = 0


#用于记录文件中出现的所有的key，以及文件中出现key 的个数，key 的walue 出现的个数
#{buyerid:1}
#{buyerid:set(value)}
#开始的时间是 1463042797 结束的时间戳是 1471274773
keyWithCount = defaultdict(int)
keyWithValueSet = defaultdict(set)

def handleKV(kv):
    (key,value )= kv.split(":")
    keyWithCount[key] += 1
    keyWithValueSet[key].add(value)


def handleLine(line):
    kvPairs = line.split('\t');
    goodid = kvPairs[1]
    goodidPrefix = goodid.split(':')[1].split('_')[0]
    goodidPrefixWithCount[goodidPrefix] +=1
    buyerid = kvPairs[2]
    buyeridPrefix = buyerid.split(':')[1].split('_')[0]
    buyeridPrefixWithCount[buyeridPrefix] +=1
    for kv in kvPairs:
        handleKV(kv)

def init(file):
    with open(file) as f:
        for line in f:
            handleLine(line)
    print u'goodidPrefix with Count is : '

    print goodidPrefixWithCount
    print buyeridPrefixWithCount
    for k in keyWithCount:
        print u"Key is " ,k , u"Value is " ,keyWithCount[k]
    #print keyWithValueSet
    #print keyWithCount
    for k in keyWithValueSet:
        print u'Key is ',k,u'Value is'
        for value in keyWithValueSet[k]:
            print value


if __name__ == "__main__":
    init('/Users/xiyuanbupt/IdeaProjects/order-system/order_records.txt')

