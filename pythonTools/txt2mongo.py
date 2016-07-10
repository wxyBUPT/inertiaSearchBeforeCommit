#coding=utf-8

from pymongo import MongoClient
import json

client = MongoClient("mongodb://10.109.247.29")
db = client['tianchi']
print db['test'].find_one()

def line2json(line):
    result = dict({})
    KVPairs = line.split('\t')
    for KVPair in KVPairs:
        (K,V) = KVPair.split(":")
        result[K]=V
    return result

def file2mongo(filePath,dbName):
    f = open(filePath)
    for line in f:
        pass
        #print line2json(line)
        db[dbName].insert(line2json(line))

if __name__ == "__main__":
    file2mongo("/Users/xiyuanbupt/IdeaProjects/order-system/order_records.txt","order_records")
    print "done"
    file2mongo("/Users/xiyuanbupt/IdeaProjects/order-system/buyer_records.txt","buyer_records")
    print "done"
    file2mongo("/Users/xiyuanbupt/IdeaProjects/order-system/good_records.txt","good_records")
    print "done"

