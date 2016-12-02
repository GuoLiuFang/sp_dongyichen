#coding:utf-8
from hdfs import InsecureClient
client = InsecureClient('http://192.168.12.151:50070', user='guoliufang')
def includeTag(line):
    with client.write('/tmp/samplesTExtHdfsForPy',mode='aw+') as writer:
        writer.write(line)
    # f = open(name="/tmp/includeTag.txt",mode='w+')
    # f.write(line)
    # f.write("\n")
    return '1'*1000
def notIncludeTag(line):
    with client.write('/tmp/samplesNOtINcludeTExtHdfsForPy',mode='aw+') as writer:
        writer.write(line)
    return '2'*1000