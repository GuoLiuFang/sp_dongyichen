# coding:utf-8
print "草拟吗"
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import re
import os


def fixData(day):
    start = """'""" + day + """'"""
    # end = """'""" + day + """ 00:10:59'"""
    end = """'""" + day + """ 23:59:59'"""
    dbConenectMessage = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108', db='honeycomb',
                                        use_unicode=True, port=5209, charset='utf8')
    messageExecutor = dbConenectMessage.cursor()
    sql = """select * from honeycomb.sms_received_histories_all_thread where content is not null and record_time between """ + start + """ and """ + end
    # print sql
    messageExecutor.execute(sql)
    messageContent = messageExecutor.fetchall()
    return messageContent


def getValidMessage(message):
    finished = ('点播了', '已', '感谢您', '订购的')
    for i in finished:
        if i in message:
            return True
    return False


def getSubString(message):
    a = 0
    if '已' in message:
        a = message.index('已')
    b = 0
    if '订购的' in message:
        b = message.index('订购的')
    start = max(a, b)
    leng = 28
    targetStr = message[start:start + leng]
    return targetStr


def getStatus(message):
    baoyue = ('订购', '定制', '订制', '办理')
    dianbo = ('点播', '感谢您')
    cancel = ('取消', '退订')
    for i in dianbo:
        if i in message:
            return 1
    for i in baoyue:
        targetStr = getSubString(message)
        if i in targetStr:
            return 2
    for i in cancel:
        targetStr = getSubString(message)
        if i in targetStr:
            return 0
    return -1


def getChargeStr(message):
    for yewuTuple in yewucodeList:
        yewucode_name = yewuTuple[7].encode(encoding='utf-8')
        if yewucode_name in message:
            return yewuTuple
    return -1


dbConnectChargeStatistic = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108', db='honeycomb',
                                           use_unicode=True, port=5209, charset='utf8')
chargeCodeStatisticExecutor = dbConnectChargeStatistic.cursor()
sql = """select * from charge_codes_statistics"""
chargeCodeStatisticExecutor.execute(sql)
yewucodeList = chargeCodeStatisticExecutor.fetchall()
# messageContent = fixData('2016-11-01')
messageContent = fixData(sys.argv[1])
# 这是修补数据所在的地址
csvfile = open("/data/sdg/guoliufang/mysqloutfile/FixCsv.txt", mode='wa+')
# csvfile = open("/Users/LiuFangGuo/Downloads/ResultCsv.txt", mode='wa+')
csvlist = []

for index in range(len(messageContent)):
    message = messageContent[index][2].encode(encoding='utf-8')
    isValid = getValidMessage(message)
    if not isValid:
        # -11代表不包含完成时的状态关键字
        csvlist.append(
            (
                messageContent[index][0], messageContent[index][1], messageContent[index][2], messageContent[index][3],
                messageContent[index][4], messageContent[index][5], messageContent[index][6], messageContent[index][7],
                messageContent[index][8], messageContent[index][9], messageContent[index][10],
                messageContent[index][11], messageContent[index][12], messageContent[index][13],
                -11,
                -1, -1, -1, -1, -1, -1, -1, -1))
        continue
    else:
        status = getStatus(message)
        if status > -1:
            chargeStr = getChargeStr(message)
            if chargeStr == -1:
                # -13代表没有合适的charge_code 组合
                csvlist.append((
                    messageContent[index][0], messageContent[index][1], messageContent[index][2],
                    messageContent[index][3], messageContent[index][4], messageContent[index][5],
                    messageContent[index][6], messageContent[index][7], messageContent[index][8],
                    messageContent[index][9], messageContent[index][10], messageContent[index][11],
                    messageContent[index][12], messageContent[index][13], -13, -1, -1,
                    -1, -1, -1, -1, -1, -1))
                continue
            else:
                csvlist.append((
                    messageContent[index][0], messageContent[index][1], messageContent[index][2],
                    messageContent[index][3], messageContent[index][4], messageContent[index][5],
                    messageContent[index][6], messageContent[index][7], messageContent[index][8],
                    messageContent[index][9], messageContent[index][10], messageContent[index][11],
                    messageContent[index][12], messageContent[index][13], status,
                    chargeStr[1], chargeStr[2], chargeStr[3], chargeStr[4], chargeStr[5], chargeStr[6], chargeStr[7],
                    chargeStr[8]))
                continue
        else:
            # -12代表虽然包含了完成时，但是仍然不是要找的3个类别中的东西
            csvlist.append((
                messageContent[index][0], messageContent[index][1], messageContent[index][2], messageContent[index][3],
                messageContent[index][4], messageContent[index][5], messageContent[index][6], messageContent[index][7],
                messageContent[index][8], messageContent[index][9], messageContent[index][10],
                messageContent[index][11], messageContent[index][12], messageContent[index][13],
                -12, -1, -1, -1,
                -1, -1, -1, -1, -1))
            continue
for record in csvlist:
    csvfile.write('|'.join(str(e) for e in record) + "\n")
csvfile.close()
