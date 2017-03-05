# coding:utf-8
print "草拟吗"
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb


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
    # messageExecutor.execute(sql + """ limit 1 """)
    messageContent = messageExecutor.fetchall()
    return messageContent


def getValidMessage(message):
    finished = ('点播了', '已', '感谢您', '订购的', '订购了', '生效', '即将扣费')
    for i in finished:
        if i in message:
            return True
    return False


def getSubString(message):
    leng = len(message)
    a = 0
    if '已' in message:
        a = message.index('已')
    b = 0
    if '订购的' in message:
        b = message.index('订购的')
    c = 0
    if '订购了' in message:
        c = message.index('订购了')
    start = max(a, b, c)
    if start > 0:
        leng = 38
    targetStr = message[start:start + leng]
    return targetStr


def getStatus(message):
    baoyue = ('订购', '定制', '订制', '办理', '生效', '即将扣费')
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


def getSpStr(charge_code_id):
    for spMapTuple in chMapSp:
        if charge_code_id == spMapTuple[0]:
            return spMapTuple
    return -1


dbConnectChargeStatistic = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108', db='honeycomb',
                                           use_unicode=True, port=5209, charset='utf8')
chargeCodeStatisticExecutor = dbConnectChargeStatistic.cursor()
sql = """select * from charge_codes_statistics where yewucode_name not like '%包月%' AND yewucode_name not like '%小额支付%'"""
spMapSql = """select * from charge_sp_mapping"""
chargeCodeStatisticExecutor.execute(sql)
yewucodeList = chargeCodeStatisticExecutor.fetchall()
chargeCodeStatisticExecutor.execute(spMapSql)
chMapSp = chargeCodeStatisticExecutor.fetchall()
# messageContent = fixData('2016-11-01')
messageContent = fixData(sys.argv[1])
# 这是修补数据所在的地址
csvfile = open("/data/sdg/guoliufang/mysqloutfile/FixCsv.txt", mode='wa+')
# csvfile = open("/Users/LiuFangGuo/Downloads/ResultCsv.txt", mode='wa+')
csvlist = []

for index in range(len(messageContent)):
    message = messageContent[index][2].encode(encoding='utf-8')
    # message = """订购提醒：尊敬的客户，您好！您订购中国移动的生活全知道业务，10.00元/月，即将扣费，如有疑问可在24小时内回复“否”，我们将立即为您取消业务订购并不收取您任何费用。如需帮助，请咨询10086。中国移动"""
    isValid = getValidMessage(message)
    if not isValid:
        # -11代表不包含完成时的状态关键字
        csvlist.append(
            (
                messageContent[index][0], messageContent[index][1], messageContent[index][2], messageContent[index][3],
                messageContent[index][4], messageContent[index][5], messageContent[index][6], messageContent[index][7],
                messageContent[index][8], messageContent[index][9], messageContent[index][10],
                messageContent[index][11], messageContent[index][12], messageContent[index][13],
                -11, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1))
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
                    messageContent[index][12], messageContent[index][13], -13, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1))
                continue
            else:
                spMapStr = getSpStr(chargeStr[1])
                spId = -1
                spName = -1
                if spMapStr != -1:
                    spId = spMapStr[1]
                    spName = spMapStr[2]
                csvlist.append((
                    messageContent[index][0], messageContent[index][1], messageContent[index][2],
                    messageContent[index][3], messageContent[index][4], messageContent[index][5],
                    messageContent[index][6], messageContent[index][7], messageContent[index][8],
                    messageContent[index][9], messageContent[index][10], messageContent[index][11],
                    messageContent[index][12], messageContent[index][13], status,
                    chargeStr[1], chargeStr[2], chargeStr[3], chargeStr[4], chargeStr[5], chargeStr[6], chargeStr[7],
                    chargeStr[8], spId, spName))
                continue
        else:
            # -12代表虽然包含了完成时，但是仍然不是要找的3个类别中的东西
            csvlist.append((
                messageContent[index][0], messageContent[index][1], messageContent[index][2], messageContent[index][3],
                messageContent[index][4], messageContent[index][5], messageContent[index][6], messageContent[index][7],
                messageContent[index][8], messageContent[index][9], messageContent[index][10],
                messageContent[index][11], messageContent[index][12], messageContent[index][13],
                -12, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1))
            continue
for record in csvlist:
    csvfile.write('|'.join(str(e) for e in record) + "\n")
csvfile.close()
