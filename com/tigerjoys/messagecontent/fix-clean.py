# coding:utf-8
print "草拟吗"
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb


def fixData():
    dbConenectMessage = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108', db='honeycomb',
                                        use_unicode=True, port=5209, charset='utf8')
    messageExecutor = dbConenectMessage.cursor()
    sql = """select * from honeycomb.message_analysises where content is not null and yewucode_name like '%包月%' or yewucode_name like '%小额支付%' or length(yewucode_name) != 0 or yewucode_name in ('传情彩字','儿童健康乐园','博物博览','小额支付','就医指导','指尖聚焦','无忧择业','生活全知道','移动凤凰快报','行业分析','财税新闻','赛事动态',' 超级乐乐乐','金融守则','随身教授')"""
    messageExecutor.execute(sql)
    messageContent = messageExecutor.fetchall()
    return messageContent


def getValidMessage(message):
    finished = ('点播了', '已', '感谢您', '订购的', '订购了', '生效')
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
    c = 0
    if '订购了' in message:
        c = message.index('订购了')
    d = 0
    if '生效' in message:
        d = message.index('生效')
    start = max(a, b, c, d)
    leng = 38
    targetStr = message[start:start + leng]
    return targetStr


def getStatus(message):
    baoyue = ('订购', '定制', '订制', '办理', '生效')
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
sql = """select * from charge_codes_statistics where yewucode_name not like '%包月%' AND yewucode_name not like '%小额支付%' and length(yewucode_name) != 0"""
chargeCodeStatisticExecutor.execute(sql)
yewucodeList = chargeCodeStatisticExecutor.fetchall()
messageContent = fixData()
# 这是修补数据所在的地址
csvfile = open("/data/sdg/guoliufang/mysqloutfile/CuoWuXiuZheng.txt", mode='wa+')
# csvfile = open("/Users/LiuFangGuo/Downloads/CuoWuXiuZheng.txt", mode='wa+')
csvlist = []

for index in range(len(messageContent)):
    message = messageContent[index][2].encode(encoding='utf-8')
    # message = """尊敬的1星级客户，您好!中国移动提醒您:您已于2016年12月02日 订购了[  1、赛事动态；]其中:产品赛事动态将于2016年12月02日生效,产品费用为10元;您可享有1倍积分回馈、10元话费透支等星级服务，回复短信CXXJ到10086或手机登录wap.sx.10086.cn查询您的星级并了解星级服务权益。"""
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
