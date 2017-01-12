# coding=utf-8
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import re


def fixData(day):
    start = """'""" + day + """'"""
    # end = """'""" + day + """ 00:10:59'"""
    end = """'""" + day + """ 23:59:59'"""
    dbConenectMessage = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108', db='honeycomb',
                                        use_unicode=True, port=5209, charset='utf8')
    messageExecutor = dbConenectMessage.cursor()
    sql = """select * from honeycomb.sms_received_histories_all_thread where content is not null and status !=2 and record_time between """ + start + """ and """ + end
    # print sql
    messageExecutor.execute(sql)
    messageContent = messageExecutor.fetchall()
    return messageContent


def getValidMessage(message):
    finished = ('点播了', '已', '感谢您使用', '订购的')
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
    dianbo = ('点播', '感谢您使用')
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


def ChargeCodeInSpNames(param):
    for sp_tuple in sp_channels:
        sp_name = sp_tuple[1].encode(encoding='utf-8')
        if param == sp_name:
            return True
    return False


def getSpName(message):
    sp_name_list = []
    for sp_tuple in sp_channels:
        sp_name = sp_tuple[1].encode(encoding='utf-8')
        if sp_name in message:
            ch_code = getChargeConde('-' + sp_name + '-', message)
            if ch_code == -1:
                continue
            else:
                if (ChargeCodeInSpNames(ch_code[6])) or (sp_name in sp_name_list):
                    continue
                else:
                    sp_name_list.append(sp_name)
                    return (
                        sp_tuple[0], sp_name, ch_code[0], ch_code[1], ch_code[2], ch_code[3], ch_code[4],
                        ch_code[5], ch_code[6])
    return -1


def getChargeConde(sp_name, message):
    for charge_tuple in charge_codes:
        sp_charge_str = charge_tuple[2].encode(encoding='utf-8')
        if sp_name in sp_charge_str:
            start = sp_charge_str.index(sp_name)
            targetStr = sp_charge_str[start + len(sp_name):]
            code_list = targetStr.split('-')
            if len(code_list) > 1:
                for code in code_list:
                    if (code) and (code in message):
                        tmp = re.match("""\d+元""", code)
                        if tmp:
                            continue
                        else:
                            charge_code_instruc_no_t = charge_tuple[4].replace("""*#T""", """""")
                            return (charge_tuple[0], charge_tuple[1], sp_charge_str, charge_tuple[3], charge_tuple[4],
                                    charge_code_instruc_no_t, code)
    return -1


# ---从这里开始是 main 函数入口
dbConenectReference = MySQLdb.connect(host='192.168.12.66', user='tigerreport', passwd='titmds4sp',
                                      db='TigerReport_production', use_unicode=True, charset='utf8')
executor = dbConenectReference.cursor()
executor.execute("""select id, name from sp_channels""")
sp_channels = executor.fetchall()
executor.execute("""select id, amount, name, dest_number, code from charge_codes""")
charge_codes = executor.fetchall()
# messageContent = fixData('2016-11-01')
messageContent = fixData(sys.argv[1])
# 这是修补数据所在的地址
csvfile = open("/data/sdg/guoliufang/mysqloutfile/FixCsv.txt", mode='wa+')
# csvfile = open("/Users/LiuFangGuo/Downloads/ResultCsv.txt", mode='wa+')
csvlist = []
for index in range(len(messageContent)):
    message = messageContent[index][2].encode(encoding='utf-8')
    sc = messageContent[index][4]
    rimsi = messageContent[index][5]
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
                -1, -1, -1, -1, -1, -1, -1, -1, -1))
        continue
    else:
        status = getStatus(message)
        if status > -1:
            sp_name = getSpName(message)
            if sp_name == -1:
                # -13代表没有合适的 sp_name 和 charge_code 组合
                csvlist.append((
                    messageContent[index][0], messageContent[index][1], messageContent[index][2],
                    messageContent[index][3], messageContent[index][4], messageContent[index][5],
                    messageContent[index][6], messageContent[index][7], messageContent[index][8],
                    messageContent[index][9], messageContent[index][10], messageContent[index][11],
                    messageContent[index][12], messageContent[index][13], -13, -1, -1,
                    -1, -1, -1, -1, -1, -1, -1))
                continue
            else:
                csvlist.append((
                    messageContent[index][0], messageContent[index][1], messageContent[index][2],
                    messageContent[index][3], messageContent[index][4], messageContent[index][5],
                    messageContent[index][6], messageContent[index][7], messageContent[index][8],
                    messageContent[index][9], messageContent[index][10], messageContent[index][11],
                    messageContent[index][12], messageContent[index][13], status,
                    sp_name[0], sp_name[1], sp_name[2], sp_name[3], sp_name[4], sp_name[5], sp_name[6], sp_name[7],
                    sp_name[8]))
        else:
            # -12代表虽然包含了完成时，但是仍然不是要找的3个类别中的东西
            csvlist.append((
                messageContent[index][0], messageContent[index][1], messageContent[index][2], messageContent[index][3],
                messageContent[index][4], messageContent[index][5], messageContent[index][6], messageContent[index][7],
                messageContent[index][8], messageContent[index][9], messageContent[index][10],
                messageContent[index][11], messageContent[index][12], messageContent[index][13],
                -12, -1, -1, -1,
                -1, -1, -1, -1, -1, -1))
            continue
for record in csvlist:
    csvfile.write('|'.join(str(e) for e in record) + "\n")
