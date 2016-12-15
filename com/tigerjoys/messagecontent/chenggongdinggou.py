# coding=utf-8
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import re
from suds.client import Client
import threading
import time


def fetchMessageAll(start, end):
    dbConenectMessage = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108', db='honeycomb',
                                        use_unicode=True, port=5209, charset='utf8')
    messageExecutor = dbConenectMessage.cursor()
    messageExecutor.execute(
        """select create_time,uuid,content,id,sc,rimsi,record_time from honeycomb.sms_received_histories_all where content is not null and id BETWEEN """ + start + """ and """ + end)
    messageContent = messageExecutor.fetchall()
    return messageContent


def fetchMessageByDay(day):
    dbConenectMessage = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108', db='honeycomb',
                                        use_unicode=True, port=5209, charset='utf8')
    messageExecutor = dbConenectMessage.cursor()
    dayEnd = day + " 23:59:59"
    sql = """select create_time,uuid,content,id,sc,rimsi,record_time from honeycomb.sms_received_histories_all where content is not null and create_time BETWEEN '""" + day + """' and '""" + dayEnd + """'"""
    messageExecutor.execute(sql)
    messageContent = messageExecutor.fetchall()
    return messageContent


def fetchMaxMinId():
    dbConenectMessage = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108', db='honeycomb',
                                        use_unicode=True, port=5209, charset='utf8')
    messageExecutor = dbConenectMessage.cursor()
    sql_get_max_id = """select max(id) from honeycomb.sms_received_histories_all"""
    messageExecutor.execute(sql_get_max_id)
    max_id = messageExecutor.fetchone()[0]
    sql_get_min_id = """select max(id) from honeycomb.sms_received_histories_all_thread"""
    messageExecutor.execute(sql_get_min_id)
    min_id = messageExecutor.fetchone()[0]

    return (min_id, max_id)


def getValidMessage(message):
    finished = ('点播了', '已', '感谢您使用')
    for i in finished:
        if i in message:
            return True
    return False


def getSubString(message):
    start = message.index('已')
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
    result_list = []
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
                    result_list.append((
                        sp_tuple[0], sp_name, ch_code[0], ch_code[1], ch_code[2], ch_code[3], ch_code[4],
                        ch_code[5], ch_code[6]))
    if len(result_list) > 0:
        return result_list
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
                            return -1
                        else:
                            charge_code_instruc_no_t = charge_tuple[4].replace("""*#T""", """""")
                            return (charge_tuple[0], charge_tuple[1], sp_charge_str, charge_tuple[3], charge_tuple[4],
                                    charge_code_instruc_no_t, code)
    return -1


def getProCity(sc, rimsi):
    city_id = -1
    city_name = -1
    city_pro_id = -1
    operator_id = -1
    operator_name = -1
    province_name = -1
    province_id = -1
    if not str(sc).isdigit():
        sc = '0'
    if not str(rimsi).isdigit():
        rimsi = '0'
    wsdl_url = """http://panda.didiman.com:82/Panda/LocationWebService?wsdl"""
    client = Client(wsdl_url)
    if str(sc) or str(rimsi):
        result = client.service.locate1(sc, rimsi)
        if hasattr(result.result, 'cities'):
            city_id = result.result.cities[0].id
            city_name = result.result.cities[0].name
            city_pro_id = result.result.cities[0].province_id
        operator_id = result.result.operator.id
        if hasattr(result.result.operator, 'name'):
            operator_name = result.result.operator.name
        if hasattr(result.result, 'province'):
            if hasattr(result.result.province, 'name'):
                province_name = result.result.province.name
            if hasattr(result.result.province, 'id'):
                province_id = result.result.province.id
    return (city_id, city_name, city_pro_id, operator_id, operator_name, province_name, province_id)


def badyRun(param1, param2):
    print "线程开始执行", param1, param2
    messageContent = fetchMessageAll(param1, param2)
    csvfile = open("/data/sdg/guoliufang/other_work_space/ResultCsv.txt" + param1, mode='wa+')
    # csvfile = open("/Users/LiuFangGuo/Downloads/ResultCsv.txt", mode='wa+')
    for index in range(len(messageContent)):
        csvlist = []
        message = messageContent[index][2].encode(encoding='utf-8')
        # message = """(1/2)您已成功定制联通宽带在线有限公司5575(10655575102)的10元给力付包月业务，发送TD10到10655575102退订"""
        sc = messageContent[index][4]
        rimsi = messageContent[index][5]
        try:
            proCity = getProCity(sc, rimsi)
        except Exception as exx:
            print "在 id 什么的位置发生了什么什么错误", messageContent[index][3], "前面是id 后面是异常的内容", exx
        isValid = getValidMessage(message)
        if not isValid:
            # -11代表不包含完成时的状态关键字
            csvlist.append(
                (
                    messageContent[index][0], messageContent[index][1], messageContent[index][2],
                    messageContent[index][3],
                    messageContent[index][4], messageContent[index][5], messageContent[index][6], proCity[0],
                    proCity[1],
                    proCity[2], proCity[3],
                    proCity[4], proCity[5], proCity[6],
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
                        messageContent[index][6], proCity[0],
                        proCity[1], proCity[2], proCity[3], proCity[4], proCity[5], proCity[6], -13, -1, -1,
                        -1, -1, -1, -1, -1, -1, -1))
                    continue
                else:
                    for i in sp_name:
                        csvlist.append((
                            messageContent[index][0], messageContent[index][1], messageContent[index][2],
                            messageContent[index][3], messageContent[index][4], messageContent[index][5],
                            messageContent[index][6], proCity[0],
                            proCity[1], proCity[2], proCity[3], proCity[4], proCity[5], proCity[6], status,
                            i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8]))
            else:
                # -12代表虽然包含了完成时，但是仍然不是要找的3个类别中的东西
                csvlist.append((
                    messageContent[index][0], messageContent[index][1], messageContent[index][2],
                    messageContent[index][3],
                    messageContent[index][4], messageContent[index][5], messageContent[index][6], proCity[0],
                    proCity[1],
                    proCity[2], proCity[3],
                    proCity[4], proCity[5], proCity[6],
                    -12, -1, -1, -1,
                    -1, -1, -1, -1, -1, -1))
                continue
        if len(csvlist) == 100:
            for record in csvlist:
                csvfile.write('|'.join(str(e) for e in record) + "\n")
            csvlist = []
    print "线程执行结束", param1, param2


# ---从这里开始是 main 函数入口
dbConenectReference = MySQLdb.connect(host='192.168.12.66', user='tigerreport', passwd='titmds4sp',
                                      db='TigerReport_production', use_unicode=True, charset='utf8')
executor = dbConenectReference.cursor()
executor.execute("""select id, name from sp_channels""")
sp_channels = executor.fetchall()
executor.execute("""select id, amount, name, dest_number, code from charge_codes""")
charge_codes = executor.fetchall()
for i in range(1, 140000000, 10000000):
    threadList = []
    for j in range(i, i + 10000000 - 1, 100000):
        start = j
        end = 100000 + j - 1
        # print "开始，结束", start, end
        threadList.append(threading.Thread(target=badyRun, args=(str(start), str(end))))
    # print "*" * 100
    for t in threadList:
        t.setDaemon(True)
        t.start()
    for t in threadList:
        t.join()
