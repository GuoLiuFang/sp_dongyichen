# coding:utf-8
# print  "操你妈逼"
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import re
import os

os.system("""rm -rf /data/sdg/guoliufang/mysqloutfile/chargeCodeStatistic.txt""")
dbConenectMessage = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108', db='honeycomb',
                                    use_unicode=True, port=5209, charset='utf8')
messageExecutor = dbConenectMessage.cursor()
sql = """truncate charge_codes_statistic"""
messageExecutor.execute(sql)
dbConenectReference = MySQLdb.connect(host='192.168.12.66', user='tigerreport', passwd='titmds4sp',
                                      db='TigerReport_production', use_unicode=True, charset='utf8')
executor = dbConenectReference.cursor()
executor.execute(
    """select id, amount, name, dest_number, code from charge_codes where name like '%元%'""")
charge_codes = executor.fetchall()
csvfile = open("/data/sdg/guoliufang/mysqloutfile/chargeCodeStatistic.txt", mode='wa+')
# csvfile = open("/Users/LiuFangGuo/Downloads/chargeCodeStatistic.txt", mode='wa+')
csvlist = []
for charge_tuple in charge_codes:
    sp_charge_str = charge_tuple[2].encode(encoding='utf-8')
    code_list = sp_charge_str.split('-')
    for index in range(len(code_list)):
        tmp = re.match("""\d+元""", code_list[index])
        if tmp:
            charge_code_instruc_no_t = charge_tuple[4].replace("""*#T""", """""").replace("\r\n", "")
            union_name = charge_code_instruc_no_t + "_" + charge_tuple[3].replace("\r\n", "")
            csvlist.append(
                (charge_tuple[0], charge_tuple[1], charge_tuple[2].replace("\r\n", ""),
                 charge_tuple[3].replace("\r\n", ""),
                 charge_tuple[4].replace("\r\n", ""),
                 charge_code_instruc_no_t.replace("\r\n", ""),
                 code_list[index - 1].replace("\r\n", ""), union_name.rstrip()))
            break
for record in csvlist:
    csvfile.write('|'.join(str(e) for e in record) + "\n")
csvfile.close()
os.system(
    """/usr/local/Calpont/bin/cpimport honeycomb charge_codes_statistic -s '|' /data/sdg/guoliufang/mysqloutfile/chargeCodeStatistic.txt""")
