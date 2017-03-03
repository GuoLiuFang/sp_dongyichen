# coding:utf-8
# print  "操你妈逼"
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import re
import os
import datetime

# 每个月某日把最近7个月的charge_code_id抓出来
os.system("""rm -rf /data/sdg/guoliufang/mysqloutfile/chargeCodeStatistic.txt""")
dbConenectMessage = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108', db='honeycomb',
                                    use_unicode=True, port=5209, charset='utf8')
messageExecutor = dbConenectMessage.cursor()
sql = """truncate charge_codes_statistics"""
messageExecutor.execute(sql)
dbConenectReference = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108',
                                      db='smart_production', use_unicode=True, port=5209, charset='utf8')
executor = dbConenectReference.cursor()
currentYue = datetime.datetime.now().strftime("%Y-%m")
halfYearAgo = (datetime.datetime.now() - datetime.timedelta(days=6 * 30)).strftime("%Y-%m")
end = """'""" + currentYue + """-01'"""
print end
start = """'""" + halfYearAgo + """-01'"""
print start
zhongyao = """select id, amount, name, dest_number, code from charge_codes where name like '%元%' and id in (select distinct charge_code_id from charge_code_deliver_details where created_at between """ + start + """ and """ + end + """)"""
print zhongyao
executor.execute(zhongyao)
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
                ("", charge_tuple[0], charge_tuple[1], charge_tuple[2].replace("\r\n", ""),
                 charge_tuple[3].replace("\r\n", ""),
                 charge_tuple[4].replace("\r\n", ""),
                 charge_code_instruc_no_t.replace("\r\n", ""),
                 code_list[index - 1].replace("\r\n", ""), union_name.rstrip()))
            break
for record in csvlist:
    csvfile.write('|'.join(str(e).strip(' \t\n\r') for e in record) + "\n")
csvfile.close()

os.system(
    """/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root -uroot -ptiger2108 honeycomb -e "load data infile '/data/sdg/guoliufang/mysqloutfile/chargeCodeStatistic.txt' into table charge_codes_statistics fields terminated by '|'" """)

removeTwoWordssql = """delete from charge_codes_statistics where length(trim(yewucode_name)) < 7"""
messageExecutor.execute(removeTwoWordssql)
