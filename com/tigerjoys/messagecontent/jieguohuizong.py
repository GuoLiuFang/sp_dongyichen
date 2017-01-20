# coding:utf-8
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import psycopg2
from calendar import monthrange
import os


def getFormatStartEnd(yuefen):
    y = yuefen[:4]
    m = yuefen[4:]
    lastday = monthrange(int(y), int(m))
    start = """'""" + str(y) + "-" + str(m) + "-01'"
    end = """'""" + str(y) + "-" + str(m) + "-" + str(lastday[1]) + """ 23:59:59'"""
    return (start, end)


def noProvince():
    csvfile = open("/data/sdg/guoliufang/mysqloutfile/noProvince.txt", mode='wa+')
    # csvfile = open("/Users/LiuFangGuo/Downloads/noProvince.txt", mode='wa+')
    csvlist = []
    dbMysqlConn = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108', db='honeycomb',
                                  use_unicode=True, port=5209, charset='utf8')
    myExecutor = dbMysqlConn.cursor()
    smsSQL = """select extract(YEAR_MONTH FROM `record_time`) as yuefen, yewucode_name, count(DISTINCT uuid) from message_analysises where yewucode_name is not null and yewucode_name != '-1' and status = 2 GROUP  BY yuefen,yewucode_name"""
    myExecutor.execute(smsSQL)
    smsListTuple = myExecutor.fetchall()
    dbGpsqlConn = psycopg2.connect(database='tjdw', user='tj_root', password='77pbV1YU!T', host='192.168.12.14',
                                   port=5432)
    gpExecutor = dbGpsqlConn.cursor()
    for smsTuple in smsListTuple:
        # 0-yuefen,1-yewu,2-duanxin
        start_end = getFormatStartEnd(str(smsTuple[0]))
        unionNameSQL = """select distinct union_name from charge_codes_statistics where yewucode_name = '""" + smsTuple[
            1] + """'"""
        myExecutor.execute(unionNameSQL)
        unionNameListTuple = myExecutor.fetchall()
        inUnionNameListPartSQL = ''
        for unionNameTuple in unionNameListTuple:
            inUnionNameListPartSQL = """','""".join(str(e) for e in unionNameTuple) + """','""" + inUnionNameListPartSQL
        inUnionNameListPartSQL = inUnionNameListPartSQL[:-3]
        gpXiaFaLiLunShiJiSQL = "select code_event,count(distinct uuid) from BDL.crab_code_histories  where record_time BETWEEN " + \
                               start_end[0] + " and " + start_end[
                                   1] + " and code_union_name in ('" + inUnionNameListPartSQL + "') group by code_event order by code_event"
        gpExecutor.execute(gpXiaFaLiLunShiJiSQL)
        XiaFaLiLunShiJiTupleList = gpExecutor.fetchall()
        a10 = 0
        a20 = 0
        a30 = 0
        for rTuple in XiaFaLiLunShiJiTupleList:
            if rTuple[0] == 10:
                a10 = rTuple[1]
            elif rTuple[0] == 20:
                a20 = rTuple[1]
            elif rTuple[0] == 30:
                a30 = rTuple[1]
        inUnionNameListPartSQL = "'" + inUnionNameListPartSQL + "'"
        csvlist.append((smsTuple[0], smsTuple[1], inUnionNameListPartSQL, -1, a10, a20, smsTuple[2], a30))
    for record in csvlist:
        csvfile.write('|'.join(str(e) for e in record) + "\n")
    csvfile.close()


def withProvince():
    csvfile = open("/data/sdg/guoliufang/mysqloutfile/withProvince.txt", mode='wa+')
    # csvfile = open("/Users/LiuFangGuo/Downloads/withProvince.txt", mode='wa+')
    csvlist = []
    dbMysqlConn = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108', db='honeycomb',
                                  use_unicode=True, port=5209, charset='utf8')
    myExecutor = dbMysqlConn.cursor()
    smsSQL = """select extract(YEAR_MONTH FROM `record_time`) as yuefen, yewucode_name, province_id, count(DISTINCT uuid) from message_analysises where yewucode_name is not null and yewucode_name != '-1' and province_id != -1 and status = 2 GROUP  BY yuefen,yewucode_name,province_id"""
    myExecutor.execute(smsSQL)
    smsListTuple = myExecutor.fetchall()
    dbGpsqlConn = psycopg2.connect(database='tjdw', user='tj_root', password='77pbV1YU!T', host='192.168.12.14',
                                   port=5432)
    gpExecutor = dbGpsqlConn.cursor()
    for smsTuple in smsListTuple:
        # 0-yuefen,1-yewu,3-duanxin,2-province-id
        start_end = getFormatStartEnd(str(smsTuple[0]))
        unionNameSQL = """select distinct union_name from charge_codes_statistics where yewucode_name = '""" + smsTuple[
            1] + """'"""
        myExecutor.execute(unionNameSQL)
        unionNameListTuple = myExecutor.fetchall()
        inUnionNameListPartSQL = ''
        for unionNameTuple in unionNameListTuple:
            inUnionNameListPartSQL = """','""".join(str(e) for e in unionNameTuple) + """','""" + inUnionNameListPartSQL
        inUnionNameListPartSQL = inUnionNameListPartSQL[:-3]
        gpXiaFaLiLunShiJiSQL = "select code_event,count(distinct uuid) from BDL.crab_code_histories  where record_time BETWEEN " + \
                               start_end[0] + " and " + start_end[
                                   1] + " and code_union_name in ('" + inUnionNameListPartSQL + "') and province_id = " + str(
            smsTuple[2]) + " group by code_event order by code_event"
        gpExecutor.execute(gpXiaFaLiLunShiJiSQL)
        XiaFaLiLunShiJiTupleList = gpExecutor.fetchall()
        a10 = 0
        a20 = 0
        a30 = 0
        for rTuple in XiaFaLiLunShiJiTupleList:
            if rTuple[0] == 10:
                a10 = rTuple[1]
            elif rTuple[0] == 20:
                a20 = rTuple[1]
            elif rTuple[0] == 30:
                a30 = rTuple[1]
        inUnionNameListPartSQL = "'" + inUnionNameListPartSQL + "'"
        csvlist.append((smsTuple[0], smsTuple[1], inUnionNameListPartSQL, smsTuple[2], a10, a20, smsTuple[3], a30))
    for record in csvlist:
        csvfile.write('|'.join(str(e) for e in record) + "\n")
    csvfile.close()


noProvince()
os.system(
    """/usr/local/Calpont/bin/cpimport honeycomb jieguohuizong -s '|' /data/sdg/guoliufang/mysqloutfile/noProvince.txt""")
withProvince()
os.system(
    """/usr/local/Calpont/bin/cpimport honeycomb jieguohuizong -s '|' /data/sdg/guoliufang/mysqloutfile/withProvince.txt""")
