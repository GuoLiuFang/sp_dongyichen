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


def composeSp(spIDList):
    sp_id_list = ""
    sp_name_list = ""
    for spTuple in spIDList:
        sp_id_list = sp_id_list + str(spTuple[0]) + ","
        sp_name_list = sp_name_list + str(spTuple[1]) + ","
    if not len(sp_id_list) > 0:
        sp_id_list = "-1,"
    if not len(sp_name_list) > 0:
        sp_name_list = "-1,"
    return (sp_id_list[:-1], sp_name_list[:-1])


def noProvince():
    csvfile = open("/data/sdg/guoliufang/mysqloutfile/SpnoProvince.txt", mode='wa+')
    # csvfile = open("/Users/LiuFangGuo/Downloads/SpnoProvince.txt", mode='wa+')
    csvlist = []
    dbMysqlConn = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108', db='honeycomb',
                                  use_unicode=True, port=5209, charset='utf8')
    myExecutor = dbMysqlConn.cursor()
    smsSQL = """select extract(YEAR_MONTH FROM `record_time`) as yuefen, sp_id, sp_name, count(DISTINCT uuid) from message_analysises_sp where sp_id is not null and sp_id != -1 and sp_name is not null and sp_name != '-1' and status = 2 GROUP  BY yuefen,sp_id,sp_name"""
    myExecutor.execute(smsSQL)
    smsListTuple = myExecutor.fetchall()
    dbGpsqlConn = psycopg2.connect(database='tjdw', user='tj_root', password='77pbV1YU!T', host='192.168.12.14',
                                   port=5432)
    gpExecutor = dbGpsqlConn.cursor()
    for smsTuple in smsListTuple:
        # 0-yuefen,1-sp_id,2-sp_name,3-duamxin
        start_end = getFormatStartEnd(str(smsTuple[0]))
        # 这里面也就是union_name_list发生了变化
        unionNameSQL = "select distinct union_name from charge_codes_statistics where record_time BETWEEN " + start_end[
            0] + " and " + start_end[1] + " and sp_id = " + str(smsTuple[1])
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
        # compute the intersection
        xifaSql = "select distinct uuid from BDL.crab_code_histories  where record_time BETWEEN " + start_end[
            0] + " and " + start_end[
                      1] + " and code_union_name in ('" + inUnionNameListPartSQL + "') and code_event = 10"
        gpExecutor.execute(xifaSql)
        XiaList = gpExecutor.fetchall()
        lilunSql = "select distinct uuid from BDL.crab_code_histories  where record_time BETWEEN " + start_end[
            0] + " and " + start_end[
                       1] + " and code_union_name in ('" + inUnionNameListPartSQL + "') and code_event = 20"
        gpExecutor.execute(lilunSql)
        LiLunList = gpExecutor.fetchall()
        shijiSql = "select distinct uuid from BDL.crab_code_histories  where record_time BETWEEN " + start_end[
            0] + " and " + start_end[
                       1] + " and code_union_name in ('" + inUnionNameListPartSQL + "') and code_event = 30"
        gpExecutor.execute(shijiSql)
        ShiJiList = gpExecutor.fetchall()
        # 重新计算短信的内容
        duanxinSql = "select distinct uuid from message_analysises_sp  where record_time BETWEEN " + start_end[
            0] + " and " + start_end[
                         1] + " and  sp_id is not null and sp_id != -1 and sp_name is not null and sp_name = '" + \
                     smsTuple[2] + "' and status = 2 and sp_id = " + str(smsTuple[1])
        myExecutor.execute(duanxinSql)
        DuanXinList = myExecutor.fetchall()
        xiafaSet = set(XiaList)
        lilunSet = set(LiLunList)
        duanxinSet = set(DuanXinList)
        shijiSet = set(ShiJiList)
        xiafa_inner_lilun = len(xiafaSet.intersection(lilunSet))
        lilun_inner_sms = len(lilunSet.intersection(duanxinSet))
        sms_inner_shiji = len(duanxinSet.intersection(shijiSet))

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
        csvlist.append(
            (smsTuple[0], smsTuple[1], smsTuple[2], inUnionNameListPartSQL, -1, a10, xiafa_inner_lilun, a20,
             lilun_inner_sms, smsTuple[3], sms_inner_shiji, a30))
    for record in csvlist:
        csvfile.write('|'.join(str(e) for e in record) + "\n")
    csvfile.close()


def withProvince():
    csvfile = open("/data/sdg/guoliufang/mysqloutfile/SpwithProvince.txt", mode='wa+')
    # csvfile = open("/Users/LiuFangGuo/Downloads/SpwithProvince.txt", mode='wa+')
    csvlist = []
    dbMysqlConn = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108', db='honeycomb',
                                  use_unicode=True, port=5209, charset='utf8')
    myExecutor = dbMysqlConn.cursor()
    smsSQL = """select extract(YEAR_MONTH FROM `record_time`) as yuefen, sp_id, sp_name, province_id, count(DISTINCT uuid) from message_analysises_sp where sp_id is not null and sp_id != -1 and sp_name is not null and sp_name != '-1' and province_id != -1 and status = 2 GROUP  BY yuefen,sp_id,sp_name,province_id"""
    myExecutor.execute(smsSQL)
    smsListTuple = myExecutor.fetchall()
    dbGpsqlConn = psycopg2.connect(database='tjdw', user='tj_root', password='77pbV1YU!T', host='192.168.12.14',
                                   port=5432)
    gpExecutor = dbGpsqlConn.cursor()
    for smsTuple in smsListTuple:
        # 0-yuefen,1-sp_id,2-sp_name,3-province-id,4-duanxi
        start_end = getFormatStartEnd(str(smsTuple[0]))
        unionNameSQL = "select distinct union_name from charge_codes_statistics where record_time BETWEEN " + start_end[
            0] + " and " + start_end[1] + " and sp_id = " + str(smsTuple[1])
        myExecutor.execute(unionNameSQL)
        unionNameListTuple = myExecutor.fetchall()
        inUnionNameListPartSQL = ''
        for unionNameTuple in unionNameListTuple:
            inUnionNameListPartSQL = """','""".join(str(e) for e in unionNameTuple) + """','""" + inUnionNameListPartSQL
        inUnionNameListPartSQL = inUnionNameListPartSQL[:-3]
        gpXiaFaLiLunShiJiSQL = "select code_event,count(distinct uuid) from BDL.crab_code_histories  where record_time BETWEEN " + \
                               start_end[0] + " and " + start_end[
                                   1] + " and code_union_name in ('" + inUnionNameListPartSQL + "') and province_id = " + str(
            smsTuple[3]) + " group by code_event order by code_event"
        gpExecutor.execute(gpXiaFaLiLunShiJiSQL)
        XiaFaLiLunShiJiTupleList = gpExecutor.fetchall()
        # compute the intersection
        xifaSql = "select distinct uuid from BDL.crab_code_histories  where record_time BETWEEN " + start_end[
            0] + " and " + start_end[
                      1] + " and code_union_name in ('" + inUnionNameListPartSQL + "') and code_event = 10 and province_id = " + str(
            smsTuple[3])
        gpExecutor.execute(xifaSql)
        XiaList = gpExecutor.fetchall()
        lilunSql = "select distinct uuid from BDL.crab_code_histories  where record_time BETWEEN " + start_end[
            0] + " and " + start_end[
                       1] + " and code_union_name in ('" + inUnionNameListPartSQL + "') and code_event = 20 and province_id = " + str(
            smsTuple[3])
        LiLunList = gpExecutor.fetchall()
        shijiSql = "select distinct uuid from BDL.crab_code_histories  where record_time BETWEEN " + start_end[
            0] + " and " + start_end[
                       1] + " and code_union_name in ('" + inUnionNameListPartSQL + "') and code_event = 30 and province_id = " + str(
            smsTuple[3])
        gpExecutor.execute(shijiSql)
        ShiJiList = gpExecutor.fetchall()
        duanxinSql = "select distinct uuid from message_analysises_sp  where record_time BETWEEN " + start_end[
            0] + " and " + start_end[
                         1] + " and  sp_id is not null and sp_id != -1 and sp_name is not null and sp_name = '" + \
                     smsTuple[2] + "' and status = 2  and province_id = " + str(smsTuple[3]) + " and sp_id = " + \
                     str(smsTuple[1])
        myExecutor.execute(duanxinSql)
        DuanXinList = myExecutor.fetchall()
        xiafaSet = set(XiaList)
        lilunSet = set(LiLunList)
        duanxinSet = set(DuanXinList)
        shijiSet = set(ShiJiList)
        xiafa_inner_lilun = len(xiafaSet.intersection(lilunSet))
        lilun_inner_sms = len(lilunSet.intersection(duanxinSet))
        sms_inner_shiji = len(duanxinSet.intersection(shijiSet))

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
        csvlist.append((smsTuple[0], smsTuple[1], smsTuple[2], inUnionNameListPartSQL, smsTuple[3], a10,
                        xiafa_inner_lilun, a20, lilun_inner_sms, smsTuple[4], sms_inner_shiji, a30))
    for record in csvlist:
        csvfile.write('|'.join(str(e) for e in record) + "\n")
    csvfile.close()


noProvince()
os.system(
    """/usr/local/Calpont/bin/cpimport honeycomb sp_result_analysis -s '|' /data/sdg/guoliufang/mysqloutfile/SpnoProvince.txt""")
withProvince()
os.system(
    """/usr/local/Calpont/bin/cpimport honeycomb sp_result_analysis -s '|' /data/sdg/guoliufang/mysqloutfile/SpwithProvince.txt""")
