# coding:utf-8
print "草拟吗"
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import psycopg2
from calendar import monthrange


def getDNLsql(yuefen):
    y = yuefen[:4]
    print y
    m = yuefen[4:]
    print m
    lastday = monthrange(int(y), int(m))
    start = """'""" + str(y) + "-" + str(m) + "-01'"
    print start
    end = """'""" + str(y) + "-" + str(m) + "-" + str(lastday[1]) + """ 23:59:59'"""
    print end
    print lastday
    return (start, end)


def noProvince():
    csvfile = open("/data/sdg/guoliufang/mysqloutfile/noProvince.txt", mode='wa+')
    csvlist = []
    dbConenectMessageAnalysises = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108',
                                                  db='honeycomb',
                                                  use_unicode=True, port=5209, charset='utf8')
    maExecutor = dbConenectMessageAnalysises.cursor()
    sql_getDuanXin = """select extract(YEAR_MONTH FROM `record_time`) as yuefen,yewucode_name,count(DISTINCT uuid) from message_analysises GROUP  BY yuefen,yewucode_name limit 10"""
    maExecutor.execute(sql_getDuanXin)
    yewumaHuizong = maExecutor.fetchall()
    dbConnectPostGre = psycopg2.connect(database='tjdw', user='tj_root', password='77pbV1YU!T', host='192.168.12.14',
                                        port=5432)
    postCur = dbConnectPostGre.cursor()

    for tuple in yewumaHuizong:
        # print tuple[0], tuple[1], tuple[2]
        # 0-yuefen,1-yewu,2-duanxin
        start_end = getDNLsql(str(tuple[0]))
        dest_num_sql = """select distinct union_name from message_analysises where record_time BETWEEN """ + start_end[
            0] + " and " + start_end[1] + " and yewucode_name = '" + tuple[1] + "'"
        print dest_num_sql
        maExecutor.execute(dest_num_sql)
        dest_num_list = maExecutor.fetchall()[0]
        print dest_num_list
        # 插入数据库的列表
        code_dest_list = ','.join(str(e) for e in dest_num_list)
        print "要插入code_des_num_list_sql", code_dest_list
        dest_nums = """','""".join(str(e) for e in dest_num_list)
        print '*' * 30
        print dest_nums
        gp_sql = "select code_event,count(distinct uuid) from BDL.crab_code_histories  where record_time BETWEEN " + \
                 start_end[0] + " and " + start_end[
                     1] + " and code_union_name in ('" + dest_nums + "') group by code_event order by code_event"
        print gp_sql
        postCur.execute(gp_sql)
        rows = postCur.fetchall()
        print "&&&&&&&&before" * 10
        print rows
        a10 = 0
        a20 = 0
        a30 = 0
        for rtuple in rows:
            if rtuple[0] == 10:
                a10 = rtuple[1]
                print a10
            elif rtuple[0] == 20:
                a20 = rtuple[1]
                print a20
            elif rtuple[0] == 30:
                a30 = rtuple[1]
                print a30
        print "&&&&&&&&after" * 10
        csvlist.append((tuple[0], tuple[1], code_dest_list, '', a10, a20, tuple[2], a30))
    for record in csvlist:
        csvfile.write('|'.join(str(e) for e in record) + "\n")
    csvfile.close()


# def withProvince():


# dbConenectMessage = MySQLdb.connect(host='192.168.12.155', user='guoliufang', passwd='tiger2108', db='honeycomb',
#                                     use_unicode=True, port=5209, charset='utf8'))
# messageExecutor = dbConenectMessage.cursor()
# sql = """select * from honeycomb.sms_received_histories_all_thread where content is not null and record_time between """ + start + """ and """ + end
# # print sql
# messageExecutor.execute(sql)
# messageContent = messageExecutor.fetchall()
noProvince()
