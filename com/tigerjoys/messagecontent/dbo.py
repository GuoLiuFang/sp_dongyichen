# coding:utf-8
import MySQLdb

dbConenect = MySQLdb.connect(host='192.168.12.66', user='tigerreport', passwd='titmds4sp', db='TigerReport_production',
                             use_unicode=True)
executor = dbConenect.cursor()
executor.execute("""select * from sp_channels""")
data = executor.fetchone()
print str(data).decode(encoding='unicode_escape')
