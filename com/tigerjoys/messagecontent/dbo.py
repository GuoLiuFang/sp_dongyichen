# coding:utf-8
import MySQLdb
from chenggongdinggou import getProCity
from suds.client import Client

dbConenect = MySQLdb.connect(host='192.168.12.66', user='tigerreport', passwd='titmds4sp', db='TigerReport_production',
                             use_unicode=True)
executor = dbConenect.cursor()
executor.execute("""select * from sp_channels""")
data = executor.fetchone()
# print str(data).decode(encoding='unicode_escape')
wsdl_url = """http://panda.didiman.com:82/Panda/LocationWebService?wsdl"""
client = Client(wsdl_url)
sc = ''
rimsi = ''
getProCity(sc, rimsi)
