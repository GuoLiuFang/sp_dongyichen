# -*-coding:utf-8-*-
from pyspark import SparkContext
from dealWith import *
sc = SparkContext()
sourceData = sc.textFile("/data/logcenter/wolf/wolf_access.2016-10-07.log")
filterTagData = sourceData.filter(lambda line: ("paywg_dcby_mr" in line) and ("linkid=" in line))
caonima=filterTagData.map(lambda line: includeTag(line) if ("actionId=" in line) else notIncludeTag(line) )
print caonima.first()
