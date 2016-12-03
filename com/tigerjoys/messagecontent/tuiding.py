# coding=utf-8
# print "草拟吗"
import re
message = """(1/2)您已成功定制联通宽带在线有限公司5575(10655575102)的10元给力付包月业务，发送TD10到10655575102退订"""
code="""10元"""
result = re.match("""\d+元""",code)
if result:
    print result.group()
else:
    print "没有匹配到"