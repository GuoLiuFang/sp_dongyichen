# coding:utf-8
import re
pattern = "您已成功订购(?P<action>.*?)的(?P<yewu>.*?)，"
mather = re.compile(pattern)
result = mather.findall("订购提醒：您好！您已成功订购北京中天华宇科技有限责任公司公司的的涉税查询业务，从2016年11月01日开始生效,信息费3.0元/月(由中国移动代收)，72小时内退订免费。中国移动倡导透明消费，业务查询和退订可编辑短信0000至10086，业务退订24小时内生效。我们一直努力，为您十分满意！中国移动")
print str(result).decode(encoding='string_escape')