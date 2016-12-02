# coding:utf-8
import re
message='订购提醒：您好！您已成功订购由北京中天华宇科技有限责任公司提供的税务杂志精编，10元/月（由中国移动代收费），72小时内退订免费。【发送6700至10086每月免费体验10GB咪咕视频定向流量，可以连续体验3个月！详情 http://url.cn/40C8anS 】'
sp_name='中天华宇'
pattern = sp_name+".*公司"
print pattern
if re.search(pattern, message):
    print "zhaodaole"
    print message