#coding=utf-8
import sys
from handler import *

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer

kafka = KafkaClient("10.160.9.106:9092")
consumer = SimpleConsumer(kafka, "my-group", "test")

slice_number = 10000
num = 0

# 笔记点击数据
discovery_click_data = {}
# 类别列表点击数据
list_oid_click_data = {}
# 专题页点击数据
event_page_click_data = {}
# 专题页fpid数据 
event_page_fpid_data = {}

# 初始化一些笔记的level
discovery_dict = discovery_level_data_init()

for msg in consumer:
    result = re.search(time_url_regex, str(msg))
    
    if result:
        num += 1

        log_time_str = result.group(1)
        log_time = parse_time(log_time_str)
        date_key = format_time(log_time)

        url = result.group(2)

        url_handler = Handler(url, date_key)
        # print log_time, url

        # 笔记点击
        url_handler.discovery_click_inspector(discovery_click_data, discovery_level_dict=discovery_dict)

        # # 类别列表点击
        # url_handler.discovery_list_inspector(list_oid_click_data)

        # # 专题页点击
        # url_handler.event_page_click_inspector(event_page_click_data)

        # # 专题页微信分享
        # url_handler.event_page_weixin_share_inspector(event_page_fpid_data)


        # 判断是否达到分页值，进行提交
        if num == slice_number:
            num = 0
            
            # 提交笔记点击数
            url_handler.discovery_click_handler(discovery_click_data)
            discovery_click_data = {}

            # # 提交类别列表点击数
            # url_handler.discovery_list_handler(list_oid_click_data)
            # list_oid_click_data = {}

            # # 提交专题页点击数
            # url_handler.event_page_click_handler(event_page_click_data)
            # event_page_click_data = {}

            # # 提交专题页微信分享数
            # url_handler.event_page_weixin_share_handler(event_page_fpid_data)
            # event_page_fpid_data = {}


kafka.close()




