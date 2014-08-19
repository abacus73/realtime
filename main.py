#coding=utf-8
import sys
from helper import *

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer

kafka = KafkaClient("10.160.9.106:9092")
consumer = SimpleConsumer(kafka, "my-group", "test")

slice_number = 1000
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
        # if url.startswith('/api/1/discovery/list') and url.find('start=5') < 0:

        #     if date_key not in list_oid_click_data:
        #         list_oid_click_data[date_key] = {}

        #     result = re.search(list_category_oid_regex, url)
        #     if not result: result = re.search(list_newest_oid_regex, url)
        #     if result:
        #         oid = result.group(1)

        #         list_oid_daily_data = list_oid_click_data[date_key]

        #         if oid not in list_oid_daily_data:
        #             list_oid_daily_data[oid] = 0

        #         list_oid_daily_data[oid] += 1

        # # 专题页点击
        # if url.startswith('/event/page/'):
        #     if date_key not in event_page_click_data:
        #         event_page_click_data[date_key] = {}

        #     daily_data = event_page_click_data[date_key]
        #     # print url
        #     result = re.search(event_page_id_regex, url)
        #     if result:
        #         oid = result.group(1)
        #         if oid not in daily_data:
        #             daily_data[oid] = 1
        #         else:
        #             daily_data[oid] += 1 

        #         ppid_result = re.search(ppid_regex, url)
        #         if ppid_result:
        #             event_page_oid = oid

        #             ppid = ppid_result.group(1)
        #             ppid = ppid_decrypt(ppid)

        #             fpid = ppid[0:32]

        #             if fpid != INIT_PPID:
        #                 if event_page_oid not in event_page_fpid_data:
        #                     event_page_fpid_data[event_page_oid] = {}

        #                 event_page_item_data = event_page_fpid_data[event_page_oid]
        #                 if fpid not in event_page_item_data:
        #                     event_page_item_data[fpid] = 1


        # 判断是否达到分页值，进行提交
        if num == slice_number:
            num = 0

            # 提交笔记点击数
            print discovery_click_data

            for current_key in discovery_click_data:
                current_data = analysis_db.clicks_json.find_one({'key': current_key})

                if not current_data:
                    current_data = {
                        'key': current_key
                    }
                    current_data.update(discovery_click_data[current_key])
                    # analysis_db.clicks_json.insert(current_data)
                else:
                    current_data['daily_all_clicks'] += discovery_click_data[current_key]['daily_all_clicks']
                    current_data['daily_essence_clicks'] += discovery_click_data[current_key]['daily_essence_clicks']
                    # analysis_db.clicks_json.update({'key': current_key}, current_data)

            discovery_click_data = {}

            # # 提交类别列表点击数
            # print list_oid_click_data
            # for current_key in list_oid_click_data:
            #     list_oid_daily_data = list_oid_click_data[current_key]

            #     current_time = parse_key_time(current_key)

            #     for oid_key in list_oid_daily_data:
            #         current_data = analysis_db.list_oid_json.find_one({'key': current_key, 'oid': oid_key})

            #         if not current_data:
            #             current_data = {
            #                 'key': current_key,
            #                 'time': current_time,
            #                 'oid': oid_key,
            #                 'count': list_oid_daily_data[oid_key]
            #             }
            #             # analysis_db.list_oid_json.insert(current_data)
            #         else:
            #             current_data['count'] += list_oid_daily_data[oid_key]
            #             # analysis_db.list_oid_json.update({'key': current_key}, current_data)
            # list_oid_click_data = {}

            # # 提交专题页点击数
            # print event_page_click_data
            # for current_key in event_page_click_data:
            #     data_current = event_page_click_data[current_key]

            #     for event_page_id in data_current:
            #         print event_page_id, data_current[event_page_id]
            #         # try:
            #         #     mongo_db.event_page.update({'_id':ObjectId(event_page_id)}, {'$inc': {'click': data_current[event_page_id]}})
            #         # except Exception, e:
            #         #     print e

            # event_page_click_data = {}

            # print event_page_fpid_data
            # # 提交专题页微信分享数
            # for oid in event_page_fpid_data:
            #     temp_fpid_dict = event_page_fpid_data[oid]
            #     need_to_add_fpid_list = []

            #     fpid_list = list(temp_fpid_dict)

            #     event_page_oid = 'event_page.%s' % oid

            #     exists_fpid_records = mongo_db.fpid_record.find({'oid': event_page_oid, 'fpid': {'$in':fpid_list }}, {'fpid': 1})
            #     exists_fpid_dict = {}

            #     for record in exists_fpid_records:
            #         exists_fpid_dict[record['fpid']] = 1

            #     for x in fpid_list:
            #         if x not in exists_fpid_dict:
            #             need_to_add_fpid_list.append(x)

            #     print 'need to add: %s' % oid, need_to_add_fpid_list

            #     # for fpid in need_to_add_fpid_list:
            #     #     mongo_db.fpid_record.insert({'oid': event_page_oid, 'fpid': fpid})

            #     # count = len(need_to_add_fpid_list)
            #     # if count > 0:
            #     #     mongo_db.event_page.update({'_id': ObjectId(oid)}, {'$inc': {'weixin_share': count}})


            # event_page_fpid_data = {}


kafka.close()




