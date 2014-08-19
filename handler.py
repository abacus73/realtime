#coding=utf-8
from helper import *

class Handler():
    def __init__(self, url, date_key):
        self.url = url
        self.date_key = date_key

    def discovery_click_inspector(self, discovery_click_data, discovery_level_dict={}):
        url = self.url
        date_key = self.date_key

        if not url.startswith('/api/1/discovery/item'): return

        if date_key not in discovery_click_data:
            discovery_click_data[date_key] = {
                'daily_all_clicks': 0,
                'daily_essence_clicks': 0
            }

        daily_data = discovery_click_data[date_key]
        # print url
        result = re.search(discovery_oid_regex, url)
        if not result: return

        oid = result.group(1)

        if oid not in discovery_level_dict:
            discovery = mongo_db.discovery.find_one({'_id':ObjectId(oid)}, {'level':1})
            if discovery:
                level = discovery.get('level', 1)
            else:
                level = 0

            discovery_level_dict[oid] = level
        else:
            level = discovery_level_dict[oid]

        daily_data['daily_all_clicks'] += 1

        if level >= 3:
            daily_data['daily_essence_clicks'] += 1

    def discovery_click_handler(self, discovery_click_data):
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


    def discovery_list_inspector(self, list_oid_click_data):
        url = self.url
        date_key = self.date_key

        if not url.startswith('/api/1/discovery/list') or url.find('start=5') >= 0: return

        if date_key not in list_oid_click_data:
            list_oid_click_data[date_key] = {}

        result = re.search(list_category_oid_regex, url)
        if not result: result = re.search(list_newest_oid_regex, url)
        if not result: return

        oid = result.group(1)

        list_oid_daily_data = list_oid_click_data[date_key]

        if oid not in list_oid_daily_data:
            list_oid_daily_data[oid] = 0

        list_oid_daily_data[oid] += 1

    def discovery_list_handler(self, list_oid_click_data):
        print list_oid_click_data
        
        for current_key in list_oid_click_data:
            list_oid_daily_data = list_oid_click_data[current_key]

            current_time = parse_key_time(current_key)

            for oid_key in list_oid_daily_data:
                current_data = analysis_db.list_oid_json.find_one({'key': current_key, 'oid': oid_key})

                if not current_data:
                    current_data = {
                        'key': current_key,
                        'time': current_time,
                        'oid': oid_key,
                        'count': list_oid_daily_data[oid_key]
                    }
                    # analysis_db.list_oid_json.insert(current_data)
                else:
                    current_data['count'] += list_oid_daily_data[oid_key]
                    # analysis_db.list_oid_json.update({'key': current_key}, current_data)

    def event_page_click_inspector(self, event_page_click_data):
        url = self.url
        date_key = self.date_key

        if not url.startswith('/event/page/'): return

        if date_key not in event_page_click_data:
            event_page_click_data[date_key] = {}

        daily_data = event_page_click_data[date_key]

        result = re.search(event_page_id_regex, url)
        if not result: return

        oid = result.group(1)
        if oid not in daily_data:
            daily_data[oid] = 1
        else:
            daily_data[oid] += 1 

    def event_page_click_handler(self, event_page_click_data):
        print event_page_click_data

        for current_key in event_page_click_data:
            data_current = event_page_click_data[current_key]

            for event_page_id in data_current:
                print event_page_id, data_current[event_page_id]
                # try:
                #     mongo_db.event_page.update({'_id':ObjectId(event_page_id)}, {'$inc': {'click': data_current[event_page_id]}})
                # except Exception, e:
                #     print e

    def event_page_weixin_share_inspector(self, event_page_fpid_data):
        url = self.url
        date_key = self.date_key

        if not url.startswith('/event/page/'): return

        result = re.search(event_page_id_regex, url)
        if not result: return

        oid = result.group(1)
        event_page_oid = oid

        print url

        ppid_result = re.search(ppid_regex, url)
        if not ppid_result: return

        ppid = ppid_result.group(1)
        ppid = ppid_decrypt(ppid)

        fpid = ppid[0:32]


        if fpid == INIT_PPID: return

        if event_page_oid not in event_page_fpid_data:
            event_page_fpid_data[event_page_oid] = {}

        event_page_item_data = event_page_fpid_data[event_page_oid]
        if fpid not in event_page_item_data:
            event_page_item_data[fpid] = 1

    def event_page_weixin_share_handler(self, event_page_fpid_data):
        print event_page_fpid_data
        
        for oid in event_page_fpid_data:
            temp_fpid_dict = event_page_fpid_data[oid]
            need_to_add_fpid_list = []

            fpid_list = list(temp_fpid_dict)

            event_page_oid = 'event_page.%s' % oid

            exists_fpid_records = mongo_db.fpid_record.find({'oid': event_page_oid, 'fpid': {'$in':fpid_list }}, {'fpid': 1})
            exists_fpid_dict = {}

            for record in exists_fpid_records:
                exists_fpid_dict[record['fpid']] = 1

            for x in fpid_list:
                if x not in exists_fpid_dict:
                    need_to_add_fpid_list.append(x)

            print 'need to add: %s' % oid, need_to_add_fpid_list

            # for fpid in need_to_add_fpid_list:
            #     mongo_db.fpid_record.insert({'oid': event_page_oid, 'fpid': fpid})

            # count = len(need_to_add_fpid_list)
            # if count > 0:
            #     mongo_db.event_page.update({'_id': ObjectId(oid)}, {'$inc': {'weixin_share': count}})

def test():
    pass

if __name__=='__main__':
    if len(sys.argv) == 2:
        if sys.argv[1] == 'test':
            test()