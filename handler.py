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
            discovery = prod_slave.discovery.find_one({'_id':ObjectId(oid)}, {'level':1})
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
        # print discovery_click_data

        for current_date_key in discovery_click_data:
            daily_data = discovery_click_data[current_date_key]

            for data_type_oid in daily_data:
                count = daily_data[data_type_oid]
                analysis_db.discovery_clicks.update({'date': current_date_key, 'oid': data_type_oid}, {'$inc': {'count': count}}, upsert=True)

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
        # print list_oid_click_data
        
        for current_date_key in list_oid_click_data:
            list_oid_daily_data = list_oid_click_data[current_date_key]

            for oid_key in list_oid_daily_data:
                count = list_oid_daily_data[oid_key]
                current_data = analysis_db.list_oid_clicks.update({'date': current_date_key, 'oid': oid_key}, {'$inc': {'count': count}}, upsert=True)

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
        # print event_page_click_data

        for current_date_key in event_page_click_data:
            event_page_daily_data = event_page_click_data[current_date_key]

            for event_page_id in event_page_daily_data:
                count = event_page_daily_data[event_page_id]
                event_page_oid_key = 'event_page.%s' % event_page_id

                current_data = analysis_db.event_page_clicks.update({'date': current_date_key, 'oid': event_page_oid_key}, {'$inc': {'count': count}}, upsert=True)

                try:
                    prod_master.event_page.update({'_id':ObjectId(event_page_id)}, {'$inc': {'click': count}})
                except Exception, e:
                    print e

    def event_page_weixin_share_inspector(self, event_page_fpid_data):
        url = self.url
        date_key = self.date_key

        if not url.startswith('/event/page/'): return

        result = re.search(event_page_id_regex, url)
        if not result: return

        oid = result.group(1)
        event_page_oid = oid

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
        # print event_page_fpid_data
        _num = 0
        
        for oid in event_page_fpid_data:
            temp_fpid_dict = event_page_fpid_data[oid]
            need_to_add_fpid_list = []

            fpid_list = list(temp_fpid_dict)

            event_page_oid = 'event_page.%s' % oid

            exists_fpid_records = analysis_db.event_share_record.find({'_type':'weixin', 'oid': event_page_oid, 'fpid': {'$in':fpid_list }}, {'fpid': 1})
            exists_fpid_dict = {}

            for record in exists_fpid_records:
                exists_fpid_dict[record['fpid']] = 1

            for x in fpid_list:
                if x not in exists_fpid_dict:
                    need_to_add_fpid_list.append(x)

            count = len(need_to_add_fpid_list)
            # print 'need to add: %s' % oid, count

            for fpid in need_to_add_fpid_list:
                analysis_db.event_share_record.insert({'_type':'weixin', 'oid': event_page_oid, 'fpid': fpid})

                _num += 1

                if _num == 100:
                    time.sleep(0.1)
                    _num = 0

            if count > 0:
                try:
                    prod_master.event_page.update({'_id': ObjectId(oid)}, {'$inc': {'weixin_share': count}})
                except Exception, e:
                    print e

def test():
    pass

if __name__=='__main__':
    if len(sys.argv) == 2:
        if sys.argv[1] == 'test':
            test()