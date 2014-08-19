#coding=utf-8
from helper import *

class Handler():
    def __init__(self, url, date_key):
        self.url = url
        self.date_key = date_key

    def discovery_click_inspector(discovery_click_data, discovery_level_dict={}):
        url = self.url
        date_key = self.date_key

        if url.startswith('/api/1/discovery/item'):
            if date_key not in discovery_click_data:
                discovery_click_data[date_key] = {
                    'daily_all_clicks': 0,
                    'daily_essence_clicks': 0
                }

            daily_data = discovery_click_data[date_key]
            # print url
            result = re.search(discovery_oid_regex, url)
            if result:
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


    def discovery_list():
        url = self.url



    def event_page_click():
        url = self.url



def test():
    pass

if __name__=='__main__':
    if len(sys.argv) == 2:
        if sys.argv[1] == 'test':
            test()