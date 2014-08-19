#coding=utf-8
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
import re, datetime
from pymongo import Connection
mongo_db = Connection()['db_'+env]

oid_regex = re.compile('oid=discovery\.(.{24})')

list_category_oid_regex = re.compile('oid=(category\..{24})')
list_newest_oid_regex = re.compile('oid=(stype.newest)')

event_page_id_regex = re.compile('\/event\/page\/([^\.\/]{24})')

def parse_time(time_str):
    return datetime.datetime.strptime(time_str, '%d/%b/%Y:%H:%M:%S')

def parse_key_time(time_str):
    return datetime.datetime.strptime(time_str, '%Y-%m-%d')

def format_time(_date):
    return _date.strftime('%Y-%m-%d')


class Handler():
    def __init__(self, url):
        self.url = url

    def discovery_click():
        url = self.url

        if url.startswith('/api/1/discovery/item'):
            if date_key not in data_dict:
                data_dict[date_key] = {
                    'daily_all_clicks': 0,
                    'daily_essence_clicks': 0
                }

            data = data_dict[date_key]
            # print url
            oid = oid_regex.findall(url)
            if len(oid) != 1: 
                print line
                return

            oid = oid[0]
            if oid not in discovery_dict:

                time.sleep(0.1)
                # print 'sleeping..'

                discovery = mongo_db.discovery.find_one({'_id':ObjectId(oid)}, {'level':1})
                if discovery:
                    level = discovery.get('level', 1)
                else:
                    level = 0

                discovery_dict[oid] = level
            else:
                level = discovery_dict[oid]

            daily_all_clicks += 1
            data['daily_all_clicks'] += 1

            if level >= 3:
                data['daily_essence_clicks'] += 1

    def discovery_list():
        url = self.url

        if url.find('start=5') >= 0: return
        
        if date_key not in list_oid_dict:
            list_oid_dict[date_key] = {}

        # print url
        oid = list_category_oid_regex.findall(url)
        if len(oid) != 1: 
            oid = list_newest_oid_regex.findall(url)

        if len(oid) != 1: return

        oid = oid[0]

        list_oid_daily_data = list_oid_dict[date_key]

        if oid not in list_oid_daily_data:
            list_oid_daily_data[oid] = 0

        list_oid_daily_data[oid] += 1

    def event_page_click():
        url = self.url

        if date_key not in event_page_data_dict:
            event_page_data_dict[date_key] = {}

        data = event_page_data_dict[date_key]
        # print url
        oid = event_page_id_regex.findall(url)
        if len(oid) != 1: 
            return

        oid = oid[0]
        if oid not in data:

            data[oid] = 1
        else:
            data[oid] += 1 


def test():
    pass

if __name__=='__main__':
    if len(sys.argv) == 2:
        if sys.argv[1] == 'test':
            test()