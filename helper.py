#coding=utf-8
from pymongo import Connection
from bson.objectid import ObjectId

import re

import datetime, time

from pyDes import *
from binascii import unhexlify as unhex
from binascii import hexlify as hex

# 数据库相关
mongo_db = Connection()['db_prod']
analysis_db = Connection()['db_analysis_prod']

def discovery_level_data_init():
    discovery_dict = {}

    d_current = datetime.datetime.now()
    discovery_dict = {}
    search_dict = {
        'time': {
            '$gte': d_current - datetime.timedelta(days=3),
            '$lte': d_current
        }
    }
    discoveries = mongo_db.discovery.find(search_dict, {'level':1})
    for discovery in discoveries:
        level = discovery.get('level', 1)
        discovery_dict[str(discovery['_id'])] = level

    return discovery_dict

# 正则相关
time_url_regex = r'\[(.+)\s\+0800\]\s"GET\s(.+)\sHTTP'
discovery_oid_regex = r'oid=discovery\.([a-z0-9]{24})'
list_category_oid_regex = r'oid=(category\.[a-z0-9]{24})'
list_newest_oid_regex = r'oid=(stype.newest)'
event_page_id_regex = r'\/event\/page\/([a-z0-9]{24})'
ppid_regex = r'ppid=([a-z0-9]{64})'


# 时间处理相关
def parse_time(time_str):
    return datetime.datetime.strptime(time_str, '%d/%b/%Y:%H:%M:%S')
def parse_key_time(time_str):
    return datetime.datetime.strptime(time_str, '%Y-%m-%d')
def format_time(_date):
    return _date.strftime('%Y-%m-%d')


# ppid加解密相关
ppid_key = triple_des("mulaoshi\r\n\tABC\r\n0987*54B", CBC, "12341234")
def ppid_encrypt(a): return hex(ppid_key.encrypt(unhex(a)))   
def ppid_decrypt(a): return hex(ppid_key.decrypt(unhex(a)))

INIT_PPID = '0'*32
