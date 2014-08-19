#coding=utf-8

# ppid encrypt decrypt
from pyDes import *
from binascii import unhexlify as unhex
from binascii import hexlify as hex
ppid_key = triple_des("mulaoshi\r\n\tABC\r\n0987*54B", CBC, "12341234")
def ppid_encrypt(a): return hex(ppid_key.encrypt(unhex(a)))   
def ppid_decrypt(a): return hex(ppid_key.decrypt(unhex(a)))

INIT_PPID = '0'*32
