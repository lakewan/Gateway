# coding=utf-8
#import ConfigParser
from configparser import SafeConfigParser
import os
from datetime import datetime
import binascii

curdir = os.getcwd() 

def read_conf(section,item):
    sfile=curdir+"\\config.ini"
#   confg= configparser.ConfigParser()
    confg= SafeConfigParser()
    try:
        confg.read(sfile)
        rtval=confg.get(section,item)
        return rtval
    except Exception as e :
        print("READ CONFIGURE FILE FAILURE:",e)
        

def create_log(logstr):
    print(logstr+'\n')
    path = os.getcwd()+'\\log\\'
    filename= path+datetime.now().strftime('%Y-%m-%d') + '.' + "txt"
    if not os.path.exists(path):
        os.makedirs(path)
    try:
        log_file=open(filename,'a')
        loginfo= logstr + '\n'
        log_file.write(loginfo)
        log_file.close()
    except Exception as e :
        print("create log file failure:", e )
 
def crc16(instr,HtoL):
    data = bytearray.fromhex(instr)
    crc = 0xFFFF
    for pos in data:
        crc ^= pos
        for i in range(8):
            if ((crc & 1) != 0):
                crc >>= 1
                crc ^= 0xA001
            else:
                crc >>= 1
    rvalue = hex(((crc & 0xff) << 8) + (crc >> 8))
    if rvalue[:2] == '0x' or rvalue[:2] == '0X':
       rvalue=rvalue[2:]
    rvalue=('0000'+rvalue)[-4:] 
    if HtoL:
       rvalue=rvalue[2:]+ ' ' +rvalue[:2] 
    else:
        rvalue=rvalue[:2]+ ' ' +rvalue[2:]
    return rvalue      
  
def getcurrtime():
    curtime=datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
    return curtime

def hex2signal(datastr): #16进制有符号数转10进制 大端数据，高位在前
    data = datastr # hex data
    try:
        width = len(datastr)*4;
    except IndexError:
        width = 8
    dec_data = int(data, 16);
    if(dec_data>2**(width-1)-1):
        dec_data = (2**width-dec_data)*(-1);
    return dec_data
#查找对象列表中属性为特定值的对象的列表索引

   
    
