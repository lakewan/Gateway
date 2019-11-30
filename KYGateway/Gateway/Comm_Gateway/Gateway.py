# coding=utf-8
import tkinter as tk
from threading import Thread as myThread
import comm_module as mymodule
from socket import socket,AF_INET,SOCK_STREAM
import pymysql 
from DBUtils.PooledDB import PooledDB
from datetime import datetime,timedelta 
from struct import unpack
from binascii import hexlify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from time import sleep


class GUI:
    def __init__(self, root):
        self.root = root
        self.leftFrame = tk.Frame(self.root, width=30, height=30)
        self.leftFrame.grid(row=0, column=0)
        self.rightFrame = tk.Frame(self.root, width=65, height=30)
        self.rightFrame.grid(row=0, column=1)
        tk.Label(self.leftFrame, text='\已连接').grid(row=0, column=0)
        self.listBox = tk.Listbox(self.leftFrame, width=20, height=20)
        self.listBox.grid(row=1, column=0)
        self.entry = tk.Entry(self.rightFrame, font=('Serief', 18), width=30)
        self.entry.grid(row=0, column=0)
        self.sendBtn = tk.Button(self.rightFrame, text='发送',  command=self.send, width=5)
        self.sendBtn.grid(row=0, column=1)
        tk.Label(self.rightFrame, text='通讯').grid(row=1,column=0)
        self.infoList = tk.Text(self.rightFrame, width=58, height=25)
        self.infoList.grid(row=2, columnspan=2)
        self.infoList.tag_config('name', background='yellow', foreground='red')
        self.infoList.tag_config('conment', background='black', foreground='white')
    def send(self):
        _index = self.listBox.curselection() # 01 03 00 00 00 10 44 06
        if _index:
            sData = self.entry.get()
            sData =sData.replace(' ','')
            conn_list[_index[0]].sock.sendall(bytes().fromhex(sData.replace(' ','')))
            
buffsize=1024
#全局变量
s = socket(AF_INET, SOCK_STREAM)

#数据库连接信息
linkhost= mymodule.read_conf('DB','host') 
linkdb=mymodule.read_conf('DB','database')
linkuser=mymodule.read_conf('DB','user')
linkpwd=mymodule.read_conf('DB','pwd')
linkport=mymodule.read_conf('DB','port')
linkcharset = mymodule.read_conf('DB','charset')
default_overtime=int(mymodule.read_conf('SERVER','default_overtime'))  #超时时间
default_collecttime=int(mymodule.read_conf('SERVER','default_collecttime')) #获取任务
default_statetime=int(mymodule.read_conf('SERVER','default_statetime')) #获取设备状态
default_ordertime=int(mymodule.read_conf('SERVER','default_ordertime')) #获取设备状态
commtype_valid = int(mymodule.read_conf('SERVER','commtype')) #获取设备状态

#设备信息
comm_list=[]  #网关数组
controller_list=[]   #控制器数组
sensor_list=[] #传感器数据
conn_list = []#客户端连接信息，端口号，地址，序列号，超时时间
unknow_list = []

#recv_thread=[] #连接线程数组
#send_thread=[] #发指令线程数组
#collect_thread=[] #采集数据线程数组
#getstate_thread=[] #采集数据线程数组            

            
#生成MySQL连接池        
try:
    mysqlpool=PooledDB(
        creator=pymysql,
        maxconnections=10,
        mincached=2,        #最小的空闲连接
        maxcached=4,        #最大的空闲连接
        maxshared=0,        #当连接数达到这个数，新请求的连接会分享已经分配出去的连接
        blocking=True,
        host=linkhost,
        port=int(linkport),
        user=linkuser,
        password=linkpwd,
        charset='utf8',
        db=linkdb
        )
except Exception as e:
    showinfo=mymodule.getcurrtime()+' database link failure:'+str(e)
    mymodule.create_log(showinfo)
            
def createGUI():
    global gui    
    root = tk.Tk()    #创建TK窗口
    gui = GUI(root) #调用窗口函数，布局和构建
    root.title('恺易网关') #标题
    root.mainloop() #获取界面事件
    
#通讯设备：ID,编号，序列号，通讯地址，公司ID，类型，参数,类别，控制通道最大数量，传感通道最大通道数
class comm_class():
    def __init__(self,commid,commcode,serial_num,commaddr,companyid,commtype,commpara,commclass,controlmax,sensormax):
        self.commid = commid            #通讯设备id
        self.commcode = commcode        #编号：水肥机没有编号，以ID代替
#        self.commname = commname       #名称    
        self.serial_num = serial_num    #序列号
        self.commaddr = commaddr        #通讯地址      
        self.companyid = companyid      #所属公司ID
        self.commtype = commtype        #类型：PLC、KLC、FKC、XPC、YYC、SFJ-1200、SFJ-0804
#        self.commclass= commclass      #设备类别，水肥机：KY2016-A，KY2016-B，通讯设备：FKC,KLC,PLC,XPH,
        self.commpara= commpara         #参数，水肥机-肥液路数，通讯设备-采集间隔
        self.commclass= commclass       #设备类别：1-物联网设备，2-水肥机，3-棚博士        
        self.controlmax= controlmax     #控制通道最大通道数
        self.sensormax= sensormax       #传感器通道对到通道数
        
#控制器：ID,编号，地址，类型，所属网关序号、设备参数、设备地块、设备公式、类别    
class controller_class():
    def __init__(self,devid,devcode,devaddr,devtype,commnum,devpara,blockid,devformula,devclass):
        self.devid = devid          #设备id
        self.devcode = devcode      #设备编号，水肥机没有编号，以ID代替
#        self.devname=devname        #设备名称
        self.devaddr = devaddr      #设备地址   
        self.devtype = devtype      #设备类型:1-开关，2-脉冲，3-行程
        self.devpara = devpara      #设备参数：控制器-行程时长     
        self.commnum = commnum      #所属网关序列号     
        self.blockid=blockid        #设备地块
        self.devformula=devformula  #0
#        self.commpanyid=companyid   #公司id
        self.devclass=devclass      #10-物联网设备，20-水肥机，30-棚博士

#传感器：ID,编号，地址，类型，所属网关序号、设备参数、设备地块、设备公式、类别    
class sensor_class():
    def __init__(self,devid,devcode,devaddr,devtype,commnum,devpara,blockid,formula,devclass):
        self.devid = devid          #设备id
        self.devcode = devcode      #设备编号，水肥机没有编号，以ID代替
#        self.devname=devname        #设备名称
        self.devaddr = devaddr      #设备地址   
        self.devtype = devtype      #设备类型:1-数字，2-4-20ma电流，3-0-20ma电流，4-0-5v电压，5-0-10v,6-高低电平
        self.devpara = devpara      #设备参数,传感器-调整值        
        self.commnum = commnum      #所属网关序列号     
        self.blockid=blockid        #设备地块
        self.formula=formula  #公司：WENDU、SHIDU、Q-WENDU、Q-SHIDU、CO2、BEAM、EC、PH、FS、FX、YL、QY、OPR
#        self.commpanyid=companyid   #公司id
        self.devclass=devclass      #11-物联网设备，21-水肥机，31-棚博士        
        
        

#已连接设备:连接sock，连接IP，设备序列号，最近连接时间，超时时间，任务状态，是否在线       
class conn_class():        
    def __init__(self,sock,addr,serial_num,lasttime,overtime,taskstate,isonline):
        self.sock = sock                #连接sock
        self.addr = addr                #连接IP
        self.serial_num = serial_num    #设备序列号   
        self.lasttime = lasttime        #最近连接时间
        self.overtime = overtime        #超时时间 
        self.state=taskstate            #任务状态，0-无任务，1-有任务(水肥机)
        self.isonline=isonline          #是否在线 0-不在线，1-在线
        
class unknow_class():
    def __init__(self,sock,addr,linktimes):
        self.sock = sock                #连接sock
        self.addr = addr                #连接IP
        self.linktimes = linktimes    #设备序列号   
     

def find_comm_sn(for_value):
    _index=-1
    bResult=False
    if len(comm_list) > 0:
        for i in range(0,len(comm_list)):
            if comm_list[i].serial_num==for_value:
                _index = i
                bResult=True
                break
        return _index
    else:
        return _index

def find_conn_sn(for_value):
    _index=-1
    bResult=False
    if len(conn_list) > 0:
        for i in range(0,len(conn_list)):
            if conn_list[i].serial_num==for_value:
                _index = i
                bResult=True
                break
        return _index
    else:
        return _index

def find_conn_addr(for_value):
    _index=-1
    bResult=False
    if len(conn_list) > 0:
        for i in range(0,len(conn_list)):
            if conn_list[i].addr==for_value :
                _index = i
                bResult=True
                break
        return _index
    else:
        return _index

#devtype:10-控制器，11-传感器，20-水肥机 控制器，21-水肥机传感器 
def find_devid(for_value,devclass):
    _index=-1
    bResult=False
    if devclass == 10 or devclass == 20:
        if len(controller_list) > 0:
            for i in range(0,len(controller_list)):
                if controller_list[i].devid==for_value and controller_list[i].devclass==devclass:
                    _index = i
                    bResult=True
                    break
    elif devclass == 11 or devclass == 21:
        if len(sensor_list) > 0:
            for i in range(0,len(sensor_list)):
                if sensor_list[i].devid==for_value and sensor_list[i].devclass==devclass:
                    _index = i
                    bResult=True
                    break
    return _index
    
#devclass:10-控制器，11-传感器，20-水肥机 控制器，21-水肥机传感器      
def find_devaddr(for_value,comm_sn,devclass):
    _index=-1
    bResult=False
    if devclass == 10 or devclass == 20:
        if len(controller_list) > 0:
            for i in range(0,len(controller_list)):
                if controller_list[i].devaddr==for_value and controller_list[i].devclass==devclass and controller_list[i].commnum==comm_sn:
                    _index = i
                    bResult=True
                    break
    elif devclass == 11 or devclass == 21:
        if len(sensor_list) > 0:
            for i in range(0,len(sensor_list)):
                if sensor_list[i].devaddr==for_value and sensor_list[i].devclass==devclass and sensor_list[i].commnum==comm_sn:
                    _index = i
                    bResult=True
                    break
    return _index  

def PLC_handlerecv(comm_index,sdata):
    comm_sn=comm_list[comm_index].serial_num
    temp1=sdata[:-4]
    temp2=sdata[-4:]
    a=mymodule.crc16(temp1,0).replace(' ','')

    if True:
        sOrder=sdata[2:4]
        #获取记录
        try:
            if sOrder == '03' and len(sdata)<=1024:
                print(sdata)

            elif sOrder == '0f' or (sOrder == '05' and sdata[18:20]=='0f' ):#粘包
                if sOrder == '0f':           
                #0000201910180001 020503e8ff000c79 020f 0c50(addr) 0010(out num) 02(byte num) 3f02(数据) a6b1指令回传
                    lendata=int(sdata[8:12],16)
                    dataset=sdata[14:-4]
                elif sOrder == '05' and sdata[18:20]=='0f':
                    lendata=int(sdata[24:28],16)
                    dataset=sdata[30:-4]
                if lendata > 0:
                    sSQL="insert into yw_d_controller_tbl(id,Code,PortNum,Commucation_ID,onoff) values "
                    try:
                        tempa=''
                        tempb=''
                        statusvar=''
                        sstate=''
                        for i in range(0,int(len(dataset)/2)):
                            tempa=dataset[i*2:i*2+2] #注意modbus标准版和矩形modbus区别，modbus按顺序
                            tempb=tempb+('00000000'+bin(int(tempa,16)).replace('0b',''))[-8:][::-1]
                            statusvar=tempb
                        for i in range(0,lendata):
                            control_index=-1
                            if i < comm_list[comm_index].controlmax:
                                control_index= find_devaddr(i+1,comm_sn,10)
                            else:
                                continue
                            if control_index >=0:
                                if controller_list[control_index].devtype=='1':  
                                    contrlstate = statusvar[i]
                                    if  int(contrlstate,16)==1:
                                        sstate='1'
                                    elif int(contrlstate,16)==0:
                                        sstate='2'
                                    else:
                                        print('state unnomal')
                                elif controller_list[control_index].devtype=='3':
                                    contrlstate = statusvar[i]
                                    closestate = statusvar[i+1]
                                    if  int(contrlstate,16)==1 and int(closestate,16)==0:
                                        sstate='1' #开
                                    elif int(contrlstate,16)==0 and int(closestate,16)==1:
                                        sstate='2' #关
                                    elif int(contrlstate,16)==0 and int(closestate,16)==0:
                                        sstate='3' #停   
                                    else:
                                        print('state unnomal')
                                sSQL = sSQL+"('"+str(controller_list[control_index].devid)+"','"+str(controller_list[control_index].devcode)+"',"
                                sSQL = sSQL+"'"+str(controller_list[control_index].devaddr)+"',"
                                sSQL = sSQL+"'"+str(controller_list[control_index].commnum)+"','"+sstate+"'),"
                    except Exception as err:
                        showinfo =mymodule.getcurrtime()+ ' PLC('+comm_sn + ') handle state data failture:'+str(err)
                        mymodule.create_log(showinfo)
    
                    try:
                        if sSQL[-2:] =='),':
                            sSQL=sSQL[0:-1]+" ON DUPLICATE KEY UPDATE onoff = values(onoff);"    
                            staconn=mysqlpool.connection()
                            stacursor=staconn.cursor()
                            showinfo=mymodule.getcurrtime() + ' Get PLC(' + comm_sn + ')'+ ' device state : '  + sSQL
                            mymodule.create_log(showinfo) 
                            stacursor.execute(sSQL)
                            staconn.commit() 
                            stacursor.close()
                            staconn.close()    
                    except Exception as err:
                        showinfo =mymodule.getcurrtime()+ ' PLC('+comm_sn + ') update state failture:'+str(err)
                        mymodule.create_log(showinfo)
                         
        except Exception as err:
            showinfo =mymodule.getcurrtime()+  ' PLC('+comm_sn + ') handle received data exception:'+str(err)
            mymodule.create_log(showinfo)
def PLC_sendorder(dev_index,comm_index,conn_index,onedata):
#指令id,设备id，指令名称，指令参数(延迟时间)，预计时间，下发时间，网关序号,设备编号        
    comm_sn=comm_list[comm_index].serial_num
    #deviceno=onedata[7]
    icommnum = int(comm_list[comm_index].commaddr)
    icontlnum = int(controller_list[dev_index].devaddr)
    sock=conn_list[conn_index].sock
    sendtimes=0
    sendresult=''
    while True:
        try:
            if onedata != None:
                deviceno=onedata[7]
                if onedata[2] == 'AC-OPEN':
                    if onedata[3] != None and onedata[3] != 'NULL':
                        if int(onedata[3]) > 0:
                            sleep(int(onedata[3])) #延迟
                    if dev_index >=0 and conn_list[conn_index].isonline == 1:
                        if controller_list[dev_index].devtype=='1':
                            sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:]
                            sendstr = sendstr + ' ' + '05'
                            scontlnum = ('0000'+hex(icontlnum-1+2000).replace('0x',''))[-4:]
                            sendstr = sendstr +' '+  scontlnum[:2] + ' '+scontlnum[-2:]     #起始地址
                            sendstr = sendstr +' '+ 'FF 00' #输出数量+字节数+输出值
                            sendstr = sendstr +' '+ mymodule.crc16(sendstr,0)
                            sendstr=sendstr.replace(' ','')
                            sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                        elif controller_list[dev_index].devtype=='3':
                            sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:]
                            sendstr = sendstr + ' ' + '05'
                            scontlnum = ('0000'+hex(icontlnum+5000).replace('0x',''))[-4:]
                            sendstr = sendstr +' '+  scontlnum[:2] + ' '+scontlnum[-2:]     #起始地址
                            sendstr = sendstr +' '+ 'FF 00' #输出数量+字节数+输出值
                            sendstr = sendstr +' '+ mymodule.crc16(sendstr,0)
                            sendstr=sendstr.replace(' ','')
                            sendresult1=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                            if sendresult1 == None:
                                sleep(2)
                                sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:]
                                sendstr = sendstr + ' ' + '05'
                                scontlnum = ('0000'+hex(icontlnum-1+2000).replace('0x',''))[-4:]
                                sendstr = sendstr +' '+  scontlnum[:2] + ' '+scontlnum[-2:]     #起始地址
                                sendstr = sendstr +' '+ 'FF 00' #输出数量+字节数+输出值
                                sendstr = sendstr +' '+ mymodule.crc16(sendstr,0)
                                sendstr=sendstr.replace(' ','')
                                sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ',''))) 
                        if sendresult== None:
                            showinfo=mymodule.getcurrtime()+' '+'PLC('+comm_sn+ ')'+ ' '+deviceno+' '+'OPEN: '+sendstr
                            mymodule.create_log(showinfo)
                            break
                        else:
                            sleep(60)
                            showinfo=mymodule.getcurrtime()+' '+'PLC('+comm_sn+ ')'+ ' '+deviceno+' '+'send order(AC-OPEN): '+sendstr+ ' failed times:' + str(sendtimes)
                            sendtimes=sendtimes+1
                                 
                elif  onedata[2] == 'AC-CLOSE':
                    deviceno=onedata[7]
                    if onedata[3] != None and onedata[3] != 'NULL':
                        if int(onedata[3]) > 0:
                            sleep(int(onedata[3])) 
                    if dev_index >=0 and conn_list[conn_index].isonline == 1:
                        if controller_list[dev_index].devtype=='1':
                            sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:]
                            sendstr = sendstr + ' ' + '05'
                            scontlnum = ('0000'+hex(icontlnum-1+5000).replace('0x',''))[-4:]
                            sendstr = sendstr +' '+  scontlnum[:2] + ' '+scontlnum[-2:]     #起始地址
                            sendstr = sendstr +' '+ 'FF 00' #输出数量+字节数+输出值
                            sendstr = sendstr +' '+ mymodule.crc16(sendstr,0)
                            sendstr=sendstr.replace(' ','')
                            sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                        elif controller_list[dev_index].devtype=='3':
                            sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:]
                            sendstr = sendstr + ' ' + '05'
                            scontlnum = ('0000'+hex(icontlnum-1+5000).replace('0x',''))[-4:]
                            sendstr = sendstr +' '+  scontlnum[:2] + ' '+scontlnum[-2:]     #起始地址
                            sendstr = sendstr +' '+ 'FF 00' #输出数量+字节数+输出值
                            sendstr = sendstr +' '+ mymodule.crc16(sendstr,0)
                            sendstr=sendstr.replace(' ','')
                            sendresult1=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                            if sendresult1 == None:
                                sleep(2)
                                sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:]
                                sendstr = sendstr + ' ' + '05'
                                scontlnum = ('0000'+hex(icontlnum+2000).replace('0x',''))[-4:]
                                sendstr = sendstr +' '+  scontlnum[:2] + ' '+scontlnum[-2:]     #起始地址
                                sendstr = sendstr +' '+ 'FF 00' #输出数量+字节数+输出值
                                sendstr = sendstr +' '+ mymodule.crc16(sendstr,0)
                                sendstr=sendstr.replace(' ','')
                                sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                    if sendresult== None:
                        showinfo=mymodule.getcurrtime()+' '+'PLC('+comm_sn+ ')'+ ' '+deviceno+' '+'CLOSE: '+sendstr
                        mymodule.create_log(showinfo)
                        break
                    else:
                        sleep(60)
                        showinfo=mymodule.getcurrtime()+' '+'PLC('+comm_sn+ ')'+ ' '+deviceno+' '+'send order(AC-CLOSE): '+sendstr+ ' failed times:' + str(sendtimes)
                        sendtimes=sendtimes+1
                            
                elif  onedata[2] == 'AC-STOP':
                    deviceno=onedata[7]
                    if onedata[3] != None and onedata[3] != 'NULL':
                        if int(onedata[3]) > 0 :
                           sleep(int(onedata[3]))
                    if dev_index >=0  and conn_list[conn_index].isonline == 1:
                        if controller_list[dev_index].devtype=='1':
                            sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:]
                            sendstr = sendstr + ' ' + '05'
                            scontlnum = ('0000'+hex(icontlnum-1+5000).replace('0x',''))[-4:]
                            sendstr = sendstr +' '+  scontlnum[:2] + ' '+scontlnum[-2:]     #起始地址
                            sendstr = sendstr +' '+ 'FF 00' #输出数量+字节数+输出值
                            sendstr = sendstr +' '+ mymodule.crc16(sendstr,0)
                            sendstr=sendstr.replace(' ','')
                            sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                        elif controller_list[dev_index].devtype=='3':
                            sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:]
                            sendstr = sendstr + ' ' + '05'
                            scontlnum = ('0000'+hex(icontlnum-1+5000).replace('0x',''))[-4:]
                            sendstr = sendstr +' '+  scontlnum[:2] + ' '+scontlnum[-2:]     #起始地址
                            sendstr = sendstr +' '+ 'FF 00' #输出数量+字节数+输出值
                            sendstr = sendstr +' '+ mymodule.crc16(sendstr,0)
                            sendstr=sendstr.replace(' ','')
                            sendresult1=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                            if sendresult1 == None:
                                sleep(1)
                                sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:]
                                sendstr = sendstr + ' ' + '05'
                                scontlnum = ('0000'+hex(icontlnum+5000).replace('0x',''))[-4:]
                                sendstr = sendstr +' '+  scontlnum[:2] + ' '+scontlnum[-2:]     #起始地址
                                sendstr = sendstr +' '+ 'FF 00' #输出数量+字节数+输出值
                                sendstr = sendstr +' '+ mymodule.crc16(sendstr,0)
                                sendstr=sendstr.replace(' ','')
                                sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                    if sendresult== None:
                        showinfo=mymodule.getcurrtime()+' '+'PLC('+comm_sn+ ')'+ ' '+deviceno+' '+'STOP: '+sendstr
                        mymodule.create_log(showinfo)
                        break
                    else:
                        sleep(60)
                        showinfo=mymodule.getcurrtime()+' '+'PLC('+comm_sn+ ')'+ ' '+deviceno+' '+'send order(AC-STOP): '+sendstr+ ' failed times:' + str(sendtimes)
                        sendtimes=sendtimes+1
                elif onedata[2] == 'COLLECT-DATA':
                    pass
                    
                elif onedata[2] == 'GET-STATE': 
                     pass          
        except Exception as err:
            showinfo =mymodule.getcurrtime()+  ' PLC('+comm_sn + ') send order exception : '+str(err)
            mymodule.create_log(showinfo)
            sendtimes=sendtimes+1
            sleep(60)
        if sendtimes > 5:
            conn_list[conn_index].isonline = 0
            mymodule.create_log(showinfo)
            showinfo=mymodule.getcurrtime()+' '+'PLC('+comm_sn + ')'+' thread shutdown for send order over 3 times '
            mymodule.create_log(showinfo) 
            break
    if dev_index >= 0:           
        reslSQL='update yw_c_control_log_tbl set ExecuteTime=%s,ExecuteResult=%s where id = "' + str(onedata[0]) + '"'
        reslconn=mysqlpool.connection()
        reslcursor=reslconn.cursor()
        reslcursor.execute(reslSQL,args=(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'4'))
        reslconn.commit() 
        reslcursor.close()
        reslconn.close()

def KLC_handlerecv(comm_index,sdata):
    comm_sn=comm_list[comm_index].serial_num
    #print('start fkc compute'+sdata)
    #sdata='010320018367de022304e90233000103238fb0033309d81423aa8b143300060483666c'
    lenofdata=int(sdata[4:6],16)*2
    datatemp=sdata[6:]
    if lenofdata == len(sdata[6:]):
        sOrder=sdata[2:4]
        #获取记录
        try:
            showinfo = mymodule.getcurrtime()+' KLC('+comm_sn + ') receive sensor data:'+sdata
            mymodule.create_log(showinfo)
            if sOrder == '03' and sdata[0:2] == "01" :
                sSQL="INSERT INTO yw_c_sensordata_tbl (Device_ID,Device_Code,ReportTime,ReportValue,Block_ID) values "
                for i in range(0,8):
                    
                    sensordata = datatemp[0:8]
                    datatemp=datatemp[8:]
                    sensor_index=-1
                    if i < comm_list[comm_index].sensormax:
                        sensor_index= find_devaddr(i+1,comm_sn,11)
                    else:
                        continue
                    if sensor_index >=0:  
                        sensorformat=sensordata[2:4]
                        formatstr=('0000000'+bin(int(sensorformat,16)).replace('0b',''))[-8:]
                        signalofdata=0
                        valueofdata=0
                        doubleofdata=0
                        highofdata=0
                        if formatstr[0:1]=='0':
                            signalofdata=0 #有符号数
                        else:
                            signalofdata=1
                        if formatstr[1:2]=='0':
                            valueofdata=0  #开关量
                        else:
                            valueofdata=1
                        if formatstr[2:3]=='0':
                            doubleofdata=0 # 四字节
                        else:
                           doubleofdata=1        
                        if formatstr[3:4]=='0':
                            highofdata=0 # 高低位，
                        else:
                            hignofdata=1
                        str1=('00000000'+formatstr[5:])[-8:]
                        dotbit=int(('00000000'+formatstr[5:])[-8:],2)
                        sensorvalue=''
                        if doubleofdata == 0: #双字节
                            sensordata1=sensordata[4:8]
                            if signalofdata == 0: #
                                sensorvalue=int(sensordata1,16)/(10**dotbit)
                            elif signalofdata == 1: #
                                plusdata=(('0000000'+bin(int(sensordata[2:4],16)).replace('0b',''))[-8:])[0:1]
                                if plusdata == '0':
                                   sensorvalue=int(sensordata1,16)/(10**dotbit)                     
                                elif plusdata == '1':
                                   sensorvalue=mymodule.hex2signal(sensordata1)/(10**dotbit)
                        elif doubleofdata == 1:#四字节
                            if highofdata==0:
                                sensordata1 = datatemp[4:8] + sensordata[4:8]
                            elif highofdata==1:
                                sensordata1=sensordata[4:8] + datatemp[4:8]
                            else:
                                break
                            datatemp=datatemp[8:]
                            if signalofdata == 0: # 无符号数
                                sensorvalue=int(sensordata1,16)/(10**dotbit) 
                            elif signalofdata == 1: # 有符号数
                                plusdata=(('0000000'+bin(int(sensordata[2:4],16)).replace('0b',''))[-8:])[0:1]
                                if plusdata == '0':
                                    sensorvalue=int(sensordata1,16)/(10**dotbit)                      
                                elif plusdata == '1':
                                    sensorvalue=mymodule.hex2signal(sensordata1)/(10**dotbit)
                             
                        sensorformula=sensor_list[sensor_index].formula
                        if sensorformula == 'WENDU':
                            sensorvalue="{:.1f}".format(sensorvalue)
                        elif sensorformula == 'SHIDU':
                            sensorvalue="{:.1f}".format(sensorvalue)
                        elif sensorformula == 'BEAM':
                            sensorvalue="{:.0f}".format(sensorvalue)
                        elif sensorformula == 'CO2':
                            sensorvalue="{:.0f}".format(sensorvalue)
                        elif sensorformula == 'Q-WENDU':
                            sensorvalue="{:.1f}".format(sensorvalue)    
                        elif sensorformula == 'Q-SHIDU':
                            sensorvalue="{:.1f}".format(sensorvalue)
                        elif sensorformula == 'PH':
                            sensorvalue="{:.1f}".format(sensorvalue)
                        elif sensorformula == 'EC':
                            sensorvalue="{:.2f}".format(sensorvalue)    
                        elif sensorformula == 'SWD':
                            sensorvalue="{:.1f}".format(sensorvalue)
                        elif sensorformula == 'SWZ':
                            sensorvalue="{:.1f}".format(sensorvalue)    
                        elif sensorformula == 'ORP':
                            sensorvalue="{:.2f}".format(sensorvalue)
                        else:
                            continue    
                        sSQL = sSQL+"('"+str(sensor_list[sensor_index].devid)+"','"+sensor_list[sensor_index].devcode+"','"+mymodule.getcurrtime()+"','"+sensorvalue+"','"+str(sensor_list[sensor_index].blockid)+"'),"

                    else:
                        #showinfo =getcurrtime()+ ' KLC('+comm_sn + ') not find device channel:' +str(i)
                        #create_log(showinfo)
                        pass

               #####     
                try:
                    if sSQL[-2:] =='),':
                        sSQL=sSQL[0:-1]+";"
                        comtconn=mysqlpool.connection()
                        comtcursor=comtconn.cursor()
                        comtcursor.execute(sSQL)
                        showinfo=mymodule.getcurrtime() + ' insert (' + comm_sn + ')'+ ' sensor data : '  + sSQL
                        mymodule.create_log(showinfo)                    
                        comtconn.commit()  
                        comtcursor.close()
                        comtconn.close()
                    else:
                        pass
                except Exception as err:
                    showinfo = mymodule.getcurrtime()+ ' KLC('+comm_sn + ') insert data failure : '+str(err)
                    mymodule.create_log(showinfo)
                    
             #状态指令       
            elif sOrder == '03' and sdata[0:2] == "02":
                #'02 03 08 A1 40 FF FF A2 40 FF FF
                sSQL="insert into yw_d_controller_tbl(id,onoff) values "
                try:
                    sstate=""
                    for i in range(0,2):
                        control_index= find_devaddr(i+1,comm_sn,10)
                        if control_index >=0:  
                            contrlstate = sdata[i*8+10:i*8+14]
                            if  contrlstate == "FFFF":
                                sstate='1'
                            elif contrlstate == "0000":
                                sstate='2'
                            else:
                                print('state unnomal')
                            sSQL = sSQL+"('"+str(controller_list[control_index].devid)+"','"+sstate+"'),"
                except Exception as err:
                    showinfo =mymodule.getcurrtime()+ ' KLC('+comm_sn + ') handle state data failture : '+str(err)
                    mymodule.create_log(showinfo)

                try:
                    if sSQL[-2:] =='),':
                        sSQL=sSQL[0:-1]+" ON DUPLICATE KEY UPDATE onoff = values(onoff);"    
                        staconn=mysqlpool.connection()
                        stacursor=staconn.cursor()
                        showinfo=mymodule.getcurrtime() + ' Get (' + comm_sn + ')'+ ' device state : '  + sSQL
                        mymodule.create_log(showinfo) 
                        stacursor.execute(sSQL)
                        staconn.commit() 
                        stacursor.close()
                        staconn.close()    
                except Exception as err:
                    showinfo =mymodule.getcurrtime()+ ' KLC('+comm_sn + ') update state failture : '+str(err)
                    mymodule.create_log(showinfo)

        except Exception as err:
            showinfo =mymodule.getcurrtime()+  ' KLC('+comm_sn + ') handle received data exception : '+str(err)
            mymodule.create_log(showinfo)
def KLC_sendorder(dev_index,comm_index,conn_index,onedata):
#指令id,设备id，指令名称，指令参数(延迟时间)，预计时间，下发时间，网关序号,设备编号        
    comm_sn=comm_list[comm_index].serial_num
    #deviceno=onedata[7]
    icommnum = int(comm_list[comm_index].commaddr)
    icontlnum = int(controller_list[dev_index].devaddr)
    sock=conn_list[conn_index].sock
    sendtimes=0
    while True:
        try:
            if onedata[2] == 'AC-OPEN':
                deviceno=onedata[7]
                if onedata[3] != None and onedata[3] != 'NULL':
                    if int(onedata[3]) > 0 :
                        sleep(int(onedata[3]))
                if dev_index >=0 and conn_list[conn_index].isonline == 1:
                    sendstr = '15 01 00 00 00 0B 02 10'
                    if icontlnum==1:
                        sendstr = sendstr + ' ' +  '00 00'
                    elif icontlnum==2: 
                        sendstr = sendstr + ' ' +  '00 02'
                    sendstr = sendstr + ' ' +'00 02 04'
                    sendstr = sendstr + ' ' + 'A' + str(icontlnum) +' ' +'40 FF FF'
                    sendstr = sendstr +' '+ mymodule.crc16(sendstr,0) 
                    sendstr=sendstr.replace(' ','')
                    sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                    if sendresult == None:
                        showinfo=mymodule.getcurrtime()+' '+'KLC('+comm_sn+ ')'+ ' '+deviceno+' '+'OPEN : '+sendstr
                        mymodule.create_log(showinfo)
                        sleep(0.5)
                        break
                    else:
                        sleep(60)
                        showinfo=mymodule.getcurrtime()+' '+'KLC('+comm_sn+ ')'+ ' '+deviceno+' '+'OPEN : '+sendstr + ' failed times:' + str(sendtimes)
                        sendtimes=sendtimes+1
                             
            elif  onedata[2] == 'AC-CLOSE':
                deviceno=onedata[7]
                if onedata[3] != None and onedata[3] != 'NULL':
                    if int(onedata[3]) > 0:
                        sleep(int(onedata[3]))
                if dev_index >=0 and conn_list[conn_index].isonline == 1:
                    sendstr = '15 01 00 00 00 0B 02 10'
                    if icontlnum==1:
                        sendstr = sendstr + ' ' +  '00 00'
                    elif icontlnum==2: 
                        sendstr = sendstr + ' ' +  '00 02'
                    sendstr = sendstr + ' ' +'00 02 04'
                    sendstr = sendstr + ' ' + 'A' + str(icontlnum) +' ' +'40 00 00'
                    sendstr = sendstr +' '+ mymodule.crc16(sendstr,0) 
         
                    sendstr=sendstr.replace(' ','')
                    sendresult = sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                    
                    if sendresult == None:
                        showinfo=mymodule.getcurrtime()+' '+'KLC('+comm_sn+ ')'+ ' '+deviceno+' '+'CLOSE : ' + sendstr
                        mymodule.create_log(showinfo)
                        sleep(0.5) 
                        break
                    else:
                        sleep(60)
                        showinfo=mymodule.getcurrtime()+' '+'KLC('+comm_sn+ ')'+ ' '+deviceno+' '+'CLOSE : '+ sendstr + ' failed times:' + str(sendtimes)
                        sendtimes=sendtimes+1            
            elif onedata[2] == 'COLLECT-DATA':
                if conn_list[conn_index].isonline == 1:
                    sendstr= '15 01 00 00 00 06 01'
                    sendstr=sendstr+' '+ '03'
                    sendstr=sendstr+' '+ '00 00' #起始地址
                    sendstr=sendstr+' '+ '00 20' #数量
                    #sendstr=sendstr+' '+ crc16(sendstr,0)
                    sendstr=sendstr.replace(' ','')
                    sendresult = sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                    if sendresult == None:
                        showinfo=mymodule.getcurrtime()+' '+'KLC('+comm_sn + ')'+' Collect DATA: '+sendstr
                        mymodule.create_log(showinfo)
                        sleep(0.5)
                        break
                    else:
                        sleep(60)
                        showinfo=mymodule.getcurrtime()+' '+'KLC('+comm_sn + ')'+' Collect DATA: '+sendstr+ ' failed times:' + str(sendtimes)
                        sendtimes=sendtimes+1
            elif onedata[2] == 'GET-STATE': 
                if conn_list[conn_index].isonline == 1:
                    sendstr= '15 01 00 00 00 06 02'
                    sendstr=sendstr+' '+ '03'
                    sendstr=sendstr+' '+ '00 00' #
                    sendstr=sendstr+' '+ '00 04' #
                    sendstr=sendstr+' '+ mymodule.crc16(sendstr,0)
                    sendstr=sendstr.replace(' ','')
                    sendresult = sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                    if sendresult == None:
                        showinfo=mymodule.getcurrtime()+' '+'KLC('+comm_sn + ')'+' GET STATUS: '+sendstr
                        mymodule.create_log(showinfo)
                        sleep(0.5)
                        break
                    else:
                        sleep(60)
                        showinfo=mymodule.getcurrtime()+' '+'KLC('+comm_sn + ')'+' GET STATUS: '+sendstr+ ' failed times:' + str(sendtimes)
                        sendtimes=sendtimes+1       
            else:
                showinfo=mymodule.getcurrtime()+' '+'KLC('+comm_sn + ')'+' unknow order : '+ onedata[2]
                sendtimes=sendtimes+1
        except Exception as err:
            showinfo =mymodule.getcurrtime()+  ' KLC('+comm_sn + ') send order exception : '+str(err)
            mymodule.create_log(showinfo)
            sendtimes=sendtimes+1
            sleep(60)
        if sendtimes > 5:
            conn_list[conn_index].isonline = 0
            mymodule.create_log(showinfo)
            showinfo=mymodule.getcurrtime()+' '+'KLC('+comm_sn + ')'+' send thread shutdown for over 3 times '
            mymodule.create_log(showinfo)
            break
    if dev_index >= 0:
        reslSQL='update yw_c_control_log_tbl set ExecuteTime=%s,ExecuteResult=%s where id = "' + str(onedata[0]) + '"'
        reslconn=mysqlpool.connection()
        reslcursor=reslconn.cursor()
        reslcursor.execute(reslSQL,args=(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'4'))
        reslconn.commit() 
        reslcursor.close()
        reslconn.close() 
               
def SFJ_handlerecv(comm_index,sdata):
    commtype=comm_list[comm_index].commtype
    comm_sn=comm_list[comm_index].serial_num
    temp1=sdata[:-4]
    temp2=sdata[-4:]
    a=mymodule.crc16(temp1,0).replace(' ','')
    #if a==temp2.replace(' ','') or a==temp2.upper().replace(' ',''):
    if True:
        sOrder=sdata[2:4]
        
        try:
            if sOrder == '10':
                tasklog=[]
                for i in range(0,32):
                    hexdata=sdata[i*4+14:i*4+18]
                    tasklog.append(int(hexdata,16))
                isRemote=tasklog[0]
                taskArea=tasklog[1]  
                taskstate=tasklog[2]
                water_set=tasklog[3]/10
                inteval_set=tasklog[4]
                ferter_set=tasklog[5]/10
                taskdate=str(tasklog[17])+'-'+('00'+str(tasklog[18]))[-2:]+'-'+('00'+str(tasklog[19]))[-2:]
                starttime=('00'+str(tasklog[20]))[-2:]+':'+('00'+str(tasklog[21]))[-2:]+':'+('00'+str(tasklog[22]))[-2:]
                endtime=('00'+str(tasklog[26]))[-2:]+':'+('00'+str(tasklog[27]))[-2:]+':'+('00'+str(tasklog[28]))[-2:]
                tasktype=tasklog[12]
                taskid=int(sdata[16*4+6:16*4+10])
                water_real=tasklog[29]/10
                interval_real=tasklog[30]
                ferter_real=tasklog[31]/10
 
                sArea=''
                sFertcomm=''
                bitArea=''
                tempb=''
                for i in range(0,4):  #支持16个输出,
                    tempb=tempb+('0000'+bin(int((('0000'+hex(taskArea).replace('0x',''))[-4:])[i],16)).replace('0b',''))[-4:]
                bitArea=tempb[::-1]
                if commtype=='SFJ-0804': 
                    for i in range(0,8):
                        if bitArea[i]=='1':
                            sArea=sArea +str(i+1)+','
                    if sArea[len(sArea)-1] ==',':
                       sArea=sArea[:-1]
                    if tasktype == '1': 
                        if int(bitArea[8])=='1':
                            sFertcomm=sFertcomm+'A,'
                        if int(bitArea[9])=='1':
                            sFertcomm=sFertcomm+'B,'
                        if int(bitArea[10])=='1':
                            sFertcomm=sFertcomm+'C,'        
                        if int(bitArea[11])=='1':
                            sFertcomm=sFertcomm+'D,'
                        if sFertcomm[len(sFertcomm)-1] ==',':
                           sFertcomm=sFertcomm[:-1]  
                if commtype=='SFJ-1200':
                    for i in range(0,16):
                        if int(bitArea[i]):
                            sArea=sArea +str(i+1)+','
                    if sArea[len(sArea)-1] ==',':
                            sArea=sArea[:-1] 
                #任务记录
                if taskstate > 0 and  taskstate < 3:  
                    reslconn=mysqlpool.connection()
                    reslcursor=reslconn.cursor()
                    scommpany=str(comm_list[comm_index].companyid)
                    sResult = "INSERT INTO sfyth_log (R_Start,R_End,R_Interval,R_Gquantity,R_Squantity,T_Area, "
                    sResult = sResult +"T_Fertilize,R_State,T_Date,T_ID,T_Type,Company_ID,PLC_Number,DO_Type) "
                    sResult = sResult + "values('"+starttime+"','"+endtime+"','"+str(interval_real)+"','"+str(water_real)+"','"+str(ferter_real)+"','"+sArea+"',"
                    sResult = sResult + "'"+sFertcomm+"','"+str(taskstate)+"','"+taskdate+"','"+str(taskid)+"','"+str(tasktype)+"','"+scommpany+"','"+comm_sn+"','"+str(isRemote)+"')"
                    
                    reslcursor.execute(sResult) 
                    reslconn.commit()
                    reslcursor.close()
                    reslconn.close()
                    showinfo=mymodule.getcurrtime()+' '+'SFC('+comm_sn + ')'+' '+ 'add a new task log'
                    mymodule.create_log(showinfo)
                    sleep(0.5)
            #0000201910180001 020503e8ff000c79 020f0c500010023f02a6b1指令回传
            #获取记录           
            #获取状态             
            if sOrder == '0f' or (sOrder == '05' and sdata[16:20]=='020f' ):
                
                sState=''
                bState=''
                devid=[]
                devch=[]
                
                sState=sdata[-6:-4]+sdata[-8:-6]
                for i in range(0,len(sState)):
                    bState=bState+('0000'+bin(int(sState[i],16)).replace('0b',''))[-4:]
                bState=bState[::-1]
                
                sSQL="insert into sfyth_device(id,state) values"
                controlmax=comm_list[comm_index].controlmax
                for i in range(0,controlmax):
                    dev_index= find_devaddr(i+1,comm_sn,20)
                    if dev_index >=0:
                        devid=controller_list[dev_index].devid
                        sSQL = sSQL+"('"+str(devid)+"','"+str(bState[i])+"'),"
                try:
                    if sSQL[-2:] =='),':
                        sSQL=sSQL[0:-1]+" ON DUPLICATE KEY UPDATE state = values(state);"    
                        staconn=mysqlpool.connection()
                        stacursor=staconn.cursor()
                        mymodule.create_log(sSQL)
                        stacursor.execute(sSQL)
                        staconn.commit() 
                        stacursor.close()
                        staconn.close()
                except Exception as err:
                    showinfo = mymodule.getcurrtime()+' '+'SFC('+comm_sn + ')'+' '+ 'update state failed : '+str(err)
                    mymodule.create_log(showinfo)
        except Exception as err:
            showinfo = mymodule.getcurrtime()+' '+'SFC('+comm_sn + ')'+' '+ 'handle receive data exception : '+str(err)
            mymodule.create_log(showinfo)
def SFJ_sendorder(devtype,onedata):
    sendtimes=0
    while True:
        comm_sn=onedata[6]
        conn_index=find_conn_sn(comm_sn)
        if devtype ==0 and conn_list[conn_index].isonline == 1:#手动控制
            try:
                #指令id,设备id，指令名称，设备地址，预计时间，下发时间，网关序号,PLC地址   
                if onedata !=None:
                    orderId=onedata[0]
                    orderAct=onedata[2]
                    devAddr=int(onedata[3])
                    plcAddr=onedata[7]
                    sock=conn_list[conn_index].sock
                    if orderAct == 'AC-OPEN':
                        sOrder=('00'+hex(int(plcAddr,10)).replace('0x',''))[-2:]
                        sOrder= sOrder+' '+'05'
                        sOrder= sOrder+' '+('0000'+hex(int(devAddr)-1+1000).replace('0x',''))[-4:-2]
                        sOrder= sOrder+' '+('0000'+hex(int(devAddr)-1+1000).replace('0x',''))[-2:]
                        sOrder= sOrder+' '+'FF 00'
                        sOrder= sOrder+' '+mymodule.crc16(sOrder,0)
                        sOrder=sOrder.replace(' ','')
                        #sendresult=clientsock.sendall(sOrder.decode('hex'))
                        sendresult=sock.sendall(bytes().fromhex(sOrder))
                        if sendresult == None:
                            showinfo=mymodule.getcurrtime()+' '+'SFJ('+comm_sn + ')'+' '+ onedata[5].strftime('%Y-%m-%d %H:%M:%S')+' '+'OPEN : '+sOrder
                            mymodule.create_log(showinfo)
                            sleep(0.5)
                            break
                        else:
                            sendtimes=sendtimes+1
                            sleep(60)
                    elif orderAct == 'AC-CLOSE':
                        sOrder=('00'+hex(int(plcAddr,10)).replace('0x',''))[-2:]
                        sOrder= sOrder+' '+'05'
                        sOrder= sOrder+' '+('0000'+hex(int(devAddr)-1+1000).replace('0x',''))[-4:-2]+' '
                        sOrder= sOrder+' '+('0000'+hex(int(devAddr)-1+1000).replace('0x',''))[-2:]
                        sOrder= sOrder+' '+'00 00'
                        sOrder= sOrder+' '+mymodule.crc16(sOrder,0)
                        sOrder=sOrder.replace(' ','')
                        sendresult=sock.sendall(bytes().fromhex(sOrder))
                        if sendresult == None:
                            showinfo=mymodule.getcurrtime()+' '+'SFJ('+comm_sn + ')'+' '+ onedata[5].strftime('%Y-%m-%d %H:%M:%S')+' '+'CLOSE : '+sOrder
                            mymodule.create_log(showinfo)
                            sleep(0.5)
                            break
                        else:
                            sendtimes=sendtimes+1
                            sleep(60)
                    else:
                        sendtimes=sendtimes+1
                        showinfo=mymodule.getcurrtime()+' '+'SFC('+comm_sn + ')'+' '+ onedata[5].strftime('%Y-%m-%d %H:%M:%S')+' '+'error order type : '+orderAct
                        mymodule.create_log(showinfo)
            except Exception as e:
                sendtimes=sendtimes+1
                showinfo=mymodule.getcurrtime() + ' send device order(' + comm_sn + ')'+ +' '+ onedata[5].strftime('%Y-%m-%d %H:%M:%S') + 'failed : '   +str(e)
                mymodule.create_log(showinfo)
            if sendtimes > 3:
                break
            reslSQL='update yw_c_control_log_tbl set ExecuteTime=%s,ExecuteResult=%s where id = "' + str(onedata[0]) + '"'
            reslconn=mysqlpool.connection()
            reslcursor=reslconn.cursor()
            reslcursor.execute(reslSQL,args=(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'4'))
            reslconn.commit() 
            reslcursor.close()
            reslconn.close()  
                
        elif devtype ==1 and conn_list[conn_index].isonline == 1:  #任务      
            try:
                #开始时间，时长，水量，肥量，区域，肥液通道,日期、类型、PLC地址、指令id、PLC序列号
                if onedata != None:
                    taskStart=onedata[0]       
                    taskInterval=onedata[1]
                    taskWater=onedata[2] 
                    taskFertnum=onedata[3]
                    taskArea=onedata[4]
                    taskFertcomm=onedata[5]
                    taskDate=onedata[6] 
                    taskType=onedata[7] 
                    PLCAddress=onedata[8]
                    taskID=onedata[9]
                    comm_sn=onedata[10]
                    conn_index=find_conn_sn(comm_sn)
                    comm_index=find_comm_sn(comm_sn)
                    sock=conn_list[conn_index].sock
                    commtype=comm_list[comm_index].commtype
                    #灌区和肥液通道
                    sArea=''
                    sFercomm=''
                    tempArea=[]
                    iArea=''
                    exitflag=0
                    sOut=''
                    if taskWater !=None  and taskInterval !=None:
                       sendtimes=10
                    if taskWater ==None and taskInterval ==None:
                       sendtimes=10
                    if taskType=='1':
                        if int(taskFertnum*10) <= 0:
                            sendtimes=11
                    if  taskArea == None:
                        sendtimes=12
                    
                    if commtype =='SFJ-0804':
                        tempArea=taskArea.split(',',-1)
                        for i in  tempArea:
                            dev_index=find_devid(int(i),20)
                            if dev_index >=0:
                                iArea=str(controller_list[dev_index].devaddr)+','+iArea
                        for i in range(1,9):
                            tempArea=iArea.split(',',-1) ##    新增      
                            if str(i) in  tempArea: 
                                sArea=sArea+'1'
                            else:
                                sArea=sArea+'0'
                        #sArea=sArea[::-1]
                        if taskType=='1':
                            if 'A' in taskFertcomm:
                                sFercomm='1'+sFercomm
                            else:
                                sFercomm='0'+sFercomm
                            if 'B' in taskFertcomm:
                                sFercomm='1'+sFercomm
                            else:
                                sFercomm='0'+sFercomm    
                            if 'C' in taskFertcomm:
                                sFercomm='1'+sFercomm
                            else:
                                sFercomm='0'+sFercomm    
                            if 'D' in taskFertcomm:
                                sFercomm='1'+sFercomm
                            else:
                                sFercomm='0'+sFercomm    
                            #sFercomm=sFercomm[::-1]
                        sOut=(sArea+(sFercomm+'00000000')[0:8])[::-1]
                        
                    elif commtype =='SFJ-1200':
                        tempArea=taskArea.split(',',-1)
                        for i in  tempArea:
                            dev_index=find_devid(int(i),20)
                            if dev_index >=0:
                                iArea=str(controller_list[dev_index].devaddr)+','+iArea
                        sArea=''
                        for i in range(1,13):
                            tempArea=iArea.split(',',-1)
                            if str(i) in  tempArea:
                                sArea=sArea+'1'
                            else:
                                sArea=sArea+'0'
                        sArea=sArea[::-1]
                        sOut=('00000000'+sArea)[-16:]
                    sendstr=('00'+hex(int(PLCAddress,10)).replace('0x',''))[-2:] #设备号
                    sendstr=sendstr+' '+'10' #指令
                    sendstr=sendstr+' '+'01 29' #起始地址,40298(减1)
                    sendstr=sendstr+' '+'00 0D' #寄存器数量n
                    sendstr=sendstr+' '+'1A' #字节数2*n
                    sendstr = sendstr + ' ' + '00 01' #远程控制
                    temp1=('0000'+hex(int(sOut, 2)).replace('0x',''))[-4:]
                    sendstr=sendstr+' '+temp1[0:2]+' '+temp1[2:4] #灌区
                    sendstr=sendstr+' '+'00 00' #状态
                    if taskWater !=None:    
                        temp1=('0000'+hex(int(taskWater*10)).replace('0x',''))[-4:]
                        sendstr=sendstr+' '+ temp1[-4:-2]+' '+ temp1[-2:]#设定灌溉量
                    else:
                        sendstr=sendstr+' '+'00 00' #设定灌溉量                     
                    if taskInterval !=None:
                        temp1=('0000'+hex(taskInterval).replace('0x',''))[-4:]
                        sendstr=sendstr+' '+ temp1[-4:-2]+' '+ temp1[-2:]##设定时长
                    else:
                        sendstr=sendstr+' '+'00 00'##设定时长
                    if taskFertnum !=None:       
                        temp1=('0000'+hex(int(taskFertnum*10)).replace('0x',''))[-4:]
                        sendstr=sendstr+' '+ temp1[-4:-2]+' '+ temp1[-2:]##设定施肥量
                    else:
                        sendstr=sendstr+' '+'00 00'##设定施肥量
                    sendstr=sendstr+' '+'00 00 00 00 00 00 00 00 00 00 00 00' ##设定年月日
                    if taskType=='1': #任务类型
                        sendstr=sendstr+' ' +'00 01'
                    elif taskType=='0':
                        sendstr=sendstr+' ' +'00 00'
                    else:
                        sendtimes=sendtimes+1
                    sendstr=sendstr+' '+mymodule.crc16(sendstr,0)
                    sendstr=sendstr.replace(' ','')
                    sleep(0.2)
                    sendresult=sock.sendall(bytes().fromhex(sendstr))
                    if sendresult == None:
                        sendtimes=0
                        showinfo=mymodule.getcurrtime()+' '+'SFJ('+comm_sn + ')'+'send task succeed'
                        mymodule.create_log(showinfo)
                        break
                    else:
                        sendtimes=sendtimes+1
                        sleep(60)
            except Exception as e:
                sendtimes=sendtimes+1
                showinfo=mymodule.getcurrtime() + ' send task order SFJ(' + comm_sn + ')'+ 'failed : '   +str(e)
                mymodule.create_log(showinfo)
            if sendtimes > 3:
                break
        reslSQL='update yw_c_control_log_tbl set ExecuteTime=%s,ExecuteResult=%s where id = "' + str(onedata[9]) + '"'
        reslconn=mysqlpool.connection()
        reslcursor=reslconn.cursor()
        reslcursor.execute(reslSQL,args=(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'4'))
        reslconn.commit() 
        reslcursor.close()
        reslconn.close()     
def XPC_handlerecv(comm_index,sdata):
    comm_sn=comm_list[comm_index].serial_num
    temp1=sdata[:-4]
    temp2=sdata[-4:]
    a=mymodule.crc16(temp1,0).replace(' ','')
    if a==temp2.replace(' ','') or a==temp2.upper().replace(' ',''):
        sOrder=sdata[2:4]
        #获取记录
        try:
            if sOrder == '03':
                showinfo =mymodule.getcurrtime()+ ' XPC('+comm_sn + ') receive sensor data:'+sdata
                mymodule.create_log(showinfo)
                sSQL="INSERT INTO yw_c_sensordata_tbl (Device_ID,Device_Code,ReportTime,ReportValue,Block_ID) values"
                for i in range(0,16):
                    hexdata = sdata[i*4+6:i*4+10]
                    if (str(hexdata)!='7fff' and hexdata!='7FFF'):
                        sensor_index=-1
                        if i < comm_list[comm_index].sensormax:
                            sensor_index= find_devaddr(i+1,comm_sn,11)
                        else:
                            continue
                        rData=''
                        if sensor_index >=0:  
                            rData= int(hexdata[:-2],16)*16*16 + int(hexdata[-2:],16)
                            sensorformula=sensor_list[sensor_index].formula
                            if sensorformula == 'Q-WENDU':
                                rData = format(rData * 0.1,'.1f')
                            elif sensorformula == 'Q-SHIDU':
                                rData = format(rData * 0.1,'.1f')   
                            elif sensorformula == 'WENDU':
                                rData = format(rData * 0.1,'.1f')
                            elif sensorformula == 'SHIDU':
                                rData = format(rData * 0.1,'.1f')    
                            elif sensorformula == 'CO2':
                                rData = rData 
                            elif sensorformula == 'BEAM':
                                rData = rData * 10
                            elif sensorformula == 'PH':
                                rData = format(rData * 0.01,'.1f')
                            elif sensorformula == 'EC':
                                rData = format(rData * 0.01,'.1f')
                            elif sensorformula == 'FS':
                                rData = format(rData * 0.1,'.1f')  
                            elif sensorformula == 'FX':
                                rData =rData 
                            elif sensorformula == 'YL':
                                rData = format(rData * 0.1,'.1f')
                            elif sensorformula == 'QY':
                                rData = format(rData * 0.1,'.1f')    
                            elif sensorformula == 'SWD':
                                rData = format(rData * 0.1,'.1f')      
                            elif sensorformula == 'SWZ':
                                rData = format(rData * 0.01,'.1f')      
                            elif sensorformula == 'ORP':
                                rData = format(rData * 0.1,'.1f')
                            elif sensorformula == 'SPH':
                                rData = format(rData * 0.1,'.1f')    
                            elif sensorformula == 'SEC':
                                rData = format(rData * 0.001,'.2f')    
                            elif sensorformula == 'VOC':
                                rData = format(rData * 0.1,'.1f')
                            elif sensorformula == 'PM25':
                                rData = rData
                            else:
                                showinfo =mymodule.getcurrtime()+ ' XPC('+comm_sn + ') unknow formula'
                                mymodule.create_log(showinfo)
                            if rData != '':
                                sSQL = sSQL+"('"+str(sensor_list[sensor_index].devid)+"','"+sensor_list[sensor_index].devcode+"','"+mymodule.getcurrtime()+"','"+str(rData)+"','"+str(sensor_list[sensor_index].blockid)+"'),"
                        else:
                            #showinfo =getcurrtime()+ ' FKC('+comm_sn + ') not find device channel:' +str(i)
                            #create_log(showinfo)
                            pass
                try:
                    sSQL=sSQL[0:-1]+";"
                    if sSQL[-2:] ==');':
                        comtconn=mysqlpool.connection()
                        comtcursor=comtconn.cursor()
                        comtcursor.execute(sSQL)
                        showinfo=mymodule.getcurrtime() + ' insert (' + comm_sn + ')'+ ' sensor data : '  + sSQL
                        mymodule.create_log(showinfo)                    
                        comtconn.commit()  
                        comtcursor.close()
                        comtconn.close()
                except Exception as err:
                    showinfo = mymodule.getcurrtime()+ ' XPC('+comm_sn + ') insert data failure: '+str(err)
                    mymodule.create_log(showinfo)
        except Exception as err:
            showinfo =mymodule.getcurrtime()+  ' XPC('+comm_sn + ') handle received data failture: '+str(err)
            mymodule.create_log(showinfo)
def XPC_sendorder(dev_index,comm_index,conn_index,onedata):
    comm_sn=comm_list[comm_index].serial_num
    sock=conn_list[conn_index].sock
    sendtimes=0
    while True:
        try:
            if onedata[2] == 'COLLECT-DATA':
                if conn_list[conn_index].isonline == 1:
                    sendstr= ('00'+hex(int(comm_list[comm_index].commaddr)).replace('0x',''))[-2:]
                    sendstr=sendstr+' '+ '03'
                    sendstr=sendstr+' '+ '00 00' #起始地址
                    sendstr=sendstr+' '+ '00 10' #数量
                    sendstr=sendstr+' '+ mymodule.crc16(sendstr,0)
                    sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                    if sendresult==None:
                        showinfo=mymodule.getcurrtime()+' '+'XPC('+comm_sn+ ')'+ ' COLLECT DATA : '+sendstr
                        mymodule.create_log(showinfo)
                        break
                    else:
                        sleep(60)
                        showinfo=mymodule.getcurrtime()+' '+'XPC('+comm_sn+ ')'+ ' COLLECT DATA: '+sendstr + ' faile times:' + str(sendtimes)
                        sendtimes=sendtimes+1
        except Exception as e:
            sendtimes=sendtimes+1
            showinfo=mymodule.getcurrtime() + ' XPC(' + comm_sn + ')'+ ' collect data exception : '+str(e)
            mymodule.create_log(showinfo)
        if sendtimes > 5:
            conn_list[conn_index].isonline = 0
            break
                               
def YYC_handlerecv(comm_index,sdata):
    comm_sn=comm_list[comm_index].serial_num
    temp1=sdata[:-4]
    temp2=sdata[-4:]
    a=mymodule.crc16(temp1,0).replace(' ','')
    if a==temp2.replace(' ','') or a==temp2.upper().replace(' ',''):
        sOrder=sdata[2:4]
        #获取记录
        try:
            if sOrder == '03':
                showinfo = mymodule.getcurrtime()+' YYC('+comm_sn + ') receive sensor data:'+sdata
                mymodule.create_log(showinfo)
                sSQL="INSERT INTO yw_c_sensordata_tbl (Device_ID,Device_Code,ReportTime,ReportValue,Block_ID) values"
                sensordata=''
                
                for i in range(0,16):
                    recvdata = sdata[i*8+6:i*8+14]
                    recvdata=recvdata[-4:]+recvdata[:4]

                    sensor_index=-1
                    if i < comm_list[comm_index].sensormax:
                        sensor_index= find_devaddr(i+1,comm_sn,11)
                    else:
                        continue
                    rData=''
                    if sensor_index >=0:
                        rData= unpack('!f', bytes.fromhex(recvdata))[0]
                        sensorformula=sensor_list[sensor_index].formula
                        if sensorformula == 'Q-WENDU':
                            rData = format(rData,'.1f')
                        elif sensorformula == 'Q-SHIDU':
                            rData = format(rData,'.1f')   
                        elif sensorformula == 'WENDU':
                            rData = format(rData,'.1f')
                        elif sensorformula == 'SHIDU':
                            rData = format(rData,'.1f')    
                        elif sensorformula == 'CO2':
                            rData = rData 
                        elif sensorformula == 'BEAM':
                            rData = rData
                        elif sensorformula == 'PH':
                            rData = format(rData,'.1f')
                        elif sensorformula == 'EC':
                            rData = format(rData,'.1f')
                        elif sensorformula == 'FS':
                            rData = format(rData,'.1f')  
                        elif sensorformula == 'FX':
                            rData =rData 
                        elif sensorformula == 'YL':
                            rData = format(rData,'.1f')
                        elif sensorformula == 'QY':
                            rData = format(rData,'.1f')    
                        elif sensorformula == 'SWD':
                            rData = format(rData,'.1f')      
                        elif sensorformula == 'SWZ':
                            rData = format(rData,'.1f')      
                        elif sensorformula == 'ORP':
                            rData = format(rData,'.1f')
                        elif sensorformula == 'SPH':
                            rData = format(rData,'.1f')    
                        elif sensorformula == 'SEC':
                            rData = format(rData,'.2f')    
                        elif sensorformula == 'VOC':
                            rData = format(rData,'.1f')
                        elif sensorformula == 'PM25':
                            rData = rData
                        elif sensorformula == 'Y-WENDU':
                            rData = format(rData,'.1f')    
                        elif sensorformula == 'Y-SHIDU':
                            rData = format(rData,'.1f')    
                        elif sensorformula == 'GS-D':
                            rData = format(rData,'.2f')
                        elif sensorformula == 'JG-D':
                            rData = format(rData,'.2f')    
                        else:
                            showinfo =mymodule.getcurrtime()+ ' YYC('+comm_sn + ') unknow formula'
                            mymodule.create_log(showinfo)
                        if rData != '':
                            sSQL = sSQL+"('"+str(sensor_list[sensor_index].devid)+"','"+sensor_list[sensor_index].devcode+"','"+mymodule.getcurrtime()+"','"+str(rData)+"','"+str(sensor_list[sensor_index].blockid)+"'),"
                    else:
                        #showinfo =getcurrtime()+ ' FKC('+comm_sn + ') not find device channel:' +str(i)
                        #create_log(showinfo)
                        pass
 
            try:
                if sSQL[-2:] =='),':
                    sSQL=sSQL[0:-1]+";"
                    comtconn=mysqlpool.connection()
                    comtcursor=comtconn.cursor()
                    comtcursor.execute(sSQL)
                    showinfo=mymodule.getcurrtime() + ' insert (' + comm_sn + ')'+ ' sensor data : '  + sSQL
                    mymodule.create_log(showinfo)                    
                    comtconn.commit()  
                    comtcursor.close()
                    comtconn.close()
            except Exception as err:
                showinfo = mymodule.getcurrtime()+' '+'YYC('+comm_sn + ')'+' '+ 'insert data failed : '+str(err)
                mymodule.create_log(showinfo)
        except Exception as err:
            showinfo = mymodule.getcurrtime()+' '+'YYC('+comm_sn + ')'+' '+ 'receive data failed : '+str(err)
            mymodule.create_log(showinfo)
def YYC_sendorder(dev_index,comm_index,conn_index,onedata):
    comm_sn=comm_list[comm_index].serial_num
    sock=conn_list[conn_index].sock
    sendtimes=0
    while True:
        try:
            if onedata[2] == 'COLLECT-DATA':
                if conn_list[conn_index].isonline == 1:
                    sendstr= ('00'+hex(int(comm_list[comm_index].commaddr)).replace('0x',''))[-2:]
                    sendstr=sendstr+' '+ '03'
                    sendstr=sendstr+' '+ '00 00' #起始地址
                    sendstr=sendstr+' '+ '00 20' #数量
                    sendstr=sendstr+' '+ mymodule.crc16(sendstr,0)
                    sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                    if sendresult==None:
                        showinfo=mymodule.getcurrtime()+' '+'YYC('+comm_sn+ ')'+ ' collect data : '+sendstr
                        mymodule.create_log(showinfo)
                        break
                    else:
                        sleep(60)
                        showinfo=mymodule.getcurrtime()+' '+'YYC('+comm_sn+ ')'+ ' send order(COLLECT-DATA): '+sendstr + ' faile times:' + str(sendtimes)
                        sendtimes=sendtimes+1
        except Exception as e:
            sendtimes=sendtimes+1
            showinfo=mymodule.getcurrtime() + ' YYC(' + comm_sn + ')'+ ' collect data exception : '+str(e)
            mymodule.create_log(showinfo)
        if sendtimes > 5:
            conn_list[conn_index].isonline = 0
def FKC_handlerecv(comm_index,sdata):
    #020300207fff02ea00000000000000000000000000e400de00007fff0000003f00020290f305
    comm_sn=comm_list[comm_index].serial_num
    temp1=sdata[:-4]
    temp2=sdata[-4:]
    a=mymodule.crc16(temp1,0).replace(' ','')
    if a==temp2.replace(' ','') or a==temp2.upper().replace(' ',''):
        sOrder=sdata[2:4]
        #获取记录
        try:
            if sOrder == '03':
                showinfo = mymodule.getcurrtime() +'FKC('+comm_sn + ') receive sensor data:'+sdata
                mymodule.create_log(showinfo)
                sSQL="INSERT INTO yw_c_sensordata_tbl (Device_ID,Device_Code,ReportTime,ReportValue,Block_ID) values"
                for i in range(0,16):
                    
                    hexdata = sdata[i*4+8:i*4+12]
                    if (str(hexdata)!='7fff' and hexdata!='7FFF'):
                        sensor_index=-1
                        if i < comm_list[comm_index].sensormax:
                            sensor_index= find_devaddr(i+1,comm_sn,11)
                        else:
                            continue
                        if sensor_index >=0:  
                            rData= int(hexdata[:-2],16)*16*16 + int(hexdata[-2:],16)
                            sensorformula=sensor_list[sensor_index].formula
                            if sensorformula == 'Q-WENDU':
                                rData = format(rData * 0.1,'.1f')
                            elif sensorformula == 'Q-SHIDU':
                                rData = format(rData * 0.1,'.1f')   
                            elif sensorformula == 'WENDU':
                                rData = format(rData * 0.1,'.1f')
                            elif sensorformula == 'SHIDU':
                                rData = format(rData * 0.1,'.1f')    
                            elif sensorformula == 'CO2':
                                rData = rData 
                            elif sensorformula == 'BEAM':
                                rData = rData * 10
                            elif sensorformula == 'PH':
                                rData = format(rData * 0.01,'.1f')
                            elif sensorformula == 'EC':
                                rData = format(rData * 0.01,'.1f')
                            elif sensorformula == 'FS':
                                rData = format(rData * 0.1,'.1f')  
                            elif sensorformula == 'FX':
                                rData =rData 
                            elif sensorformula == 'YL':
                                rData = format(rData * 0.1,'.1f')
                            elif sensorformula == 'QY':
                                rData = format(rData * 0.1,'.1f')    
                            elif sensorformula == 'SWD':
                                rData = format(rData * 0.1,'.1f')      
                            elif sensorformula == 'SWZ':
                                rData = format(rData * 0.01,'.1f')      
                            elif sensorformula == 'ORP':
                                rData = format(rData * 0.1,'.1f')
                            elif sensorformula == 'SPH':
                                rData = format(rData * 0.1,'.1f')    
                            elif sensorformula == 'SEC':
                                rData = format(rData * 0.001,'.2f')    
                            elif sensorformula == 'VOC':
                                rData = format(rData * 0.1,'.1f')
                            elif sensorformula == 'PM25':
                                rData = rData
                            else:
                                showinfo =mymodule.getcurrtime()+ ' FKC('+comm_sn + ') unknow formula'
                                mymodule.create_log(showinfo)
                            sSQL = sSQL+"('"+str(sensor_list[sensor_index].devid)+"','"+sensor_list[sensor_index].devcode+"','"+mymodule.getcurrtime()+"','"+str(rData)+"','"+str(sensor_list[sensor_index].blockid)+"'),"
                        else:
                            pass
                   #####
  
                try:
                    if sSQL[-2:] =='),':
                        sSQL=sSQL[0:-1]+";"
                        comtconn=mysqlpool.connection()
                        comtcursor=comtconn.cursor()
                        comtcursor.execute(sSQL)
                        showinfo=mymodule.getcurrtime() + ' insert (' + comm_sn + ')'+ ' sensor data : '  + sSQL
                        mymodule.create_log(showinfo)                    
                        comtconn.commit()  
                        comtcursor.close()
                        comtconn.close()
                except Exception as err:
                    showinfo = mymodule.getcurrtime()+ ' FKC('+comm_sn + ') insert data failure: '+str(err)
                    mymodule.create_log(showinfo)
 
            elif sOrder == '70':
                sSQL="insert into yw_d_controller_tbl(id,onoff) values "
                try:
                    for i in range(0,16):
                        control_index= -1
                        if i < comm_list[comm_index].controlmax:
                            control_index= find_devaddr(i+1,comm_sn,10)
                        else:
                            continue
                        if control_index >=0:  
                            contrlstate = sdata[i*2+4:i*2+6]
                            if  int(contrlstate,16)==1:
                                sstate='1'
                            elif int(contrlstate,16)==0:
                                sstate='2'
                            else:
                                print('state unnomal')
                            sSQL = sSQL+"('"+str(controller_list[control_index].devid)+"','"+sstate+"'),"
                except Exception as err:
                    showinfo =mymodule.getcurrtime()+ ' FKC('+comm_sn + ') get control status failture:'+str(err)
                    mymodule.create_log(showinfo)

                try:
                    sSQL=sSQL[0:-1]+" ON DUPLICATE KEY UPDATE onoff = values(onoff);"    
                    staconn=mysqlpool.connection()
                    stacursor=staconn.cursor()
                    showinfo=mymodule.getcurrtime() + ' Get FKC(' + comm_sn + ')'+ ' device state : '  + sSQL
                    mymodule.create_log(showinfo) 
                    stacursor.execute(sSQL)
                    staconn.commit() 
                    stacursor.close()
                    staconn.close()    
                except Exception as err:
                    showinfo =mymodule.getcurrtime()+ ' FKC('+comm_sn + ') update control status failture: '+str(err)
                    mymodule.create_log(showinfo)

        except Exception as err:
            showinfo =mymodule.getcurrtime()+  ' FKC('+comm_sn + ') handle received data failture: '+str(err)
            mymodule.create_log(showinfo)
def FKC_sendorder(dev_index,comm_index,conn_index,onedata):
    rResult=False
    comm_sn=comm_list[comm_index].serial_num
    #deviceno=onedata[7]
    icommnum = int(comm_list[comm_index].commaddr)
    icontlnum = int(controller_list[dev_index].devaddr)
    sock=conn_list[conn_index].sock
    sendtimes=0
    while True:
        try:
            if onedata[2] == 'AC-OPEN':
                deviceno=onedata[7]
                if onedata[3] != None and onedata[3] != 'NULL':
                    if int(onedata[3]) > 0:
                        sleep(int(onedata[3]))
                if dev_index >=0 and conn_list[conn_index].isonline == 1:
                    if controller_list[dev_index].devtype=='1':
                        sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:] + ' '
                        sendstr = sendstr + '72' + ' '
                        sendstr = sendstr + ('00'+hex(icontlnum).replace('0x',''))[-2:] + ' '
                        sendstr = sendstr + '01 00' + ' '
                        sendstr = sendstr + mymodule.crc16(sendstr,0) 
                        sendstr=sendstr.replace(' ','')
                        sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                        if sendresult==None:
                            showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+ ' '+deviceno+' '+'OPEN : '+sendstr
                            mymodule.create_log(showinfo)
                            break
                        else:
                            sleep(60)
                            showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+ ' '+deviceno+' '+'OPEN : '+sendstr + ' faile times:' + str(sendtimes)
                            sendtimes=sendtimes+1
                    elif controller_list[dev_index].devtype=='3': 
                        sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:] + ' '
                        sendstr = sendstr + '72' + ' '
                        sendstr = sendstr + ('00'+hex(icontlnum+1).replace('0x',''))[-2:] + ' '
                        sendstr = sendstr + '00 00' + ' '
                        sendstr = sendstr + mymodule.crc16(sendstr,0) 
                        sendstr=sendstr.replace(' ','')
                        sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                        if sendresult==None: 
                            sleep(2)
                            sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:] + ' '
                            sendstr = sendstr + '72' + ' '
                            sendstr = sendstr + ('00'+hex(icontlnum).replace('0x',''))[-2:] + ' '
                            sendstr = sendstr + '01 00' + ' '
                            sendstr = sendstr + mymodule.crc16(sendstr,0) 
                            sendstr=sendstr.replace(' ','')
                            sendresult1=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                            if sendresult1==None:
                                showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+ ' '+deviceno+' '+'OPEN : '+sendstr
                                mymodule.create_log(showinfo)
                                break
                            else:
                                showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+ ' '+deviceno+' '+'OPEN : '+sendstr+ ' faile times:' + str(sendtimes)
                                sendtimes=sendtimes+1
                        else:
                            sleep(60)
                            showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+ ' '+deviceno+' '+'OPEN : '+sendstr+ ' faile times:' + str(sendtimes)
                            sendtimes=sendtimes+1
                           
            elif  onedata[2] == 'AC-CLOSE':
                deviceno=onedata[7]
                if onedata[3] != None and onedata[3] != 'NULL':
                    if int(onedata[3]) > 0:
                        sleep(int(onedata[3]))
                if dev_index >=0 and conn_list[conn_index].isonline == 1:
                    if controller_list[dev_index].devtype=='1':
                        sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:] + ' '
                        sendstr = sendstr + '72' + ' '
                        sendstr = sendstr + ('00'+hex(icontlnum).replace('0x',''))[-2:] + ' '
                        sendstr = sendstr + '00 00' + ' '
                        sendstr = sendstr + mymodule.crc16(sendstr,0) 
                        sendstr=sendstr.replace(' ','')
                        sendresult = sock.sendall(bytes().fromhex(sendstr))
                        if sendresult==None:
                            showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+ ' '+deviceno+' '+'CLOSE : '+sendstr
                            mymodule.create_log(showinfo)
                            break
                        else:
                            sleep(60)
                            showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+ ' '+deviceno+' '+'CLOSE : '+sendstr+ ' faile times:' + str(sendtimes)
                            sendtimes=sendtimes+1
                            
                    elif controller_list[dev_index].devtype=='3':
                        sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:] + ' '
                        sendstr = sendstr + '72' + ' '
                        sendstr = sendstr + ('00'+hex(icontlnum).replace('0x',''))[-2:] + ' '
                        sendstr = sendstr + '00 00' + ' '
                        sendstr = sendstr + mymodule.crc16(sendstr,0) 
                        sendstr=sendstr.replace(' ','')
                        sendresult = sock.sendall(bytes().fromhex(sendstr))
                        if sendresult == None:
                            sleep(2)
                            sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:] + ' '
                            sendstr = sendstr + '72' + ' '
                            sendstr = sendstr + ('00'+hex(icontlnum+1).replace('0x',''))[-2:] + ' '
                            sendstr = sendstr + '01 00' + ' '
                            sendstr = sendstr + mymodule.crc16(sendstr,0) 
                            sendstr=sendstr.replace(' ','')
                            sendresult1 = sock.sendall(bytes().fromhex(sendstr))
                            if sendresult1 == None:
                                showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+ ' '+deviceno+' '+'CLOSE : '+sendstr
                                mymodule.create_log(showinfo)
                                break
                            else:
                                showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+ ' '+deviceno+' '+'CLOSE : '+sendstr+ ' faile times:' + str(sendtimes)
                                sendtimes=sendtimes+1                            
                        else:
                            sleep(60)
                            showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+ ' '+deviceno+' '+'CLOSE : '+sendstr+ ' faile times:' + str(sendtimes)
                            sendtimes=sendtimes+1
            elif  onedata[2] == 'AC-STOP':
                deviceno=onedata[7]
                if onedata[3] != None and onedata[3] != 'NULL':
                    if int(onedata[3]) > 0:
                        sleep(int(onedata[32]))
                if dev_index >=0 and conn_list[conn_index].isonline == 1:
                    if controller_list[dev_index].devtype=='1':
                        sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:] + ' '
                        sendstr = sendstr + '72' + ' '
                        sendstr = sendstr + ('00'+hex(icontlnum).replace('0x',''))[-2:] + ' '
                        sendstr = sendstr + '00 00' + ' '
                        sendstr = sendstr + mymodule.crc16(sendstr,0) 
                        sendstr=sendstr.replace(' ','')
                        sendresult = sock.sendall(bytes().fromhex(sendstr))
                        if sendresult == None:
                            showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+ ' '+deviceno+' '+'STOP : '+sendstr
                            mymodule.create_log(showinfo)
                            break
                        else:
                            sleep(60)
                            showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+ ' '+deviceno+' '+'STOP : '+sendstr+ ' faile times:' + str(sendtimes)
                            sendtimes=sendtimes+1
                    elif controller_list[dev_index].devtype=='3':
                        sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:] + ' '
                        sendstr = sendstr + '72' + ' '
                        sendstr = sendstr + ('00'+hex(icontlnum).replace('0x',''))[-2:] + ' '
                        sendstr = sendstr + '00 00' + ' '
                        sendstr = sendstr + mymodule.crc16(sendstr,0) 
                        sendstr=sendstr.replace(' ','')
                        sendresult = sock.sendall(bytes().fromhex(sendstr))
                        if sendresult == None:
                            sleep(1)
                            sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:] + ' '
                            sendstr = sendstr + '72' + ' '
                            sendstr = sendstr + ('00'+hex(icontlnum+1).replace('0x',''))[-2:] + ' '
                            sendstr = sendstr + '00 00' + ' '
                            sendstr = sendstr + mymodule.crc16(sendstr,0) 
                            sendstr=sendstr.replace(' ','')
                            sendresult1 = sock.sendall(bytes().fromhex(sendstr))
                            if sendresult1 == None:
                                showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+ ' '+deviceno+' '+'STOP : '+sendstr
                                mymodule.create_log(showinfo)
                                break
                            else:
                                sleep(60)
                                showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+ ' '+deviceno+' '+'STOP : '+sendstr+ ' faile times:' + str(sendtimes)
                                sendtimes=sendtimes+1
                        else:
                            sleep(60)
                            showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+ ' '+deviceno+' '+'STOP : '+sendstr+ ' faile times:' + str(sendtimes)
                            sendtimes=sendtimes+1
            elif onedata[2] == 'COLLECT-DATA':
                if conn_list[conn_index].isonline == 1:
                    sendstr= ('00'+hex(int(comm_list[comm_index].commaddr)).replace('0x',''))[-2:]
                    sendstr=sendstr+' '+ '03'
                    sendstr=sendstr+' '+ '00 00' #起始地址
                    sendstr=sendstr+' '+ '00 10' #数量
                    sendstr=sendstr+' '+ mymodule.crc16(sendstr,0)
                    sendstr=sendstr.replace(' ','')
                    sendresult = sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                    if sendresult == None:
                        showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn + ')'+' Collect DATA: '+sendstr
                        mymodule.create_log(showinfo)
                        sleep(0.5)
                        break
                    else:
                        sleep(60)
                        showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn + ')'+' '+ deviceno+' Collect DATA: '+sendstr+ ' failed times:' + str(sendtimes)
                        sendtimes=sendtimes+1
            elif onedata[2] == 'GET-STATE': 
                if conn_list[conn_index].isonline == 1:
                    sendstr='00 70 00 54'
                    sendresult = sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                    if sendresult == None:
                        showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn + ')'+' GET STATUS: '+sendstr
                        mymodule.create_log(showinfo)
                        sleep(0.5)
                        break
                    else:
                        sleep(60)
                        showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn + ')'+' GET STATUS: '+sendstr+ ' failed times:' + str(sendtimes)
                        sendtimes=sendtimes+1       
            elif  onedata[2] == 'INTERVAL':
                if onedata[3] != None and onedata[3] != 'NULL': 
                    if int(onedata[3]) > 0:
                        sleep(int(onedata[3]))
                if conn_list[conn_index].isonline == 1:        
                    showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn+ ')'+' '+'SET STATUS : '+str(onedata[4])
                    mymodule.create_log(showinfo)
        except Exception as err:
            showinfo =mymodule.getcurrtime()+' '+ 'FKC('+comm_sn + ') send order failture : '+str(err)
            mymodule.create_log(showinfo)
            sendtimes=sendtimes+1
            sleep(60)
        if sendtimes > 5:
            conn_list[conn_index].isonline = 0
            mymodule.create_log(showinfo)
            showinfo=mymodule.getcurrtime()+' '+'FKC('+comm_sn + ')'+' shutdown for send over 3 times '
            mymodule.create_log(showinfo)
            break
    if dev_index >= 0:
        reslSQL='update yw_c_control_log_tbl set ExecuteTime=%s,ExecuteResult=%s where id = "' + str(onedata[0]) + '"'
        reslconn=mysqlpool.connection()
        reslcursor=reslconn.cursor()
        reslcursor.execute(reslSQL,args=(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'4'))
        reslconn.commit() 
        reslcursor.close()
        reslconn.close() 
    
    
def DYC_handlerecv(comm_index,sdata):
    comm_sn=comm_list[comm_index].serial_num
    temp1=sdata[:-4]
    temp2=sdata[-4:]
    a=mymodule.crc16(temp1,0).replace(' ','')

    if True:
        sOrder=sdata[2:4]
        #获取记录
        try:
            if sOrder == '03' and len(sdata)==14: #主动采集浓度数据
                rData=int(sdata[6:10],16)/10000
                sensor_index= find_devaddr(1,comm_sn,11)
                sSQL="INSERT INTO yw_c_sensordata_tbl (Device_ID,Device_Code,ReportTime,ReportValue,Block_ID) values"
                if sensor_index >= 0:
                    sSQL = sSQL+"('"+str(sensor_list[sensor_index].devid)+"','"+sensor_list[sensor_index].devcode+"','"+mymodule.getcurrtime()+"','"+str(rData)+"','"+str(sensor_list[sensor_index].blockid)+"'),"
                try:
                    if sSQL[-2:] =='),':
                        sSQL=sSQL[0:-1]+";"
                        comtconn=mysqlpool.connection()
                        comtcursor=comtconn.cursor()
                        comtcursor.execute(sSQL)
                        showinfo=mymodule.getcurrtime() + ' insert (' + comm_sn + ')'+ ' sensor data : '  + sSQL
                        mymodule.create_log(showinfo)                    
                        comtconn.commit()  
                        comtcursor.close()
                        comtconn.close()
                except Exception as err:
                    showinfo = mymodule.getcurrtime()+ ' DYC('+comm_sn + ') insert data failure: '+str(err)
                    mymodule.create_log(showinfo) 
            elif sOrder == '06' and len(sdata)==16: #被动获取浓度数据:
                rData=int(sdata[8:12],16)/10000
                sensor_index= find_devaddr(1,comm_sn,11)
                sSQL="INSERT INTO yw_c_sensordata_tbl (Device_ID,Device_Code,ReportTime,ReportValue,Block_ID) values"
                if sensor_index >= 0:
                    sSQL = sSQL+"('"+str(sensor_list[sensor_index].devid)+"','"+sensor_list[sensor_index].devcode+"','"+mymodule.getcurrtime()+"','"+str(rData)+"','"+str(sensor_list[sensor_index].blockid)+"'),"
                try:
                    if sSQL[-2:] =='),':
                        sSQL=sSQL[0:-1]+";"
                        comtconn=mysqlpool.connection()
                        comtcursor=comtconn.cursor()
                        comtcursor.execute(sSQL)
                        showinfo=mymodule.getcurrtime() + ' insert  DYC(' + comm_sn + ')'+ ' sensor data : '  + sSQL
                        mymodule.create_log(showinfo)                    
                        comtconn.commit()  
                        comtcursor.close()
                        comtconn.close()
                except Exception as err:
                    showinfo = mymodule.getcurrtime()+ ' DYC('+comm_sn + ') insert data failure: '+str(err)
                    mymodule.create_log(showinfo)                 
            elif sOrder == '10' and len(sdata) > 128: #任务记录
                try:
                    tasklog=[]
                    for i in range(0,32):
                        hexdata=sdata[i*4+14:i*4+18]
                        tasklog.append(int(hexdata,16))
                    isRemote=int(tasklog[0])
                    taskstate=int(tasklog[3])
                    density_set=tasklog[4]/10000
                    inteval_set=int(tasklog[5]/60) #分钟
                    task_mode=int(tasklog[6])
                    commid=comm_list[comm_index].commid
                    scheduledtime=('00'+str(tasklog[10]))[-2:]+':'+('00'+str(tasklog[11]))[-2:]+':'+('00'+str(tasklog[12]))[-2:]
                    taskdate=str(tasklog[7])+'-'+('00'+str(tasklog[8]))[-2:]+'-'+('00'+str(tasklog[9]))[-2:]
                    starttime=taskdate+' '+('00'+str(tasklog[14]))[-2:]+':'+('00'+str(tasklog[15]))[-2:]+':'+('00'+str(tasklog[16]))[-2:]
                    endtime=taskdate+' '+('00'+str(tasklog[19]))[-2:]+':'+('00'+str(tasklog[20]))[-2:]+':'+('00'+str(tasklog[21]))[-2:]
                    interval_real=int(tasklog[17]/60)
                    if taskstate > 0 and  taskstate < 3:  
                        reslconn=mysqlpool.connection()
                        reslcursor=reslconn.cursor()
                        sResult = "INSERT INTO yw_g_tasklog_tbl (TaskDate,IsRemote,TaskMode,SetDensity,SetInterval,Commucation_ID, "
                        sResult = sResult +"ExecuteTime,ExecuteResult,ActualInterval,CreateTime) "
                        sResult = sResult + "values('"+taskdate+"','"+str(isRemote)+"','"+str(task_mode)+"','"+str(density_set)+"','"+str(inteval_set)+"','"+str(commid)+"',"
                        sResult = sResult + "'"+endtime+"','1','"+str(interval_real)+"',now())"
                        reslcursor.execute(sResult) 
                        reslconn.commit()
                        
                        showinfo=mymodule.getcurrtime()+' '+'DYC('+comm_sn + ')'+' '+ 'add a new task log'
                        mymodule.create_log(showinfo)
                        sleep(0.5)
                        
                        reslcursor.close()
                        reslconn.close()
                        
                except Exception as err:
                    showinfo =mymodule.getcurrtime()+ ' DYC('+comm_sn + ') get control status failture:'+str(err)
                    mymodule.create_log(showinfo)
                    
                    
            #020507d100009cb4020f0c5000040100be43
            elif sOrder == '0f' or (sOrder == '05' and sdata[16:20]== '020f' ):
                if sOrder == '0f':           
                #0000201910180001 020503e8ff000c79 020f 0c50(addr) 0010(out num) 02(byte num) 3f02(数据) a6b1指令回传
                    lendata=int(sdata[8:12],16)
                    dataset=sdata[14:-4]
                elif sOrder == '05' and sdata[16:20]=='020f':
                    lendata=int(sdata[24:28],16)
                    dataset=sdata[30:-4]
                if lendata > 0:
                    sSQL="insert into yw_d_controller_tbl(id,Code,PortNum,Commucation_ID,onoff) values "
                    try:
                        tempa=''
                        tempb=''
                        statusvar=''
                        sstate=''
                        for i in range(0,int(len(dataset)/2)):
                            tempa=dataset[i*2:i*2+2] #注意modbus标准版和矩形modbus区别，modbus按顺序
                            tempb=tempb+('00000000'+bin(int(tempa,16)).replace('0b',''))[-8:][::-1]
                            statusvar=tempb
                        for i in range(0,lendata):
                            control_index=-1
                            if i < comm_list[comm_index].controlmax:
                                control_index= find_devaddr(i+1,comm_sn,10)
                            else:
                                continue
                            if control_index >=0:
                                if controller_list[control_index].devtype=='1':  
                                    contrlstate = statusvar[i]
                                    if  int(contrlstate,16)==1:
                                        sstate='1'
                                    elif int(contrlstate,16)==0:
                                        sstate='2'
                                    else:
                                        print('state unnomal')
                                elif controller_list[control_index].devtype=='3':
                                    contrlstate = statusvar[i]
                                    closestate = statusvar[i+1]
                                    if  int(contrlstate,16)==1 and int(closestate,16)==0:
                                        sstate='1' #开
                                    elif int(contrlstate,16)==0 and int(closestate,16)==1:
                                        sstate='2' #关
                                    elif int(contrlstate,16)==0 and int(closestate,16)==0:
                                        sstate='3' #停   
                                    else:
                                        print('state unnomal')
                                sSQL = sSQL+"('"+str(controller_list[control_index].devid)+"','"+str(controller_list[control_index].devcode)+"',"
                                sSQL = sSQL+"'"+str(controller_list[control_index].devaddr)+"',"
                                sSQL = sSQL+"'"+str(controller_list[control_index].commnum)+"','"+sstate+"'),"
                    except Exception as err:
                        showinfo =mymodule.getcurrtime()+ ' DYC('+comm_sn + ') get control status failture:'+str(err)
                        mymodule.create_log(showinfo)
    
                    try:
                        if sSQL[-2:] =='),':
                            sSQL=sSQL[0:-1]+" ON DUPLICATE KEY UPDATE onoff = values(onoff);"    
                            staconn=mysqlpool.connection()
                            stacursor=staconn.cursor()
                            showinfo=mymodule.getcurrtime() + ' Get DYC(' + comm_sn + ')'+ ' device state : '  + sSQL
                            mymodule.create_log(showinfo) 
                            stacursor.execute(sSQL)
                            staconn.commit() 
                            stacursor.close()
                            staconn.close()    
                    except Exception as err:
                        showinfo =mymodule.getcurrtime()+ ' DYC('+comm_sn + ') update control status failture:'+str(err)
                        mymodule.create_log(showinfo)
                         
        except Exception as err:
            showinfo =mymodule.getcurrtime()+  ' DYC('+comm_sn + ') handle received data failture:'+str(err)
            mymodule.create_log(showinfo)
def DYC_sendorder(dev_index,comm_index,conn_index,onedata):
#任务ID，发送日期，设置时长，设置浓度，预计时间，通讯设备序列号，通讯地址      
    sendtimes=0
    sendresult=''
#test    
    while True:
        if  dev_index != -10 and comm_index != -10 and onedata != None:
            try:    
                comm_sn=comm_list[comm_index].serial_num
                deviceno=onedata[7]
                icommnum = int(comm_list[comm_index].commaddr)
                icontlnum = int(controller_list[dev_index].devaddr)
                sock=conn_list[conn_index].sock
                if onedata[2] == 'AC-OPEN':
                    if onedata[3] != None and onedata[3] != 'NULL':
                        if int(onedata[3]) > 0:
                            sleep(int(onedata[3])) #延迟
                    if dev_index >=0 and conn_list[conn_index].isonline == 1:
                        if controller_list[dev_index].devtype=='1':
                            sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:]
                            sendstr = sendstr + ' ' + '05'
                            scontlnum = ('0000'+hex(icontlnum-1+2000).replace('0x',''))[-4:]
                            sendstr = sendstr +' '+  scontlnum[:2] + ' '+scontlnum[-2:]     #起始地址
                            sendstr = sendstr +' '+ 'FF 00' #输出数量+字节数+输出值
                            sendstr = sendstr +' '+ mymodule.crc16(sendstr,0)
                            sendstr=sendstr.replace(' ','')
                            sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                        if sendresult== None:
                            showinfo=mymodule.getcurrtime()+' '+'DYC('+comm_sn+ ')'+ ' '+deviceno+' '+'OPEN: '+sendstr
                            mymodule.create_log(showinfo)
                            reslSQL='update yw_c_control_log_tbl set ExecuteTime=%s,ExecuteResult=%s where id = "' + str(onedata[0]) + '"'
                            reslconn=mysqlpool.connection()
                            reslcursor=reslconn.cursor()
                            reslcursor.execute(reslSQL,args=(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'4'))
                            reslconn.commit() 
                            reslcursor.close()
                            reslconn.close()
                            break
                        else:
                            sleep(60)
                            showinfo=mymodule.getcurrtime()+' '+'DYC('+comm_sn+ ')'+ ' '+deviceno+' '+'OPEN: '+sendstr+ ' failed times:' + str(sendtimes)
                            sendtimes=sendtimes+1
                elif  onedata[2] == 'AC-CLOSE':
                    if onedata[3] != None and onedata[3] != 'NULL':
                        if int(onedata[3]) > 0:
                            sleep(int(onedata[3])) 
                    if dev_index >=0 and conn_list[conn_index].isonline == 1:
                        if controller_list[dev_index].devtype=='1':
                            sendstr = ('00'+hex(icommnum).replace('0x',''))[-2:]
                            sendstr = sendstr + ' ' + '05'
                            scontlnum = ('0000'+hex(icontlnum-1+5000).replace('0x',''))[-4:]
                            sendstr = sendstr +' '+  scontlnum[:2] + ' '+scontlnum[-2:]     #起始地址
                            sendstr = sendstr +' '+ 'FF 00' #输出数量+字节数+输出值
                            sendstr = sendstr +' '+ mymodule.crc16(sendstr,0)
                            sendstr=sendstr.replace(' ','')
                            sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                    if sendresult== None:
                        showinfo=mymodule.getcurrtime()+' '+'DYC('+comm_sn+ ')'+ ' '+deviceno+' '+'CLOSE: '+sendstr
                        mymodule.create_log(showinfo)
                        reslSQL='update yw_c_control_log_tbl set ExecuteTime=%s,ExecuteResult=%s where id = "' + str(onedata[0]) + '"'
                        reslconn=mysqlpool.connection()
                        reslcursor=reslconn.cursor()
                        reslcursor.execute(reslSQL,args=(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'4'))
                        reslconn.commit() 
                        reslcursor.close()
                        reslconn.close()
                        break
                    else:
                        sleep(60)
                        showinfo=mymodule.getcurrtime()+' '+'DYC('+comm_sn+ ')'+ ' '+deviceno+' '+'CLOSE: '+sendstr+ ' failed times:' + str(sendtimes)
                        sendtimes=sendtimes+1
            except Exception as err:
                showinfo =mymodule.getcurrtime()+  ' DYC('+comm_sn + ') send order failture : '+str(err)
                mymodule.create_log(showinfo)
                sendtimes=sendtimes+1
                sleep(60)
            if sendtimes > 5:
                addr=conn_list[conn_index].addr
                close_sock(sock,addr,comm_sn)
                mymodule.create_log(showinfo)
                showinfo=mymodule.getcurrtime()+' '+'DYC('+comm_sn + ')'+' task shutdown for send over 3 times '
                mymodule.create_log(showinfo)
                reslSQL='update yw_c_control_log_tbl set ExecuteTime=%s,ExecuteResult=%s where id = "' + str(onedata[0]) + '"'
                reslconn=mysqlpool.connection()
                reslcursor=reslconn.cursor()
                reslcursor.execute(reslSQL,args=(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'4'))
                reslconn.commit() 
                reslcursor.close()
                reslconn.close()
                break        
            
            
        if  dev_index == -10 and comm_index == -10 and onedata != None: 
            #任务ID，发送日期，设置时长，设置浓度，预计时间，通讯设备序列号，通讯地址
            try:
                taskinterval=onedata[2]
                taskdestity=onedata[3]
                scheduledtime=onedata[4]
                comm_sn=onedata[5]
                PLCAddress=onedata[6]
                conn_index=find_conn_sn(comm_sn)
                if conn_index >= 0:
                    sock=conn_list[conn_index].sock
                    senddestity=('0000'+hex(int(taskdestity*10000)).replace('0x',''))[-4:]
                    sendinterval=('0000'+hex(taskinterval*60).replace('0x',''))[-4:]
                    sendyear=('0000'+hex(scheduledtime.year).replace('0x',''))[-4:]
                    sendmonth=('0000'+hex(scheduledtime.month).replace('0x',''))[-4:]
                    sendday=('0000'+hex(scheduledtime.day).replace('0x',''))[-4:]
                    sendhour=('0000'+hex(scheduledtime.hour).replace('0x',''))[-4:]
                    sendminute=('0000'+hex(scheduledtime.minute).replace('0x',''))[-4:]
                    sendsecond=('0000'+hex(scheduledtime.second).replace('0x',''))[-4:]
        
                    sendstr=('00'+hex(int(PLCAddress,10)).replace('0x',''))[-2:] #设备地址
                    sendstr=sendstr+' '+'10' #指令
                    sendstr=sendstr+' '+'01 2B' #起始地址,40300(减1)
                    sendstr=sendstr+' '+'00 07' #寄存器数量n
                    sendstr=sendstr+' '+'0E' #字节数2*n
                    sendstr = sendstr + ' ' + '00 01' #传输的数据1：远程控制
                    sendstr = sendstr + ' ' + '00 00 00 00' #传输的数据2：指令ID
                    sendstr = sendstr + ' ' + '00 00' #传输的数据4：任务状态，0-未开始
                    sendstr = sendstr + ' ' + senddestity[0:2] + ' ' + senddestity[2:4]#传输的数据5：设定浓度
                    sendstr = sendstr + ' ' + sendinterval[0:2] + ' ' + sendinterval[2:4] #传输的数据6：设定时长
                    sendstr = sendstr + ' ' + '00 00' #传输的数据7：任务模式：0-即时任务
                    sendstr = sendstr +' '+ mymodule.crc16(sendstr,0)
                    sendstr=sendstr.replace(' ','')
                    sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                    if sendresult== None:
                        showinfo=mymodule.getcurrtime()+' '+'DYC('+comm_sn+ ')'+ ' Task send: '+sendstr
                        mymodule.create_log(showinfo) 
                        reslSQL='update yw_g_taskorder_tbl set ExecuteTime=%s,ExecuteResult=%s where id = "' + str(onedata[0]) + '"'
                        reslconn=mysqlpool.connection()
                        reslcursor=reslconn.cursor()
                        reslcursor.execute(reslSQL,args=(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'4'))
                        reslconn.commit() 
                        reslcursor.close()
                        reslconn.close()
                        break
                    else:
                        sleep(60)
                        showinfo=mymodule.getcurrtime()+' '+'DYC('+comm_sn+ ')'+ ' '+deviceno+' '+'Task send: '+sendstr+ ' failed times:' + str(sendtimes)
                        sendtimes=sendtimes+1
            except Exception as err:
                showinfo =mymodule.getcurrtime()+  ' DYC('+comm_sn + ') Task send order failture : '+str(err)
                mymodule.create_log(showinfo)
                sendtimes=sendtimes+1
                sleep(60)
            if sendtimes > 5:
                conn_list[conn_index].isonline = 0
                mymodule.create_log(showinfo)
                showinfo=mymodule.getcurrtime()+' '+'DYC('+comm_sn + ')'+' '+ deviceno+' shutdown for send over 3 times '
                mymodule.create_log(showinfo)        
                reslSQL='update yw_g_taskorder_tbl set ExecuteTime=%s,ExecuteResult=%s where id = "' + str(onedata[0]) + '"'
                reslconn=mysqlpool.connection()
                reslcursor=reslconn.cursor()
                reslcursor.execute(reslSQL,args=(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'4'))
                reslconn.commit() 
                reslcursor.close()
                reslconn.close()
                break

def collect_data(comm_index):
    #下发采集任务
    sendstr=''
    sendresult=''
    showinfo=''
    comm_sn=comm_list[comm_index].serial_num
    conn_index=find_conn_sn(comm_sn)
    sock=conn_list[conn_index].sock
    addr=conn_list[conn_index].addr
    collect_times=0  #重复发送任务次数
    commtype=comm_list[comm_index].commtype
    #超时
    if conn_list[conn_index].overtime > default_overtime:
        conn_list[conn_index].isonline=0
    
    try:
        if conn_list[conn_index].isonline==1 and conn_index >=0:
            sendresult=False
            if commtype=='SFJ-0804' or commtype=='SFJ-1200':
                #未来考虑检测液位、流量等
                pass
            elif commtype=='FKC':
                #采集任务信息
                collect_thread=myThread(target=FKC_sendorder,args=(-1,comm_index,conn_index,[0,0,"COLLECT-DATA"]))
                collect_thread.start()

            elif commtype=='XPC':
                collect_thread=myThread(target=XPC_sendorder,args=(-1,comm_index,conn_index,[0,0,"COLLECT-DATA"]))
                collect_thread.start()

            elif commtype=='KLC':
                collect_thread=myThread(target=KLC_sendorder,args=(-1,comm_index,conn_index,[0,0,"COLLECT-DATA"]))
                collect_thread.start()

            elif commtype=='YYC':
                collect_thread=myThread(target=YYC_sendorder,args=(-1,comm_index,conn_index,[0,0,"COLLECT-DATA"]))
                collect_thread.start()

            elif commtype=='PLC':
                pass

            elif commtype =='DYC':
                pass

    except Exception as e:
        showinfo=mymodule.getcurrtime()+' '+'('+comm_sn + ') collect data error:'  +str(e)
        mymodule.create_log(showinfo) 
            
#获取状态       
def get_state(comm_index):
    sendstr=''
    showinfo=''
    comm_sn=comm_list[comm_index].serial_num
    conn_index=find_conn_sn(comm_sn)
    sock=conn_list[conn_index].sock
    addr=conn_list[conn_index].addr
    collect_times=0  #重复发送任务次数
    commtype=comm_list[comm_index].commtype
    if conn_list[conn_index].overtime > default_overtime:
        conn_list[conn_index].isonline=0
    try:
        
        if conn_list[conn_index].isonline==1 and conn_index >=0:
            sendresult = False
            sendstr=''
            if commtype=='SFJ-0804' or commtype=='SFJ-1200':
                pass
            elif commtype=='FKC': 
                getstate_thread=myThread(target=FKC_sendorder,args=(-1,comm_index,conn_index,[0,0,"GET-STATE"]))
                getstate_thread.start()
                '''
                sendstr='00 70 00 54'
                sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                '''
            elif commtype=='KLC':
                getstate_thread=myThread(target=KLC_sendorder,args=(-1,comm_index,conn_index,[0,0,"GET-STATE"]))
                getstate_thread.start()
                '''
                sendstr= '15 01 00 00 00 06 02'
                sendstr=sendstr+' '+ '03'
                sendstr=sendstr+' '+ '00 00' #起始地址
                sendstr=sendstr+' '+ '00 04' #数量
                sendstr=sendstr+' '+ mymodule.crc16(sendstr,0)
                sendresult=sock.sendall(bytes().fromhex(sendstr.replace(' ','')))
                '''
            elif commtype=='XPC' or commtype == 'DYC' or commtype == 'PLC':
                pass
            '''
            if sendresult == None:
                conn_list[conn_index].lasttime=datetime.now()
                overtimegap=datetime.now()-conn_list[conn_index].lasttime
                conn_list[conn_index].overtime=overtimegap.days*24*3600+overtimegap.seconds
                showinfo=mymodule.getcurrtime() + ' get (' + comm_sn + ')'+ 'state:'  + sendstr + ' succeed'
                mymodule.create_log(showinfo) 
            else:
                showinfo=mymodule.getcurrtime() + ' get (' + comm_sn + ')'+ 'state:'  + sendstr + ' failed'
                mymodule.create_log(showinfo)
            '''
    except Exception as e:
        showinfo=mymodule.getcurrtime()+' '+'('+comm_sn + ') get status error:'  +str(e)
        mymodule.create_log(showinfo)                         


#连接数据库
def connect_sockserver():
    try:
        server_ip=mymodule.read_conf('SERVER','server_ip')
        server_port=mymodule.read_conf('SERVER','server_port')
        #s.setsockopt(SOL_SOCKET,SO_REUSEADDR,1) 
        s.bind((server_ip,int(server_port)))
        mymodule.create_log(mymodule.getcurrtime()+' save bind succeed')
        s.listen(5)
        mymodule.create_log(mymodule.getcurrtime()+' server listening succeed')
        if commtype_valid == 0:
            IOT_sendorder_thread=myThread(target=IOT_send_thread,args=())
            IOT_sendorder_thread.start()
        elif commtype_valid == 1:    
            SFJ_sendorder_thread=myThread(target=SFJ_send_thread,args=())
            SFJ_sendorder_thread.start()
        elif commtype_valid == 2: 
            IOT_sendorder_thread=myThread(target=IOT_send_thread,args=())
            IOT_sendorder_thread.start() 
            SFJ_sendorder_thread=myThread(target=SFJ_send_thread,args=())
            SFJ_sendorder_thread.start()   
        elif commtype_valid == 3: 
            IOT_sendorder_thread=myThread(target=IOT_send_thread,args=())
            IOT_sendorder_thread.start() 
            SFJ_sendorder_thread=myThread(target=DYJ_send_thread,args=())
            SFJ_sendorder_thread.start()       
            
    except Exception as e:
        showinfo=mymodule.getcurrtime()+' connect sockserver failure:'+str(e)
        mymodule.create_log(showinfo) 
    
def get_devinfo():
#水肥机通讯设备：ID,编号，序列号，通讯地址，公司ID，类型，参数,类别，通道数量
#编号：无，暂以名称代替
#参数：水肥机-肥液路数,通讯设备-采集间隔
#设备类型：水肥机：SFJ-0804,SFJ-1200
#通道数量：水肥机：控制通道数，通讯设备：控制通道数
#网关类别：2-水肥机
    if commtype_valid == 1 or commtype_valid == 2 :
        sSQL='SELECT e.ID,e.PLC_Name,e.PLC_Number,e.PLC_Address,e.Company_ID,e.PLC_GWType,e.PassNumber,MAX(controladdr),MAX(sensoraddr) FROM '
        sSQL=sSQL+'(SELECT a.ID,PLC_Name,a.PLC_Number,PLC_Address,a.Company_ID,PLC_GWType,PassNumber,CAST(b.`Device_Address` AS UNSIGNED) AS controladdr, NULL sensoraddr FROM sfyth_plc a '
        sSQL=sSQL+'LEFT JOIN `sfyth_device` b ON b.`PLC_Number` = a.`PLC_Number` '
        sSQL=sSQL+'WHERE a.Is_Delete = 0 AND b.`Is_Delete` = 0 AND CAST(b.`Device_Type` AS UNSIGNED) < 7 '
        sSQL=sSQL+'UNION ALL '
        sSQL=sSQL+'SELECT c.ID,PLC_Name,c.PLC_Number,PLC_Address,c.Company_ID,PLC_GWType,PassNumber,NULL controladdr, CAST(d.`Device_Address` AS UNSIGNED) AS sensoraddr FROM sfyth_plc c '
        sSQL=sSQL+'LEFT JOIN `sfyth_device` d ON d.`PLC_Number` = c.`PLC_Number` '
        sSQL=sSQL+'WHERE c.Is_Delete = 0 AND d.`Is_Delete` = 0 AND CAST(d.`Device_Type` AS UNSIGNED) > 7) e GROUP BY e.plc_number'
        try:
            recconn=mysqlpool.connection()
            reccursor=recconn.cursor()
            reccursor.execute(sSQL)
            while True:
                onedata = reccursor.fetchone()
                if not onedata:
                    break;
                else:
                    #重新扫描时，无需刷新原有的设备顺序
                    commindex=-1
                    if len(comm_list) > 0:
                        for i in comm_list:
                            if i.commid == onedata[0] and i.commclass ==2:
                                commindex=0
                                break
                    if commindex==-1 or len(comm_list)==0:
                        onecomm=comm_class(onedata[0],onedata[1],onedata[2],onedata[3],onedata[4],onedata[5],onedata[6],2,onedata[7],onedata[8])
                        comm_list.append(onecomm)
            reccursor.close()                 
            recconn.close()
        except Exception as err:
            showinfo=mymodule.getcurrtime()+' SFJ-communication configure get failure：'+str(err)
            mymodule.create_log(showinfo)
    #水肥机控制:ID,编号，地址，类型，所属网关序号、设备参数、设备地块、设备公式、类别 
    #编号：无，暂以名称代替             
    #设备类型:1-灌区，2-泵，3-施肥阀，4-清洁，5-报警，6-肥液
    #设备参数：0，无意义
    #设备地块：0,无意义
    #设备公式：0，无意义
    #类别：10-控制器，11-传感器，20-水肥机控制器，21-水肥机传感器；
        sSQL='SELECT a.ID,a.Device_Name,a.Device_Address,a.Device_Type,a.PLC_Number FROM sfyth_device a '
        sSQL=sSQL+'LEFT JOIN sfyth_plc b ON a.PLC_Number = b.PLC_Number WHERE a.Is_Delete = 0 AND b.Is_Delete = 0'
        try:
            recconn=mysqlpool.connection()
            reccursor=recconn.cursor()
            reccursor.execute(sSQL)
            while True:
                onedata = reccursor.fetchone()
                if not onedata:
                    break;
                else:
                    devindex=-1
                    if len(controller_list) > 0:
                        for i in controller_list:
                            if i.devid == onedata[0] and i.devclass ==2:
                                devindex=0
                                break
                    if devindex==-1 or len(controller_list)==0:
                        onecomm=controller_class(onedata[0],onedata[1],int(onedata[2]),onedata[3],onedata[4],0,0,'0',20)
                        controller_list.append(onecomm)
            reccursor.close()                 
            recconn.close() 
        except Exception as err:
            showinfo=mymodule.getcurrtime()+' SFJ-controller configure get failure：'+str(err)
            mymodule.create_log(showinfo)        

#物联网通讯设备：ID,编号，序列号，通讯地址，公司ID，类型，参数,类别，控制通道最大数量，传感通道最大通道数
#设备参数：KLC、PLC、XPC、FKC、YYC
#参数：通讯设备-采集间隔
#网关类别：1-物联网
    if commtype_valid == 0 or commtype_valid == 2 or commtype_valid == 3:
        sSQL='SELECT n.ID,n.Code,n.SerialNumber,n.CodeAddress,n.Company_ID,n.ClassName,n.`Interval`, COUNT(controlport) AS controlnum,COUNT(sensorport) AS sensorsum FROM '
        sSQL=sSQL+'(SELECT e.*,NULL controlport,f.PortNum AS sensorport FROM '
        sSQL=sSQL+'(SELECT a.ID,a.Code,a.SerialNumber,a.CodeAddress,a.Company_ID,c.ClassName,`Interval` FROM yw_d_commnication_tbl a '
        sSQL=sSQL+'LEFT JOIN yw_d_devicemodel_tbl b ON a.Model_ID = b.ID '
        sSQL=sSQL+'LEFT JOIN yw_d_deviceclass_tbl c ON b.DeviceClass_ID= c.ID '
        sSQL=sSQL+'LEFT JOIN ys_parameter_tbl d ON b.Formula = d.Parameter_Key '
        sSQL=sSQL+'WHERE a.UsingState="1" AND b.State="1" AND d.Parameter_Class = "YW-JSGS") e '
        sSQL=sSQL+'LEFT JOIN yw_d_sensor_tbl f ON f.Commucation_ID = e.id where f.`UsingState` = "1" '
        sSQL=sSQL+'UNION ALL '
        sSQL=sSQL+'SELECT l.*,m.PortNum,NULL sensorport FROM '
        sSQL=sSQL+'(SELECT g.ID,g.Code,g.SerialNumber,g.CodeAddress,g.Company_ID,i.ClassName,`Interval` FROM yw_d_commnication_tbl g '
        sSQL=sSQL+'LEFT JOIN yw_d_devicemodel_tbl h ON g.Model_ID = h.ID '
        sSQL=sSQL+'LEFT JOIN yw_d_deviceclass_tbl i ON h.DeviceClass_ID= i.ID '
        sSQL=sSQL+'LEFT JOIN ys_parameter_tbl k ON h.Formula = k.Parameter_Key '
        sSQL=sSQL+'WHERE g.UsingState="1" AND h.State="1" AND k.Parameter_Class = "YW-JSGS") l '
        sSQL=sSQL+'LEFT JOIN yw_d_controller_tbl m ON m.Commucation_ID = l.id WHERE m.UsingState = "1") n '
        sSQL=sSQL+'GROUP BY n.id'
        try:
            recconn=mysqlpool.connection()
            reccursor=recconn.cursor()
            reccursor.execute(sSQL)
            while True:
                onedata = reccursor.fetchone()
                if not onedata:
                    break
                else:
                    commindex=-1
                    if len(comm_list) > 0:
                        for i in comm_list:
                            if i.commid == onedata[0] and i.commclass ==1:
                                    commindex=0
                                    break
                    if commindex==-1 or len(comm_list)==0:
                        onecomm=comm_class(onedata[0],onedata[1],onedata[2],onedata[3],onedata[4],onedata[5],onedata[6],1,onedata[7],onedata[8])
                        #onecomm=onedata
                        comm_list.append(onecomm)
            reccursor.close()                 
            recconn.close()
    
        except Exception as err:
            showinfo=mymodule.getcurrtime()+' IOT-communication configure get failure：'+str(err)
            mymodule.create_log(showinfo)
            
    #传感器：#ID,编号，地址，类型，所属网关序号、设备参数、设备地块、设备公式、类别  
    #设备类型:1-数字，2-4-20ma电流，3-0-20ma电流，4-0-5v电压，5-0-10v,6-高低电平
    #设备参数:设备调整值
    #设备公式：WENDU、SHIDU、Q-WENDU、Q-SHIDU、CO2、BEAM、EC、PH、FS、FX、YL、QY、OPR等；
    #设备类别：10-控制器，11-传感器，20-水肥机控制器，21-水肥机传感器；        
        sSQL='SELECT a.ID,a.Code,a.PortNum,b.SignalType,c.SerialNumber,a.CorrectValue,a.Block_ID,d.Parameter_Value FROM yw_d_sensor_tbl a  '
        sSQL=sSQL+'LEFT JOIN yw_d_devicemodel_tbl  b ON  a.Model_ID = b.ID '
        sSQL=sSQL+'LEFT JOIN yw_d_commnication_tbl c ON a.Commucation_ID = c.ID '
        sSQL=sSQL+'LEFT JOIN ys_parameter_tbl d ON b.Formula= d.Parameter_Key '
        sSQL=sSQL+'LEFT JOIN yw_d_deviceclass_tbl e ON b.DeviceClass_ID= e.ID '
        sSQL=sSQL+'WHERE a.UsingState = "1" AND b.State = "1" AND c.UsingState = "1" AND d.Parameter_Class = "YW-JSGS"'
        try:
            recconn=mysqlpool.connection()
            reccursor=recconn.cursor()
            reccursor.execute(sSQL)
            while True:
                onedata = reccursor.fetchone()
                if not onedata:
                    break
                else:
                    devindex=-1
                    if len(sensor_list) > 0:
                        for i in sensor_list:
                            if i.devid == onedata[0] and i.devclass ==1:
                                devindex=0
                                break
                    if devindex==-1 or len(sensor_list) == 0:
                        onecomm=sensor_class(onedata[0],onedata[1],onedata[2],onedata[3],onedata[4],onedata[5],onedata[6],onedata[7],11)
                        sensor_list.append(onecomm)
            reccursor.close()                 
            recconn.close() 
        except Exception as err:
            showinfo=mymodule.getcurrtime()+' sensor device configure get failure：'+str(err)
            mymodule.create_log(showinfo)
            
    #控制器：ID,编号，地址，类型，所属网关序号、设备参数、设备地块、设备公式、类别      
    #设备类型:1-开关，2-脉冲，3-行程
    #设备参数:控制器-行程时长
    #设备公式：0-无意义；
    #设备类别：10-控制器，11-传感器，20-水肥机控制器，21-水肥机传感器；
        sSQL='SELECT a.ID,a.Code,a.PortNum,b.SignalType,c.SerialNumber,a.TravelTime,a.Block_ID FROM yw_d_controller_tbl a  '
        sSQL=sSQL+'LEFT JOIN yw_d_devicemodel_tbl  b ON  a.Model_ID = b.ID '
        sSQL=sSQL+'LEFT JOIN yw_d_commnication_tbl c ON a.Commucation_ID = c.ID '
        sSQL=sSQL+'LEFT JOIN ys_parameter_tbl d ON b.Formula= d.Parameter_Key '
        sSQL=sSQL+'LEFT JOIN yw_d_deviceclass_tbl e ON b.DeviceClass_ID= e.ID '
        sSQL=sSQL+'WHERE a.UsingState = "1" AND b.State = "1" AND c.UsingState = "1" '
        try:
            recconn=mysqlpool.connection()
            reccursor=recconn.cursor()
            reccursor.execute(sSQL)
            while True:
                onedata = reccursor.fetchone()
                if not onedata:
                    break
                else:
                    devindex=-1
                    if len(controller_list) > 0:
                        for i in controller_list:
                            if i.devid == onedata[0] and i.devclass ==1:
                                devindex=0
                                break
                    if devindex==-1 or len(controller_list) == 0:
                        onecomm=controller_class(onedata[0],onedata[1],onedata[2],onedata[3],onedata[4],onedata[5],onedata[6],'0',10)
                        controller_list.append(onecomm)
            reccursor.close()                 
            recconn.close() 
        except Exception as err:
            showinfo=mymodule.getcurrtime()+' controller device configure get failure：'+str(err)
            mymodule.create_log(showinfo)        

#处理连接请求
def handle_request():
    while True:
        clientaddress=[]
        try:
            sock,clientaddress=s.accept()
            #_dev_index = find_conn_addr(clientaddress)
            
            handlink_thread=myThread(target=recv_link,args=(sock,clientaddress))
            handlink_thread.start()
            showinfo=mymodule.getcurrtime() + ' server receive from('+ clientaddress[0]+':'+str(clientaddress[1])+') link request'
            mymodule.create_log(showinfo)
        except Exception as e:
            showinfo=mymodule.getcurrtime()+' server receive from('+ clientaddress[0]+':'+str(clientaddress[1])+') error: '+str(e)
            mymodule.create_log(showinfo) 

def recv_link(sock,addr):
    comm_sn=''
    illegaltimes = 0
    sendtimes = 0
    handle_data_scheduler=BackgroundScheduler()
    existcontroller = 0
    existsensor = 0
    while sendtimes <= 10:
        try:
            sleep(0.5)
            conn_index = -1
            comm_index = -1
            #已连接
            recvdata=sock.recv(buffsize)
            if recvdata:  #需要测试服务端和客户端断开连接后，返回的信息
                sdata = bytes(hexlify(recvdata)).decode('utf-8')
                showinfo = mymodule.getcurrtime() + ' receive from(' + addr[0]+':'+str(addr[1])+ ') data: ' + sdata
                mymodule.create_log(showinfo) 
                gw_sn=sdata[0:16]
                if gw_sn[0:12] == '150122220010': #KLC首次链接
                    gwdata=sdata[12:]
                    gwlen=int(len(gwdata)/2)
                    gw_sn=''
                    for i in range(0,gwlen):
                        gw_sn=gw_sn+chr(int(gwdata[i*2:i*2+2],16))
                elif gw_sn[0:8] == '15010000': #后续数据
                    gw_index=find_conn_addr(addr)
                    if gw_index >= 0: #已连接请求
                        gw_sn=conn_list[gw_index].serial_num
                    else:
                        gw_sn=''
                comm_sn=gw_sn
                conn_index=find_conn_sn(comm_sn)
                if conn_index >= 0:
                    if conn_list[conn_index].isonline==0: #超时
                        break
                    else:
                        comm_index=find_comm_sn(comm_sn)
                        commtype=comm_list[comm_index].commtype
                        if len(sdata)==16 or len(sdata)==44:
                            #超时设置，更新超时
                            sendtimes=0
                            conn_list[conn_index].lasttime=datetime.now()
                            conn_list[conn_index].overtime=0
                            conn_list[conn_index].isonline=1
                            showinfo = mymodule.getcurrtime() + ' receive client (' + gw_sn + ')'+ ' heart-beat'                   
                            mymodule.create_log(showinfo)
                        elif len(sdata)>16 and len(sdata)<=buffsize:
                            sdata1=sdata[16:].replace(' ','')
                            if commtype == 'PLC':#PLC
                                sendtimes=0
                                conn_list[conn_index].lasttime=datetime.now()
                                conn_list[conn_index].overtime=0
                                PLC_handlerecv(comm_index,sdata1)
                            elif commtype == 'FKC':#飞科
                                sendtimes=0
                                conn_list[conn_index].lasttime=datetime.now()
                                conn_list[conn_index].overtime=0
                                FKC_handlerecv(comm_index,sdata1)
                            elif commtype == 'XPC':#新普惠
                                sendtimes=0
                                conn_list[conn_index].lasttime=datetime.now()
                                conn_list[conn_index].overtime=0
                                XPC_handlerecv(comm_index,sdata1) 
                            elif commtype == 'YYC':#雨研
                                sendtimes=0
                                conn_list[conn_index].lasttime=datetime.now()
                                conn_list[conn_index].overtime=0
                                YYC_handlerecv(comm_index,sdata1)
                            elif commtype == 'SFJ-0804': #84A 8-灌区数，4-肥液数，A-有主管流量
                                conn_list[conn_index].lasttime=datetime.now()
                                conn_list[conn_index].overtime=0
                                SFJ_handlerecv(comm_index,sdata1)
                            elif commtype == 'SFJ-1200': #84A 12-灌区数，B-没有主管流量
                                sendtimes=0
                                conn_list[conn_index].lasttime=datetime.now()
                                conn_list[conn_index].overtime=0
                                SFJ_handlerecv(comm_index,sdata1) 
                            elif commtype == 'KLC': #昆仑海岸
                                sendtimes=0
                                conn_list[conn_index].lasttime=datetime.now()
                                conn_list[conn_index].overtime=0
                                sdata1=sdata[12:].replace(' ','')
                                KLC_handlerecv(comm_index,sdata1)
                            elif commtype == 'DYC': #昆仑海岸
                                sendtimes=0
                                conn_list[conn_index].lasttime=datetime.now()
                                conn_list[conn_index].overtime=0
                                DYC_handlerecv(comm_index,sdata1)
                            else: #未知公式
                                sendtimes=sendtimes+1
                                showinfo = mymodule.getcurrtime() + ' get comm_device (' + comm_sn + ')'+ ' unknow type'
                                mymodule.create_log(showinfo)  
                        #数据长度异常
                        else:
                            sendtimes=sendtimes + 1
                            showinfo = mymodule.getcurrtime() + ' comm_device (' + comm_sn + ')'+ ' data length error:'+sdata
                            mymodule.create_log(showinfo)
                else :#未连接请求
                    bResult=False
                    comm_index = find_comm_sn(comm_sn)
                    if comm_index >=0:
                    #创建新连接
                        if sdata[0:12]=='150122220010': #KLC首次链接
                            sendstr='15012222000180'
                            sock.sendall(bytes().fromhex(sendstr)) 
                        bResult=create_sock(sock,addr,comm_sn) 
                    else:
                        sendtimes= sendtimes + 1
                        showinfo =mymodule.getcurrtime() + ' receive client (' + comm_sn + ')' +' error connect data: ' + sdata +' ' + str(sendtimes) +' times.'                 
                        mymodule.create_log(showinfo)
                    commtype = comm_list[comm_index].commtype
                    if comm_list[comm_index].sensormax != None and  comm_list[comm_index].sensormax != 'NULL':
                        existsensor=1
                    if comm_list[comm_index].controlmax != None and  comm_list[comm_index].controlmax != 'NULL':    
                        existcontroller=1
                    if bResult:
                        if commtype=="KLC" or commtype=="FKC" or commtype=="XPC" or commtype=="YYC" :
                            if existsensor:
                                trigger_data = IntervalTrigger(seconds=default_collecttime)
                                handle_data_scheduler.add_job(collect_data,trigger_data,coalesce=True,max_instances=2,id='sensor'+comm_sn,args=(comm_index,))
                        if commtype=="KLC" or commtype=="FKC" :
                            if existcontroller:
                                trigger_state = IntervalTrigger(seconds=default_statetime)    
                                handle_data_scheduler.add_job(get_state,trigger_state,coalesce=True,max_instances=5,id='cont'+comm_sn,args=(comm_index,))
                        handle_data_scheduler.start()
    #异常数据,是否需要退出
        except Exception as e:
            showinfo=mymodule.getcurrtime() + ' receive data exception:('+addr[0]+':'+str(addr[1])+')'+str(e)
            mymodule.create_log(showinfo)
            sendtimes=sendtimes+1
    if existsensor:
        handle_data_scheduler.remove_job('sensor'+comm_sn)
    elif existcontroller:
        handle_data_scheduler.remove_job('cont'+comm_sn)
    conn_index=find_conn_sn(comm_sn)
    if conn_index >=0:
        conn_list[conn_index].isonline = 0
    sleep(3)
    close_sock(sock,addr,comm_sn)



def create_sock(sock,addr,comm_sn):
    bResult=0
    try:
        
        comm__index=find_comm_sn(comm_sn)
        conn_index = find_conn_sn(comm_sn)
        if conn_index >=0:
            close_sock(conn_list[conn_index].sock,conn_list[conn_index].addr,comm_sn)
            sleep(1)
        else:
            #已连接设备:连接sock，地址，设备序列号，最近连接时间，超时时间，任务状态，是否在线  
            conn_list.append(conn_class(sock,addr,comm_sn,datetime.now(),0,0,1))
            gui.listBox.insert(0,comm_sn)
            gui.infoList.config(state=tk.NORMAL)
            showinfo = mymodule.getcurrtime() + ' accept client[' + comm_sn + '] ('+ addr[0] + ':' + str(addr[1])+') Socket connect request' 
            mymodule.create_log(showinfo)
            gui.infoList.insert(1.0,showinfo)
            gui.infoList.config(state=tk.DISABLED)
            bResult=1
    except Exception as e:
        showinfo=mymodule.getcurrtime() + ' create socket connect failed:'+str(e)
        mymodule.create_log(showinfo)
    return bResult
        
def close_sock(sock,addr,comm_sn):
    try:
        comm_index = find_comm_sn(comm_sn)
        if comm_index < 0: #未连接或非法
            sock.shutdown(0)
            sleep(5)
            sock.close()
            showinfo = mymodule.getcurrtime() + ' close unlinked client(' + addr[0] +':' + str(addr[1])+ ') succeed'                   
            mymodule.create_log(showinfo)
        else : #已连接
            conn_index = find_conn_sn(comm_sn)
            if conn_index >= 0:
                conn_list[conn_index].isonline=0
                j=-1
                for i in range(0,gui.listBox.size()):
                    if gui.listBox.get(i)== comm_sn:
                        j=i
                        break
                if j != -1:
                    gui.listBox.delete(j) 
                
                mysock=conn_list[conn_index].sock#多次连接
                conn_list[conn_index].isonline = 0 
                conn_list.pop(conn_index)
                sleep(5) #重复发送？？？？？
                currentconn=''
                for i in conn_list:
                    currentconn=currentconn+i.serial_num+';'
                currentconn=mymodule.getcurrtime()+' '+currentconn+'\n'+'it is will deleted:'+comm_sn
                mymodule.create_log(currentconn)
                sock.shutdown(0) #0-关闭读通道，1-关闭写通道，2-关闭多写通道
                sleep(5)
                sock.close()
                showinfo = mymodule.getcurrtime() + ' close client(' + comm_sn + ')'+ ' connect shutdown succeed'                   
                mymodule.create_log(showinfo)
            else:
                pass
    except Exception as e:
        showinfo=mymodule.getcurrtime() + ' close client(' + addr[0]+ str(addr[1]) + ')'+ ' connect shutdown failed:'  +str(e)
        mymodule.create_log(showinfo)
          

def IOT_send_thread():
    #下发任务
    comm_sn=''
    while True:
        try:
            #指令id,设备id，指令名称，指令参数(延迟时间)，预计时间，下发时间，网关序号,设备编号    
            sSQL='SELECT a.id,a.Device_ID,a.ActOrder,a.actparam,a.scheduledtime,a.createtime,c.serialNumber,b.code '
            sSQL=sSQL+'FROM yw_c_control_log_tbl a '
            sSQL=sSQL+'LEFT JOIN yw_d_controller_tbl b ON a.Device_ID = b.ID '
            sSQL=sSQL+'LEFT JOIN yw_d_commnication_tbl c ON b.Commucation_ID = c.ID '
            sSQL=sSQL+'where (a.ActOrder = "AC-OPEN" OR a.ActOrder = "AC-CLOSE" OR a.ActOrder = "AC-STOP") AND a.`ExecuteResult` < 4 '
            sSQL=sSQL+'AND (ISNULL(a.ScheduledTime) OR (NOW() > a.ScheduledTime AND DATE_ADD(a.ScheduledTime,INTERVAL 10 MINUTE)>NOW())) AND (c.SerialNumber IS NOT NULL) '
            sSQL=sSQL+'UNION ALL '
            sSQL=sSQL+'SELECT a.id,a.Device_ID,a.ActOrder,a.actparam,a.scheduledtime,a.createtime,b.serialNumber,b.code  '
            sSQL=sSQL+'FROM yw_c_control_log_tbl a  '
            sSQL=sSQL+'LEFT JOIN yw_d_commnication_tbl b ON a.Device_ID = b.ID  '
            sSQL=sSQL+'where (a.ActOrder = "INTERVAL" OR a.ActOrder = "GETDATA") AND a.`ExecuteResult` < 4  '
            sSQL=sSQL+'AND (ISNULL(a.ScheduledTime) OR (NOW() > a.ScheduledTime AND DATE_ADD(a.ScheduledTime,INTERVAL 10 MINUTE)>NOW())) AND (b.SerialNumber IS NOT NULL) '
            sendconn=mysqlpool.connection()
            sendcursor=sendconn.cursor()
            sendcursor.execute(sSQL,args=())
            while True:
                onedata=sendcursor.fetchone()
                sendtimes = 0
                if not onedata:
                    break
                else:
                    dev_id=onedata[1]
                    comm_sn=onedata[6]
                    dev_index=find_devid(dev_id,10)
                    comm_index=find_comm_sn(comm_sn)
                    conn_index=find_conn_sn(comm_sn)
                    try:
                    #指令id,设备id，指令名称，指令参数，预计时间，下发时间，网关序号
                        order_delta=0
                        if onedata[4]:
                            order_delta=(datetime.now()-onedata[4]).seconds
                        #预计时间非空且超过10分钟,关闭任务
                        if  (onedata[4] and (order_delta >= 600)): 
                            reslSQL='update yw_c_control_log_tbl set ExecuteTime=%s,ExecuteResult=%s where id = "' + str(onedata[0]) + '"'
                            reslconn=mysqlpool.connection()
                            reslcursor=reslconn.cursor()
                            reslcursor.execute(reslSQL,args=(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'4'))
                            reslconn.commit() 
                            reslcursor.close()
                            reslconn.close()
                        #预计时间非空,或10分钟内，则执行任务
                        elif (not onedata[4]) or (onedata[4] and (order_delta < 600)):
                            if comm_index >= 0 and conn_index >= 0 and dev_index >=0:
                                commtype=comm_list[comm_index].commtype
                                if commtype == 'FKC':
                                    send_thread=myThread(target=FKC_sendorder,args=(dev_index,comm_index,conn_index,onedata))
                                    send_thread.start()
                                elif commtype == 'XPC':
                                    send_thread=myThread(target=XPC_sendorder,args=(dev_index,comm_index,conn_index,onedata))
                                    send_thread.start()
                                elif commtype == 'PLC':
                                    send_thread=myThread(target=PLC_sendorder,args=(dev_index,comm_index,conn_index,onedata))
                                    send_thread.start()
                                elif commtype == 'KLC':
                                    send_thread=myThread(target=KLC_sendorder,args=(dev_index,comm_index,conn_index,onedata))
                                    send_thread.start()    
                                elif commtype == 'SFJ-0804' or commtype == 'SFJ-1200':
                                    send_thread=myThread(target=SFJ_sendorder,args=(dev_index,comm_index,conn_index,onedata))
                                    send_thread.start()    
                                elif commtype == 'YYC':
                                    send_thread=myThread(target=YYC_sendorder,args=(dev_index,comm_index,conn_index,onedata))
                                    send_thread.start()
                                elif commtype == 'DYC':
                                    send_thread=myThread(target=DYC_sendorder,args=(dev_index,comm_index,conn_index,onedata))
                                    send_thread.start()
                                else:#未识别的网关类型
                                    reslSQL='update yw_c_control_log_tbl set ExecuteTime=%s,ExecuteResult=%s where id = "' + str(onedata[0]) + '"'
                                    reslconn=mysqlpool.connection()
                                    reslcursor=reslconn.cursor()
                                    reslcursor.execute(reslSQL,args=(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'50')) 
                                    reslconn.commit()
                                    reslcursor.close()
                                    reslconn.close()
                                    showinfo=mymodule.getcurrtime()+' '+'('+comm_sn  + ') order_id ('+ str(onedata[0]) +') illegal finished for unkown commcategory' 
                                    mymodule.create_log(showinfo)
                            else:#网关未连接或未知的设备或位置的网关
                                reslSQL='update yw_c_control_log_tbl set ExecuteTime=%s,ExecuteResult=%s where id = "' + str(onedata[0]) + '"'
                                reslconn=mysqlpool.connection()
                                reslcursor=reslconn.cursor()
                                reslcursor.execute(reslSQL,args=(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'60')) 
                                reslconn.commit()
                                reslcursor.close()
                                reslconn.close()
                                showinfo=mymodule.getcurrtime()+' '+'('+comm_sn  + ') order_id ('+ str(onedata[0]) +') illegal finished for unconnect comm or unknow dev or unknow connected' 
                                mymodule.create_log(showinfo)
                    except Exception as e:
                        showinfo=mymodule.getcurrtime()+ ' send_order thread has unnormal info: ' +str(e) + ' times:'+str(sendtimes)
                        mymodule.create_log(showinfo)
            sleep(default_ordertime)
        except Exception as e:
            showinfo=mymodule.getcurrtime()+ ' send_order thread has exited: ' +str(e)
            mymodule.create_log(showinfo) 

def SFJ_send_thread():
    #下发任务
    comm_sn=''
    while True:
        sleep(default_statetime)
        try:
            #指令任务id,设备id，指令名称，设备地址，预计时间，下发时间，网关序号,设备地址  
            sSQL='SELECT a.ID, a.Device_ID,ActOrder,b.Device_Address,a.ScheduledTime,a.CreateTime,c.PLC_Number,c.PLC_Address FROM sfyth_control_log a '
            sSQL= sSQL+'LEFT JOIN sfyth_device b ON a.Device_ID = b.ID  LEFT JOIN sfyth_plc c ON a.PLC_Number = c.PLC_Number '
            sSQL= sSQL+'WHERE a.ActState = 0 AND (ISNULL(a.ScheduledTime) OR (NOW() > a.ScheduledTime AND DATE_ADD(a.ScheduledTime,INTERVAL 10 MINUTE)>NOW())) '
            sendconn=mysqlpool.connection()
            sendcursor=sendconn.cursor()
            sendcursor.execute(sSQL)
            while True:
                onedata=sendcursor.fetchone()
                if not onedata :
                    break
                else:
                    comm_sn=onedata[6]
                    order_delta=0
                    if onedata[4]:
                        order_delta=(datetime.now()-onedata[4]).seconds
                    #预计时间非空且超过10分钟,关闭任务
                    if  (onedata[4] and (order_delta >= 600)): 
                        reslSQL='update sfyth_control_log set ActState="1" , ExecuteTime= now() where id = %s' 
                        reslconn=mysqlpool.connection()
                        reslcursor=reslconn.cursor()
                        reslcursor.execute(reslSQL,args=(str(onedata[0]),))
                        reslconn.commit() 
                        reslcursor.close()
                        reslconn.close()
                    #预计时间非空,或10分钟内，则执行任务
                    elif (not onedata[4]) or (onedata[4] and (order_delta < 600)):
                        reslSQL='update sfyth_control_log set ActState="1" , ExecuteTime= now() where id = %s' 
                        reslconn=mysqlpool.connection()
                        reslcursor=reslconn.cursor()
                        reslcursor.execute(reslSQL,args=(str(onedata[0]),))
                        reslconn.commit() 
                        reslcursor.close()
                        reslconn.close()
                        SFJ_sendorder(0,onedata) 
                
        except Exception as e:
            showinfo=mymodule.getcurrtime() + ' send SFJ(' + comm_sn + ')'+  ' order failed : ' +str(e)
            mymodule.create_log(showinfo)
        
        try:
            #开始时间，时长，水量，肥量，区域，肥液通道,日期、类型、PLC地址、指令id、PLC序列号
            sSQL='SELECT T_Start,T_Interval,T_Gquantity,T_Squantity,T_Area,T_Fertilize,a.T_Date,T_Type,'
            sSQL= sSQL+ 'b.PLC_Address,a.T_ID,b.PLC_Number FROM  sfyth_task a '
            sSQL= sSQL+ 'LEFT JOIN sfyth_plc b ON b.PLC_Number = a.PLC_Number '
            sSQL= sSQL+ 'WHERE a.T_State=0 AND (ISNULL(a.T_Start) OR (NOW() > a.T_Start AND  ' 
            sSQL= sSQL+ 'DATE_ADD(DATE_FORMAT(CONCAT(a.T_Date," ",a.T_Start),"%Y-%m-%d %H:%i:%s"),INTERVAL 10 MINUTE)>NOW())) '
            sSQL= sSQL+ 'ORDER BY a.T_ID ASC'
            sendconn=mysqlpool.connection()
            sendcursor=sendconn.cursor()
            sendcursor.execute(sSQL)
            while True:
                onedata=sendcursor.fetchone()
                if not onedata :
                    break
                else:
                    comm_sn=onedata[10]
                    order_delta=0
                    if onedata[0]:
                        schedulttime=(onedata[6].strftime("%Y-%m-%d")+" 00:00:00")
                        schedulttime=datetime.strptime(schedulttime,"%Y-%m-%d %H:%M:%S")
                        schedulttime=schedulttime+timedelta(seconds=onedata[0].seconds)
                        order_delta=(datetime.now()-schedulttime).seconds
                    #预计时间非空且超过10分钟,关闭任务
                    if  (onedata[0] and (order_delta >= 600)): 
                        reslSQL='UPDATE sfyth_task SET T_State="1" where T_ID=%s' 
                        reslconn=mysqlpool.connection()
                        reslcursor=reslconn.cursor()
                        reslcursor.execute(reslSQL,args=(str(onedata[9]),))
                        reslconn.commit() 
                        reslcursor.close()
                        reslconn.close()
                    #预计时间非空,或10分钟内，则执行任务
                    elif (not onedata[4]) or (onedata[4] and (order_delta < 600)):
                        reslSQL='UPDATE sfyth_task SET T_State="2" where T_ID=%s' 
                        reslconn=mysqlpool.connection()
                        reslcursor=reslconn.cursor()
                        reslcursor.execute(reslSQL,args=(str(onedata[9]),))
                        reslconn.commit() 
                        reslcursor.close()
                        reslconn.close()
                        SFJ_sendorder(1,onedata) 
        except Exception as e:
            showinfo=mymodule.getcurrtime() + ' send task order(' + comm_sn + ')'+ 'failed : '   +str(e)
            mymodule.create_log(showinfo)
            
def DYJ_send_thread():
    #下发任务
    comm_sn=''
    while True:
        try:
            #打药机任务
            #任务ID，发送日期，设置时长，设置浓度，预计时间，通讯设备序列号，通讯地址
            sSQL='SELECT a.ID,a.SendDate,SetInterval,SetDensity,ScheduledTime,b.SerialNumber,b.CodeAddress FROM  yw_g_taskorder_tbl a '
            sSQL=sSQL+'LEFT JOIN yw_d_commnication_tbl b ON a.`Commucation_ID` = b.ID '
            sSQL=sSQL+'WHERE (ExecuteResult=0 OR ISNULL(ExecuteResult)) AND (ISNULL(ScheduledTime) OR (NOW() > sendDate AND DATE_ADD(sendDate,INTERVAL 10 MINUTE)>NOW())) '        
            sSQL=sSQL+'ORDER BY a.ID ASC '
            sendconn=mysqlpool.connection()
            sendcursor=sendconn.cursor()
            sendcursor.execute(sSQL,args=())
            while True:
                onedata=sendcursor.fetchone()
                sendtimes = 0
                if not onedata:
                    break
                else:
                    try:
                       send_thread=myThread(target=DYC_sendorder,args=(-10,-10,-10,onedata))
                       send_thread.start()
                    except Exception as e:
                        showinfo=mymodule.getcurrtime()+ 'DYC send_order thread has exited: ' +str(e)
                        mymodule.create_log(showinfo)            
            sleep(default_ordertime)
        except Exception as e:
            showinfo=mymodule.getcurrtime()+ ' send_order thread has exited: ' +str(e)
            mymodule.create_log(showinfo)            

           
#本模块运行，导入到其他模块不执行
if __name__ == '__main__':

    t1 = myThread(target=createGUI, args=(), name='gui')
    t1.setDaemon(True) 
    t1.start() 
    connect_sockserver()
    get_devinfo() 
    t2 = myThread(target=handle_request, args=(), name='handle_request') 
    t2.setDaemon(True)
    t2.start() 
    while True:
        print('system runing')
        sleep(60) 

   