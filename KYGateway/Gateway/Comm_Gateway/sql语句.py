#查询物联网通讯设备
'''
SELECT n.ID,n.Code,n.SerialNumber,n.CodeAddress,n.Company_ID,n.ClassName,n.`Interval`, COUNT(controlport) AS controlnum,COUNT(sensorport) AS sensorsum FROM
(SELECT e.*,NULL controlport,f.PortNum AS sensorport  FROM 
(SELECT a.ID,a.Code,a.SerialNumber,a.CodeAddress,a.Company_ID,c.ClassName,`Interval` FROM yw_d_commnication_tbl a 
LEFT JOIN yw_d_devicemodel_tbl b ON a.Model_ID = b.ID 
LEFT JOIN yw_d_deviceclass_tbl c ON b.DeviceClass_ID= c.ID 
LEFT JOIN ys_parameter_tbl d ON b.Formula = d.Parameter_Key 
WHERE a.UsingState="1" AND b.State="1" AND d.Parameter_Class = "YW-JSGS") e
LEFT JOIN yw_d_sensor_tbl f ON f.Commucation_ID = e.id AND f.UsingState = "1"
UNION ALL
SELECT l.*,m.PortNum,NULL sensorport FROM 
(SELECT g.ID,g.Code,g.SerialNumber,g.CodeAddress,g.Company_ID,i.ClassName,`Interval` FROM yw_d_commnication_tbl g 
LEFT JOIN yw_d_devicemodel_tbl h ON g.Model_ID = h.ID 
LEFT JOIN yw_d_deviceclass_tbl i ON h.DeviceClass_ID= i.ID 
LEFT JOIN ys_parameter_tbl k ON h.Formula = k.Parameter_Key 
WHERE g.UsingState="1" AND h.State="1" AND k.Parameter_Class = "YW-JSGS") l
LEFT JOIN yw_d_controller_tbl m ON m.Commucation_ID = l.id WHERE m.UsingState = "1") n
GROUP BY n.id;
'''

#水肥机通讯设备
'''
SELECT e.ID,e.PLC_Name,e.PLC_Number,e.PLC_Address,e.Company_ID,e.PLC_GWType,e.PassNumber,MAX(controladdr),MAX(sensoraddr) FROM
(SELECT a.ID,PLC_Name,a.PLC_Number,PLC_Address,a.Company_ID,PLC_GWType,PassNumber,CAST(b.`Device_Address` AS UNSIGNED) AS controladdr, NULL sensoraddr FROM sfyth_plc a
LEFT JOIN `sfyth_device` b ON b.`PLC_Number` = a.`PLC_Number`
WHERE a.Is_Delete = 0 AND b.`Is_Delete` = 0 AND CAST(b.`Device_Type` AS UNSIGNED) < 7
UNION ALL
SELECT c.ID,PLC_Name,c.PLC_Number,PLC_Address,c.Company_ID,PLC_GWType,PassNumber,NULL controladdr, CAST(d.`Device_Address` AS UNSIGNED) AS sensoraddr FROM sfyth_plc c
LEFT JOIN `sfyth_device` d ON d.`PLC_Number` = c.`PLC_Number`
WHERE c.Is_Delete = 0 AND d.`Is_Delete` = 0 AND CAST(d.`Device_Type` AS UNSIGNED) > 7) e GROUP BY e.plc_number
'''

#控制器
'''
SELECT a.ID,a.Code,a.PortNum,b.SignalType,c.SerialNumber,a.TravelTime,a.Block_ID FROM yw_d_controller_tbl a  
LEFT JOIN yw_d_devicemodel_tbl  b ON  a.Model_ID = b.ID 
LEFT JOIN yw_d_commnication_tbl c ON a.Commucation_ID = c.ID 
LEFT JOIN ys_parameter_tbl d ON b.Formula= d.Parameter_Key 
LEFT JOIN yw_d_deviceclass_tbl e ON b.DeviceClass_ID= e.ID 
WHERE a.UsingState = "1" AND b.State = "1" AND c.UsingState = "1"
'''

#传感器
'''
SELECT a.ID,a.Code,a.PortNum,b.SignalType,c.SerialNumber,a.CorrectValue,a.Block_ID,d.Parameter_Value FROM yw_d_sensor_tbl a  
LEFT JOIN yw_d_devicemodel_tbl  b ON  a.Model_ID = b.ID 
LEFT JOIN yw_d_commnication_tbl c ON a.Commucation_ID = c.ID 
LEFT JOIN ys_parameter_tbl d ON b.Formula= d.Parameter_Key 
LEFT JOIN yw_d_deviceclass_tbl e ON b.DeviceClass_ID= e.ID 
WHERE a.UsingState = "1" AND b.State = "1" AND c.UsingState = "1" AND d.Parameter_Class = "YW-JSGS"
'''

#IOT查询任务
'''
SELECT a.id,a.Device_ID,a.ActOrder,a.actparam,a.scheduledtime,a.createtime,c.serialNumber,b.code FROM yw_c_control_log_tbl a 
LEFT JOIN yw_d_controller_tbl b ON a.Device_ID = b.ID 
LEFT JOIN yw_d_commnication_tbl c ON b.Commucation_ID = c.ID 
WHERE (a.ActOrder = "AC-OPEN" OR a.ActOrder = "AC-CLOSE" OR a.ActOrder = "AC-STOP") AND a.`ExecuteResult` < 4 
AND (ISNULL(a.ScheduledTime) OR (NOW() > a.ScheduledTime AND DATE_ADD(a.ScheduledTime,INTERVAL 10 MINUTE))) AND (c.SerialNumber IS NOT NULL) 
UNION ALL 
SELECT a.id,a.Device_ID,a.ActOrder,a.actparam,a.scheduledtime,a.createtime,b.serialNumber,b.code FROM yw_c_control_log_tbl a  
LEFT JOIN yw_d_commnication_tbl b ON a.Device_ID = b.ID  
WHERE (a.ActOrder = "INTERVAL" OR a.ActOrder = "GETDATA") AND a.`ExecuteResult` < 4  
AND (ISNULL(a.ScheduledTime) OR (NOW() > a.ScheduledTime AND DATE_ADD(a.ScheduledTime,INTERVAL 10 MINUTE))) AND (b.SerialNumber IS NOT NULL) 
'''
#水肥机查询任务
'''
SELECT a.ID, a.Device_ID,ActOrder,b.Device_Address,a.ScheduledTime,a.CreateTime,c.PLC_Number,c.PLC_Address FROM sfyth_control_log a 
LEFT JOIN sfyth_device b ON a.Device_ID = b.ID  LEFT JOIN sfyth_plc c ON a.PLC_Number = c.PLC_Number 
WHERE a.ActState = 0 AND (ISNULL(a.ScheduledTime) OR (NOW() > a.ScheduledTime AND DATE_ADD(a.ScheduledTime,INTERVAL 10 MINUTE) >NOW())) 
'''


