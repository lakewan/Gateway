查询物联网通讯设备
'''
SELECT g.*,COUNT(sensorport) AS sensorsum, COUNT(controlport) AS controlnum FROM
(SELECT e.*,f.PortNum AS sensorport,NULL controlport  FROM 
(SELECT a.ID,a.Code,a.SerialNumber,a.CodeAddress,a.Company_ID,c.ClassName,`Interval`,a.numofpass FROM yw_d_commnication_tbl a 
LEFT JOIN yw_d_devicemodel_tbl b ON a.Model_ID = b.ID 
LEFT JOIN yw_d_deviceclass_tbl c ON b.DeviceClass_ID= c.ID 
LEFT JOIN ys_parameter_tbl d ON b.Formula = d.Parameter_Key 
WHERE a.UsingState="1" AND b.State="1" AND d.Parameter_Class = "YW-JSGS") e
LEFT JOIN yw_d_sensor_tbl f ON f.Commucation_ID = e.id 
UNION ALL
SELECT e.*,NULL sensorport,f.PortNum FROM 
(SELECT a.ID,a.Code,a.SerialNumber,a.CodeAddress,a.Company_ID,c.ClassName,`Interval`,a.numofpass FROM yw_d_commnication_tbl a 
LEFT JOIN yw_d_devicemodel_tbl b ON a.Model_ID = b.ID 
LEFT JOIN yw_d_deviceclass_tbl c ON b.DeviceClass_ID= c.ID 
LEFT JOIN ys_parameter_tbl d ON b.Formula = d.Parameter_Key 
WHERE a.UsingState="1" AND b.State="1" AND d.Parameter_Class = "YW-JSGS") e
LEFT JOIN yw_d_controller_tbl f ON f.Commucation_ID = e.id WHERE f.UsingState = "1") g
GROUP BY id;
'''

