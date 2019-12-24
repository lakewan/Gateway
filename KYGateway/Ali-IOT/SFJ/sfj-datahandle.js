
//1、通讯设备的序列号无意义；2、控制设备ID无意义，灌区和施肥通道换成通道号而不是设备ID；

var setString=new String('{"method":"thing.service.property.set","version":"1.0","id": "2241348","params":{"comm_serial": "0000201909040003","comm_addr":2,"task_area":"1,6,7,13,15","task_state":0,"schedule_irrigate_num":0,"schedule_irrigate_interval":5,"schedule_fertilize_num":0,"task_type": 0,"send_order":10}}');

//var setString=new String('{"method":"thing.service.property.set","version":"1.0","id": "2241348","params":{"comm_serial": "0000201909040003","comm_addr":2,"dev_addr":1,"send_order":1}}');

// var setString = Buffer("00002233440000201909040003020f0c500010028d12d3dd","hex");
//1\3\4\8\10\13打开
//var rawdata_report_prop = Buffer("00002233440000201909040003021000000020400000043200020000000500000000000000000000000000000000000000000000000007e3000a001d0015001e000b07e3000a001d0015001e00100000000500004b54","hex")
//2019-10-29 21:30:11 21:30:16 灌溉 2/5/6/11灌区，计划时长5，实际时长5，状态：已完成
function Test()
{
    //rawDataToProtocol(rawdata_report_prop);
    protocolToRawData(JSON.parse(setString));
}
var COMMAND_REPORT = 0x00;
var COMMAND_SET = 0x01; 
var COMMAND_REPORT_REPLY = 0x02; 
var COMMAND_SET_REPLY = 0x03;
var COMMAD_UNKOWN = 0xff;   
var ALINK_PROP_REPORT_METHOD = 'thing.event.property.post'; 
var ALINK_PROP_SET_METHOD = 'thing.service.property.set';
var ALINK_PROP_SET_REPLY_METHOD = 'thing.service.property.set'; 
function rawDataToProtocol(bytes) {
    var uint8Array = new Uint8Array(bytes.length);
    for (var i = 0; i < bytes.length; i++) {
        uint8Array[i] = bytes[i] & 0xff;
    }
    //视图是一个可以从 ArrayBuffer 对象中读写多种数值类型的底层接口，使用它时，不用考虑不同平台的字节序问题。
    var dataView = new DataView(uint8Array.buffer, 0);
    var jsonMap = {};
    var fHead = uint8Array[0]; // command
    var params = {};
    var tempa ="";
    var devstate=[];
    if (fHead == COMMAND_REPORT) {
    	if (uint8Array[14] == 15){ //获取状态

    		tempa = ("00000000"+dataView.getUint8(21).toString(2)).slice(-8)+("00000000"+dataView.getUint8(20).toString(2)).slice(-8);
    		devstate= tempa.split('').reverse().join('');
	        jsonMap.method = ALINK_PROP_REPORT_METHOD; //ALink JSON格式 - 属性上报topic
	        jsonMap.version = "1.0"; //ALink JSON格式 - 协议版本号固定字段
	        jsonMap.id = "" + dataView.getInt32(1); //ALink JSON格式 - 标示该次请求id值
	        
	        params.area1 = parseInt(devstate.slice(0,1)); //对应产品属性中area1
	        params.area2 = parseInt(devstate.slice(1,2)); //对应产品属性中area2       
	        params.area3 = parseInt(devstate.slice(2,3)); //对应产品属性中area3
	        params.area4 = parseInt(devstate.slice(3,4)); //对应产品属性中area4
	        params.area5 = parseInt(devstate.slice(4,5)); //对应产品属性中area5
	        params.area6 = parseInt(devstate.slice(5,6)); //对应产品属性中area6
	        params.area7 = parseInt(devstate.slice(6,7)); //对应产品属性中area7
	        params.area8 = parseInt(devstate.slice(7,8)); //对应产品属性中area8
	        params.area9 = parseInt(devstate.slice(8,9)); //对应产品属性中area9
	        params.area10 = parseInt(devstate.slice(9,10)); //对应产品属性中area10       
	        params.area11 = parseInt(devstate.slice(10,11)); //对应产品属性中area11
	        params.area12 = parseInt(devstate.slice(11,12)); //对应产品属性中area12
	        params.fertbump = parseInt(devstate.slice(12,13)); //对应产品属性中fertbump
	        params.fertvalve = parseInt(devstate.slice(13,14)); //对应产品属性中fertvalve
	        params.cleanvalve = parseInt(devstate.slice(14,15)); //对应产品属性中cleanvalve
	        params.alarmer = parseInt(devstate.slice(15,16)); //对应产品属性中alarmer
	        jsonMap.params = params; //ALink JSON格式 - params标准字段
	        }
	    else if(uint8Array[14] == 16){ //任务上传

    		var remote_attr = dataView.getInt16[20];
    		tempa = ("00000000"+dataView.getUint16(22).toString(2)).slice(-8)+("00000000"+dataView.getUint16(23).toString(2)).slice(-8);
    		devstate= tempa.split('').reverse().join('');
	        jsonMap.method = ALINK_PROP_REPORT_METHOD; //ALink JSON格式 - 属性上报topic
	        jsonMap.version = "1.1"; //ALink JSON格式 - 协议版本号固定字段
	        jsonMap.id = "" + dataView.getInt32(1); //ALink JSON格式 - 标示该次请求id值
	        params = {};
	        i = 0;
	        var areasel="";
			for (i=0;i<12;i++) {
				if (devstate.slice(i,i+1) == 1){
					areasel=areasel+String(i+1)+",";
					}
			}
			if (areasel.slice(-1) == ","){
				areasel=areasel.slice(0,-1);
				}
			else{
				areasel=areasel;
				}
		    params.task_area = areasel; //对应产品属性中area12
			params.task_state = dataView.getUint16(24);
	        params.schedule_irrigate_num = dataView.getUint16(26)/10;
	        params.schedule_irrigate_interval = dataView.getUint16(28);
	        params.schedule_fertilize_num = dataView.getUint16(30)/10;
	        
	        params.task_type = dataView.getUint16(44);   	//0-灌溉，1-施肥
	        params.irrigate_mode = dataView.getUint16(46); //0-定量，1-定时
	        params.preirrigate = dataView.getUint16(48);	//0-无预灌溉，1-有预灌溉
	        params.clean_attr = dataView.getUint16(50);	//0-无清洁，1-有清洁

	        params.start_year = dataView.getUint16(54); 
	        params.start_month = dataView.getUint16(56); 
	        params.start_day = dataView.getUint16(58);
	        params.start_hour = dataView.getUint16(60) ;
	        params.start_minute =dataView.getUint16(62) ;
	        params.start_second = dataView.getUint16(64);
			params.end_year = dataView.getUint16(66) ;
	        params.end_month = dataView.getUint16(68) ;
	        params.end_day =dataView.getUint16(70);
	        params.end_hour = dataView.getUint16(72) ;
	        params.end_minute = dataView.getUint16(74) ;
	        params.end_second = dataView.getUint16(76);	        
	        params.real_irrigate_num = dataView.getUint16(78)/10;
	        params.real_irrigate_interval = dataView.getUint16(80);
	        params.real_fertilize_num = dataView.getUint16(82)/10;
	        jsonMap.params = params; //ALink JSON格式 - params标准字段
	        }
    } 
	else if(fHead == COMMAND_SET_REPLY) {
        jsonMap.version = '1.0'; //ALink JSON格式 - 协议版本号固定字段
        jsonMap.id = '' + dataView.getInt32(1); //ALink JSON格式 - 标示该次请求id值
        jsonMap.code = ''+ dataView.getUint8(5);
        jsonMap.data = {};
    }
    //console.log(jsonMap); 
    return jsonMap;
}
//以下是部分辅助函数 
function buffer_uint8(value) 
{ 
  var uint8Array = new Uint8Array(1); 
  var dv = new DataView(uint8Array.buffer, 0); 
  dv.setUint8(0, value); 
  return [].slice.call(uint8Array); 
} 
function buffer_uint16(value) 
{ 
  var uint8Array = new Uint8Array(2); 
  var dv = new DataView(uint8Array.buffer, 0); 
  dv.setUint16(0, value); 
  return [].slice.call(uint8Array); 
} 
function buffer_uint32(value) 
{ 
  var uint8Array = new Uint8Array(4); 
  var dv = new DataView(uint8Array.buffer, 0); 
  dv.setUint32(0, value); 
  return [].slice.call(uint8Array); 
} 
function buffer_float32(value) 
{ 
  var uint8Array = new Uint8Array(4); 
  var dv = new DataView(uint8Array.buffer, 0); 
  dv.setFloat32(0, value); 
  return [].slice.call(uint8Array); 
} 

var CRC = {};

function CRC16(data) {
    var len = data.length;
    if (len > 0) {
        var crc = 0xFFFF;

        for (var i = 0; i < len; i++) {
            crc = (crc ^ (data[i]));
            for (var j = 0; j < 8; j++) {
                crc = (crc & 1) !== 0 ? ((crc >> 1) ^ 0xA001) : (crc >> 1);
            }
        }
        var hi = ((crc & 0xFF00) >> 8);  //高位置
        var lo = (crc & 0x00FF);         //低位置

        return [hi, lo];
    }
    return [0, 0];
}

function isArray(arr) {
    return Object.prototype.toString.call(arr) === '[object Array]';
}

function ToCRC16 (str, isReverse) {
    return toString(CRC16(isArray(str) ? str : strToByte(str)), isReverse);
}

function ToModbusCRC16(str, isReverse) {
    return toString(CRC16(isArray(str) ? str : strToHex(str)), isReverse);
}

function strToByte(str) {
    var tmp = str.split(''), arr = [];
    for (var i = 0, c = tmp.length; i < c; i++) {
        var j = encodeURI(tmp[i]);
        if (j.length == 1) {
            arr.push(j.charCodeAt());
        } else {
            var b = j.split('%');
            for (var m = 1; m < b.length; m++) {
                arr.push(parseInt('0x' + b[m]));
            }
        }
    }
    return arr;
}

function convertChinese(str) {
    var tmp = str.split(''), arr = [];
    for (var i = 0, c = tmp.length; i < c; i++) {
        var s = tmp[i].charCodeAt();
        if (s <= 0 || s >= 127) {
            arr.push(s.toString(16));
        }
        else {
            arr.push(tmp[i]);
        }
    }
    return arr;
}

function filterChinese(str) {
    var tmp = str.split(''), arr = [];
    for (var i = 0, c = tmp.length; i < c; i++) {
        var s = tmp[i].charCodeAt();
        if (s > 0 && s < 127) {
            arr.push(tmp[i]);
        }
    }
    return arr;
}

function strToHex(hex, isFilterChinese) {
    hex = isFilterChinese ? filterChinese(hex).join('') : convertChinese(hex).join('');

    //清除所有空格
    hex = hex.replace(/\s/g, "");
    //若字符个数为奇数，补一个0
    hex += hex.length % 2 !== 0 ? "0" : "";

    var c = hex.length / 2, arr = [];
    for (var i = 0; i < c; i++) {
        arr.push(parseInt(hex.substr(i * 2, 2), 16));
    }
    return arr;
}

function padLeft(s, w, pc) {
    if (pc === undefined) {
        pc = '0';
    }
    for (var i = 0, c = w - s.length; i < c; i++) {
        s = pc + s;
    }
    return s;
}
function toString(arr, isReverse) {
    if (typeof isReverse === undefined) {
        isReverse = true;
    }
    var hi = arr[0], lo = arr[1];
    return padLeft((isReverse ? hi + lo * 0x100 : hi * 0x100 + lo).toString(16).toUpperCase(), 4, '0');
}



function protocolToRawData(json) 
{
    var method = json.method;
    var id = json.id;
    var version = json.version;
    var payloadArray = [];
    var code=0;
    var orderstr="";
    if (method == ALINK_PROP_SET_METHOD) // 属性设置
    {	
        var params = json.params;
        var send_order = params.send_order;
    	var commaddr = params.comm_addr;
     	var devaddr=0;
     	var commnum= params.comm_serial;     	
     	payloadArray = payloadArray.concat(buffer_uint32(parseInt(commnum.slice(0,8),16)));
     	payloadArray = payloadArray.concat(buffer_uint32(parseInt(commnum.slice(8,16),16)));
    	if (send_order == 1)
    		{  //下发开指令
    		 devaddr= params.dev_addr;
    		 payloadArray = payloadArray.concat(buffer_uint8(commaddr));
    		 payloadArray = payloadArray.concat(buffer_uint8(5));
    		 payloadArray = payloadArray.concat(buffer_uint16(devaddr));
    		 payloadArray = payloadArray.concat(buffer_uint16(65280));
    		 orderstr= ("00"+commaddr.toString(16)).slice(-2)+"05";
    		 orderstr=orderstr + ("0000"+devaddr.toString(16)).slice(-4) + "FF00";
    		 var crcval = parseInt(ToModbusCRC16(orderstr).toString(16),16);
    		 payloadArray = payloadArray.concat(buffer_uint16(crcval));
    		}
    	else if (send_order == 2)
    		{
    		 devaddr= params.dev_addr;
    		 payloadArray = payloadArray.concat(buffer_uint8(commaddr));
    		 payloadArray = payloadArray.concat(buffer_uint8(5));
    		 payloadArray = payloadArray.concat(buffer_uint16(devaddr));
    		 payloadArray = payloadArray.concat(buffer_uint16(0));
    		 orderstr= ("00"+commaddr.toString(16)).slice(-2)+"05";
    		 orderstr=orderstr + ("0000"+devaddr.toString(16)).slice(-4) + "0000";
    		 var crcval = parseInt(ToModbusCRC16(orderstr).toString(16),16);
    		 payloadArray = payloadArray.concat(buffer_uint16(crcval));
	   		}
    	
    	else if (send_order == 10)
    		{  //下发任务
			 orderstr = ("00"+commaddr.toString(16)).slice(-2);
			 orderstr = orderstr + "100129000D1A"; //10-指令，0129-起始地址（40298-1），寄存器数量-000D，字节数-1A
			 orderstr = orderstr + "0001";			//远程指令
			 var areaarray = [];
			 areaarray = params.task_area.split(",");
			 var taskarea="";
			 var waternum = params.schedule_irrigate_num;
			 var waterinterval = params.schedule_irrigate_interval;
			 var fertnum = params.schedule_fertilize_num;
			 var tasktype = params.task_type;
			 
      		 for (i=0;i<16;i++)
	      		 {
      			 if (areaarray.indexOf(String(i+1)) >=0)
      			 	{
      				 	taskarea = "1" + taskarea;
      				 	continue;
      			 	}
      			 else
      			 	{
      				 	taskarea = "0" + taskarea;
      			 	}
	      		 }
      		taskarea=parseInt(taskarea,2)
      		orderstr = orderstr + ("0000"+taskarea.toString(16)).slice(-4);	//灌区
      		orderstr = orderstr + "0000";	//状态
      		orderstr = orderstr + ("0000"+parseInt(waternum).toString(16)).slice(-4);	//灌溉设定量
      		orderstr = orderstr + ("0000"+parseInt(waterinterval).toString(16)).slice(-4);	//灌溉设定时长
      		orderstr = orderstr + ("0000"+parseInt(fertnum).toString(16)).slice(-4);	//施肥量
      		orderstr = orderstr + "000000000000000000000000";	//设定日期
      		orderstr = orderstr + ("0000"+parseInt(tasktype).toString(16)).slice(-4);	//任务类型
      		orderstr = orderstr + ToModbusCRC16(orderstr);	//CRC验证
      		payloadArray = payloadArray.concat(buffer_uint8(commaddr));
      		payloadArray = payloadArray.concat(buffer_uint32(parseInt("10012900",16)));
      		payloadArray = payloadArray.concat(buffer_uint32(parseInt("0D1A0001",16)));
      		payloadArray = payloadArray.concat(buffer_uint16(taskarea));
      		payloadArray = payloadArray.concat(buffer_uint16(0));
      		payloadArray = payloadArray.concat(buffer_uint16(waternum));
      		payloadArray = payloadArray.concat(buffer_uint16(waterinterval));
      		payloadArray = payloadArray.concat(buffer_uint16(fertnum));
      		payloadArray = payloadArray.concat(buffer_uint32(0));
      		payloadArray = payloadArray.concat(buffer_uint16(0));
      		payloadArray = payloadArray.concat(buffer_uint16(0));
      		payloadArray = payloadArray.concat(buffer_uint16(tasktype));
      		payloadArray = payloadArray.concat(buffer_uint16(parseInt(ToModbusCRC16(orderstr),16)));
      		}
    	} 
    else if (method ==  ALINK_PROP_REPORT_METHOD) { //设备上报数据返回结果
        code = json.code;
        payloadArray = payloadArray.concat(buffer_uint8(COMMAND_REPORT_REPLY)); //command字段
        payloadArray = payloadArray.concat(buffer_int32(parseInt(id))); // ALink JSON格式 'id'
        payloadArray = payloadArray.concat(buffer_uint8(code));
    } else { //未知命令，对于有些命令不做处理
        code = json.code;
        payloadArray = payloadArray.concat(buffer_uint8(COMMAD_UNKOWN)); //command字段
        payloadArray = payloadArray.concat(buffer_int32(parseInt(id))); // ALink JSON格式 'id'
        payloadArray = payloadArray.concat(buffer_uint8(code));
    }
    //console.log(payloadArray); /*10进制转换成8进制*/
    return payloadArray;
}
Test()