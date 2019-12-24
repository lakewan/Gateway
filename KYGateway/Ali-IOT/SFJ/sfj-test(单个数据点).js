var COMMAND_REPORT = 0x00;
var COMMAND_SET = 0x01; 
var COMMAND_HANDORDER_SET = 0x02; 
var COMMAND_TASKORDER_SET = 0x03;
var COMMAD_UNKOWN = 0xff;   
var ALINK_PROP_REPORT_METHOD = 'thing.event.property.post'; 
//var ALINK_PROP_SET_METHOD = 'thing.service.property.set';
var ALINK_PROP_ORDER_SET_METHOD = 'thing.service.taskorder'; 



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

    	if (uint8Array[1] == 15){ //获取状态

    		tempa = ("00000000"+dataView.getUint8(8).toString(2)).slice(-8)+("00000000"+dataView.getUint8(7).toString(2)).slice(-8);
    		devstate= tempa.split('').reverse().join('');
	        jsonMap.method = ALINK_PROP_REPORT_METHOD; //ALink JSON格式 - 属性上报topic
	        jsonMap.version = "1.0"; //ALink JSON格式 - 协议版本号固定字段
	        jsonMap.id = "" + dataView.getUint32(1); //ALink JSON格式 - 标示该次请求id值
	        
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
	    else if(uint8Array[1] == 16){ //任务上传

    		var remote_attr = dataView.getInt16[20];
    		tempa = ("00000000"+dataView.getUint16(9).toString(2)).slice(-8)+("00000000"+dataView.getUint16(10).toString(2)).slice(-8);
    		devstate= tempa.split('').reverse().join('');
	        jsonMap.method = ALINK_PROP_REPORT_METHOD; //ALink JSON格式 - 属性上报topic
	        jsonMap.version = "1.1"; //ALink JSON格式 - 协议版本号固定字段
	        jsonMap.id = "" + dataView.getUint32(1); //ALink JSON格式 - 标示该次请求id值
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
			params.task_state = dataView.getUint16(11);
	        params.schedule_irrigate_num = dataView.getUint16(13)/10;
	        params.schedule_irrigate_interval = dataView.getUint16(15);
	        params.schedule_fertilize_num = dataView.getUint16(17)/10;
	        
	        params.task_type = dataView.getUint16(31);   	//0-灌溉，1-施肥
	        params.irrigate_mode = dataView.getUint16(33); //0-定量，1-定时
	        params.preirrigate = dataView.getUint16(35);	//0-无预灌溉，1-有预灌溉
	        params.clean_attr = dataView.getUint16(37);	//0-无清洁，1-有清洁

	        params.start_year = dataView.getUint16(41); 
	        params.start_month = dataView.getUint16(43); 
	        params.start_day = dataView.getUint16(45);
	        params.start_hour = dataView.getUint16(47) ;
	        params.start_minute =dataView.getUint16(49) ;
	        params.start_second = dataView.getUint16(51);
			params.end_year = dataView.getUint16(53) ;
	        params.end_month = dataView.getUint16(55) ;
	        params.end_day =dataView.getUint16(57);
	        params.end_hour = dataView.getUint16(59) ;
	        params.end_minute = dataView.getUint16(61) ;
	        params.end_second = dataView.getUint16(63);	        
	        params.real_irrigate_num = dataView.getUint16(65)/10;
	        params.real_irrigate_interval = dataView.getUint16(67);
	        params.real_fertilize_num = dataView.getUint16(69)/10;
	        jsonMap.params = params; //ALink JSON格式 - params标准字段
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
    if (method == ALINK_PROP_ORDER_SET_METHOD) // 属性设置
    {	
        var params = json.params;
        var tasktype = params.tasktype;
    	var pass = params.pass;
     	var ordertype = params.ordertype;
     	var commaddr = 2;

    	if (tasktype == 1)
    	{  //下发开指令
    		if (ordertype == 1) 
    		{
    		 payloadArray = payloadArray.concat(buffer_uint8(2));
       		 payloadArray = payloadArray.concat(buffer_uint8(5));
       		 payloadArray = payloadArray.concat(buffer_uint16(pass));
       		 payloadArray = payloadArray.concat(buffer_uint16(65280));
       		 orderstr= ("00"+commaddr.toString(16)).slice(-2)+"05";
       		 orderstr=orderstr + ("0000"+pass.toString(16)).slice(-4) + "FF00";
       		 var crcval = parseInt(ToModbusCRC16(orderstr,true).toString(16),16);
       		 payloadArray = payloadArray.concat(buffer_uint16(crcval));
    		} 
    		else if (ordertype == 0) 
    		{
    			payloadArray = payloadArray.concat(buffer_uint8(2));
          		payloadArray = payloadArray.concat(buffer_uint8(5));
          		payloadArray = payloadArray.concat(buffer_uint16(pass));
          		payloadArray = payloadArray.concat(buffer_uint16(0));
          		orderstr= ("00"+commaddr.toString(16)).slice(-2)+"05";
          		orderstr=orderstr + ("0000"+pass.toString(16)).slice(-4) + "0000";
          		var crcval = parseInt(ToModbusCRC16(orderstr,true).toString(16),16);
          		payloadArray = payloadArray.concat(buffer_uint16(crcval));
    		}
    		 
    	}
  
    } 
    //console.log(payloadArray); /*10进制转换成8进制*/
    return payloadArray;
}