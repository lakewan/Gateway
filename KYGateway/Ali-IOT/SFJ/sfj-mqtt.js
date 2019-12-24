import com.alibaba.taro.AliyunIoTSignUtil;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// 透传类设备测试
public class IoTDemoPubSubDemo {

    public static String productKey = "a1YnjAyphtc";
    public static String deviceName = "test_machine";
    public static String deviceSecret = "o0R64zQGVdcUADXTXtA0rW25jJdJeEuT";
    public static String regionId = "cn-shanghai";

    // 物模型-属性上报topic
    private static String pubTopic = "/sys/" + productKey + "/" + deviceName + "/thing/model/up_raw";
    // 物模型-订阅属性Topic
    private static String subTopic = "/sys/" + productKey + "/" + deviceName + "/thing/model/down_raw";

    private static MqttClient mqttClient;

    public static void main(String [] args){

        initAliyunIoTClient(); // 初始化Client
//        ScheduledExecutorService scheduledThreadPool = new ScheduledThreadPoolExecutor(1,
//                new ThreadFactoryBuilder().setNameFormat("thread-runner-%d").build());
//
//        scheduledThreadPool.scheduleAtFixedRate(()->postDeviceProperties(), 10,10, TimeUnit.SECONDS);
        // 汇报属性
        postDeviceProperties();
        try {
            mqttClient.subscribe(subTopic); // 订阅Topic
        } catch (MqttException e) {
            System.out.println("error:" + e.getMessage());
            e.printStackTrace();
        }

        // 设置订阅监听
        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("connection Lost");

            }

            @Override
            public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                System.out.println("Sub message");
                System.out.println("Topic : " + s);
                System.out.println("16进制形式输出：");
                System.out.println(bytes2hex(mqttMessage.getPayload()));

                System.out.println("10进制形式输出：");
                byte[] bytes = mqttMessage.getPayload();
                for (byte t:bytes)
                {
                    System.out.print(t + " ");
                }
             }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

            }
        });

    }

    /**
     * 初始化 Client 对象
     */
    private static void initAliyunIoTClient() {

        try {
            // 构造连接需要的参数
            String clientId = "java" + System.currentTimeMillis();
            Map<String, String> params = new HashMap<>(16);
            params.put("productKey", productKey);
            params.put("deviceName", deviceName);
            params.put("clientId", clientId);
            String timestamp = String.valueOf(System.currentTimeMillis());
            params.put("timestamp", timestamp);
            // cn-shanghai
            String targetServer = "tcp://" + productKey + ".iot-as-mqtt."+regionId+".aliyuncs.com:1883";

            String mqttclientId = clientId + "|securemode=3,signmethod=hmacsha1,timestamp=" + timestamp + "|";
            String mqttUsername = deviceName + "&" + productKey;
            String mqttPassword = AliyunIoTSignUtil.sign(params, deviceSecret, "hmacsha1");

            connectMqtt(targetServer, mqttclientId, mqttUsername, mqttPassword);

        } catch (Exception e) {
            System.out.println("initAliyunIoTClient error " + e.getMessage());
        }
    }

    public static void connectMqtt(String url, String clientId, String mqttUsername, String mqttPassword) throws Exception {

        MemoryPersistence persistence = new MemoryPersistence();
        mqttClient = new MqttClient(url, clientId, persistence);
        MqttConnectOptions connOpts = new MqttConnectOptions();
        // MQTT 3.1.1
        connOpts.setMqttVersion(4);
        connOpts.setAutomaticReconnect(false);
        connOpts.setCleanSession(true);

        connOpts.setUserName(mqttUsername);
        connOpts.setPassword(mqttPassword.toCharArray());
        connOpts.setKeepAliveInterval(60);

        mqttClient.connect(connOpts);
    }

    /**
     * 汇报属性
     */
    private static void postDeviceProperties() {

        try {
            //上报数据
            //高级版 物模型-属性上报payload
            System.out.println("上报属性值");
            String hexString = "000111";
            byte[] payLoad = hexToByteArray(hexString);
            MqttMessage message = new MqttMessage(payLoad);
            message.setQos(0);
            mqttClient.publish(pubTopic, message);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    // 十进制byte[] 转16进制 String
    public static String bytes2hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        String tmp = null;
        for (byte b : bytes) {
            // 将每个字节与0xFF进行与运算，然后转化为10进制，然后借助于Integer再转化为16进制
            tmp = Integer.toHexString(0xFF & b);
            if (tmp.length() == 1) {
                tmp = "0" + tmp;
            }
            sb.append(tmp);
        }
        return sb.toString();

    }

    /**
     * hex字符串转byte数组
     * @param inHex 待转换的Hex字符串
     * @return  转换后的byte数组结果
     */
    public static byte[] hexToByteArray(String inHex){
        int hexlen = inHex.length();
        byte[] result;
        if (hexlen % 2 == 1){
            //奇数
            hexlen++;
            result = new byte[(hexlen/2)];
            inHex="0"+inHex;
        }else {
            //偶数
            result = new byte[(hexlen/2)];
        }
        int j=0;
        for (int i = 0; i < hexlen; i+=2){
            result[j]=hexToByte(inHex.substring(i,i+2));
            j++;
        }
        return result;
    }

    /**
     * Hex字符串转byte
     * @param inHex 待转换的Hex字符串
     * @return  转换后的byte
     */
    public static byte hexToByte(String inHex) {
        return (byte) Integer.parseInt(inHex, 16);
    }
}