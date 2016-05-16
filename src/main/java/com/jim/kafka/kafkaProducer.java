package com.jim.kafka;

import java.util.Properties;  
import java.util.concurrent.TimeUnit;  
  
import kafka.javaapi.producer.Producer;  
import kafka.producer.KeyedMessage;  
import kafka.producer.ProducerConfig;  
import kafka.serializer.StringEncoder;  

/**
 * 生产者：负责生产数据
 * @author Jim
 *
 */
public class kafkaProducer extends Thread{  
	// 主题
    private String topic;  
      
    public kafkaProducer(String topic){  
        super();  
        this.topic = topic;  
    }  
      
      
    @Override  
    public void run() {  
        Producer producer = createProducer();  
        int i=0;  
        while(true){  
            producer.send(new KeyedMessage<Integer, String>(topic, "message: " + i++));
            if(i ==100){
            	break;
            }
            try {  
                TimeUnit.SECONDS.sleep(1);  
                
            } catch (InterruptedException e) {  
                e.printStackTrace();  
            }  
        }  
    }  
  
    private Producer createProducer() {  
        Properties properties = new Properties();  
        properties.put("zookeeper.connect", "192.168.220.130:2182");//声明zk  
        properties.put("serializer.class", StringEncoder.class.getName());  
        properties.put("metadata.broker.list", "192.168.220.130:9092");// 声明kafka broker  
        properties.put("group.id", "test"); // 分组
        return new Producer<Integer, String>(new ProducerConfig(properties));  
     }  
      
    public static void main(String[] args) {  
        new kafkaProducer("test").start();// 使用kafka集群中创建好的主题 test   
    }  
       
}  