package com.jim.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * 消费者
 * @author Jim
 *
 */
public class kafkaConsumer2 extends Thread {
	// 主题
	private String topic;

	public kafkaConsumer2(String topic) {
		super();
		this.topic = topic;
	}

	@Override
	public void run() {
		ConsumerConnector consumer = createConsumer();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams 
				= consumer.createMessageStreams(topicCountMap);
		// 获取每次接收到的这个数据
		KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
		while (iterator.hasNext()) {
			String message = new String(iterator.next().message());
			System.out.println("接收到: " + message);
		}
	}

	private ConsumerConnector createConsumer() {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "192.168.220.130:2181");// 声明zk
		properties.put("group.id", "test");// 必须要使用别的组名称，
											// 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
		// # true时，Consumer会在消费消息后将offset同步到zookeeper，
		// 这样当Consumer失败后，新的consumer就能从zookeeper获取最新的offset
		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("session.timeout.ms", "30000");
		// properties.put("key.deserializer",
		// "org.apache.kafka.common.serialization.StringDeserializer");
		// properties.put("value.deserializer",
		// "org.apache.kafka.common.serialization.StringDeserializer");
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
	}

	public static void main(String[] args) {
		new kafkaConsumer2("test2").start();// 使用kafka集群中创建好的主题 test

	}

}