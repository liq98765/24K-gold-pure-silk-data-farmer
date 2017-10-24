package com.app.simple.producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
	private final Producer producer;
	public static String TOPIC = "";
	public static String MSG = "";

	public KafkaProducer(String topic,String msg) {
		TOPIC = topic;
		MSG = msg;
		Properties props = new Properties();
		// 此处配置的是kafka的端口
		props.put("metadata.broker.list", "node5:9092");
		props.put("zk.connect", "node5:2181");

		// 配置value的序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// 配置key的序列化类
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");

		props.put("request.required.acks", "1");

		producer = new Producer<String, String>(new ProducerConfig(props));
	}

	@SuppressWarnings("deprecation")
	public void produce() {
		int messageNo = 0;
		final int COUNT = 1000;

		while (messageNo<COUNT) {
			String key = String.valueOf(messageNo);
			KeyedMessage keyMessage = new KeyedMessage<String, String>(TOPIC, key, MSG);
			producer.send(keyMessage);
            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
			System.out.println(MSG);
			messageNo++;
		}
	}

//	public static void main(String[] args) {
//		new KafkaProducer(TOPIC,MSG).produce();
//	}
}
