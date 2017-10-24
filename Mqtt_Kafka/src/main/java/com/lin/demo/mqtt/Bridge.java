package com.lin.demo.mqtt;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class Bridge implements MqttCallback {
	private Logger logger = Logger.getLogger(this.getClass().getName());
	private MqttAsyncClient mqtt;
	private Producer<String, String> kafkaProducer;

	private void connect(String serverURI, String clientId, String zkConnect) throws MqttException {

		mqtt = new MqttAsyncClient(serverURI, clientId);
		mqtt.setCallback(this);
		IMqttToken token = mqtt.connect();
		Properties props = new Properties();

		//Updated based on Kafka v0.8.1.1
		props.put("metadata.broker.list", "ubuntu:9092");//////////////TODO
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "com.lin.demo.mqtt.SimplePartitioner");
        props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		kafkaProducer = new Producer<String, String>(config);
		token.waitForCompletion();
		logger.info("Connected to MQTT and Kafka");
	}

	private void reconnect() throws MqttException {
		IMqttToken token = mqtt.connect();
		token.waitForCompletion();
	}

	private void subscribe(String[] mqttTopicFilters) throws MqttException {

		int[] qos = new int[mqttTopicFilters.length];
		for (int i = 0; i < qos.length; ++i) {
			qos[i] = 0;
		}
		//mqtt.subscribe(mqttTopicFilters, qos);
		mqtt.subscribe("OsoyooData", 0);
	}

	@Override
	public void connectionLost(Throwable cause) {
		logger.warn("Lost connection to MQTT server", cause);
		while (true) {
			try {
				logger.info("Attempting to reconnect to MQTT server");
				reconnect();
				logger.info("Reconnected to MQTT server, resuming");
				return;
			} catch (MqttException e) {
				logger.warn("Reconnect failed, retrying in 10 seconds", e);
			}
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
			}
		}
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		// TODO Auto-generated method stub

	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {

		byte[] payload = message.getPayload();
		System.out.println(new String(message.getPayload()));
		//Updated based on Kafka v0.8.1.1
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, new String(message.getPayload()));
		kafkaProducer.send(data);
	}

	/**
	 * @param args
	 */
	public static void main(String args[]) {

		try {

			Bridge bridge = new Bridge();
			bridge.connect("tcp://ubuntu:1883", "mqttKafkaBridge", "ubuntu:2181");//////////////TODO
			bridge.subscribe("OsoyooData".split(","));
		} catch (MqttException e) {
			e.printStackTrace(System.err);
		}
	}
}