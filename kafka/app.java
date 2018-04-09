package com.yxs.test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import scala.collection.Map;

/**
 * Hello world!
 *
 */
public class App {

	/*
	 * 
	 * {"listener_security_protocol_map":{"PLAINTEXT":"PLAINTEXT"},"endpoints":[
	 * "PLAINTEXT://yxsicd-Aspire-4741:9092"],"jmx_port":-1,"host":
	 * "yxsicd-Aspire-4741","timestamp":"1521734620784","port":9092,"version":4}
	 * 
	 * 
	 */

	public static ConcurrentHashMap<String, KafkaProducer<String, String>> producerMap = new ConcurrentHashMap<String, KafkaProducer<String, String>>();

	public static ConcurrentHashMap<String, KafkaConsumer<String, String>> consumerMap = new ConcurrentHashMap<String, KafkaConsumer<String, String>>();

	static ObjectMapper om = new ObjectMapper();

	public static ExecutorService servicePool = Executors.newCachedThreadPool();

	public static KafkaProducer<String, String> getProducer(String name) {

		KafkaProducer<String, String> kafkaProducer = producerMap.get(name);
		if (kafkaProducer != null) {
			return kafkaProducer;
		}

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		producerMap.put(name, producer);

		return producer;
	}

	public static KafkaConsumer<String, String> getConsumer(String name) {

		KafkaConsumer<String, String> consumer = consumerMap.get(name);
		if (consumer != null) {
			return consumer;
		}

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", name);
		props.put("enable.auto.commit", "false");
		// props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>(props);
		consumerMap.put(name, consumer);
		return consumer;

		// consumer.subscribe(Arrays.asList("inventory_port_calc",
		// "inventory_ne_calc"));
		// while (true) {
		// ConsumerRecords<String, String> records = consumer.poll(100);
		// for (ConsumerRecord<String, String> record : records)
		// System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(),
		// record.key(), record.value());
		// }
		//
		// producerMap.put(name, producer);
		//
		// return producer;
	}

	public static ConsumerRecords<String, String> ReadMessage(String name, String topic, int count) {
		KafkaConsumer<String, String> consumer = getConsumer(name);
		TopicPartition tp = new TopicPartition(topic, 0);
		consumer.assign(Arrays.asList(tp));
		consumer.seek(tp, 0);
		ConsumerRecords<String, String> records = consumer.poll(count);
		return records;
	}

	public static void ListenMessage(String input_name, String input_topic) {

		final String name = input_name;
		final String topic = input_topic;

		Runnable command = new Runnable() {

			@Override
			public void run() {
				System.out.printf("listen start, name is %s, topic is %s", name, topic);
				KafkaConsumer<String, String> consumer = getConsumer(name);
				TopicPartition tp = new TopicPartition(topic, 0);
				consumer.assign(Arrays.asList(tp));
				consumer.seek(tp, 0);
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(1000);
					ShowRecordsMaxOffset(name, records);
				}
			}

		};
		servicePool.execute(command);

	}

	public static void ShowRecords(ConsumerRecords<String, String> records) {

		for (ConsumerRecord<String, String> record : records) {
			System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		}
	}

	public static void ShowRecordsMaxOffset(String name, ConsumerRecords<String, String> records) {

		for (ConsumerRecord<String, String> record : records) {

			long offset = record.offset();
			if (offset % 500 == 0) {
				System.out.printf("name= %s, offset = %d, key = %s, value = %s%n", name, record.offset(), record.key(), record.value());
			}
		}
	}

	public static void initTopic() {
		Properties topicConfig = new Properties();
		topicConfig.setProperty("compression.type", "gzip");
		// topicConfig.put("key.serializer",
		// "org.apache.kafka.common.serialization.StringSerializer");
		// topicConfig.put("value.serializer",
		// "org.apache.kafka.common.serialization.StringSerializer");
		String serverstring = "localhost:2181";
		MyStringS zkSerializer = new MyStringS();
		ZkClient zkClient = new ZkClient(serverstring, 5000, 5000, zkSerializer);
		// zkClient.setZkSerializer(zkSerializer);
		ZkConnection zkConnection = new ZkConnection(serverstring);
		ZkUtils zk = new ZkUtils(zkClient, zkConnection, false);

		Map<String, Properties> fetchAllTopicConfigs = AdminUtils.fetchAllTopicConfigs(zk);
		System.out.println(fetchAllTopicConfigs);

		// AdminUtils.deleteTopic(zk, "inventory_ne_input");
		// AdminUtils.deleteTopic(zk, "inventory_port_input");
		// AdminUtils.deleteTopic(zk, "inventory_ne_output");
		// AdminUtils.deleteTopic(zk, "inventory_port_output");

		AdminUtils.createTopic(zk, "inventory_ne_input", 2, 1, topicConfig, RackAwareMode.Safe$.MODULE$);
		AdminUtils.createTopic(zk, "inventory_port_input", 2, 1, topicConfig, RackAwareMode.Safe$.MODULE$);
		AdminUtils.createTopic(zk, "inventory_ne_output", 2, 1, topicConfig, RackAwareMode.Safe$.MODULE$);
		AdminUtils.createTopic(zk, "inventory_port_output", 2, 1, topicConfig, RackAwareMode.Safe$.MODULE$);

		fetchAllTopicConfigs = AdminUtils.fetchAllTopicConfigs(zk);
		System.out.println(fetchAllTopicConfigs);
	}

	public static <T> T parseJson(String input, Class<T> valueType) {
		try {
			return om.readValue(input, valueType);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void initStream() throws IOException {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "inventory");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		final StreamsBuilder builder = new StreamsBuilder();

		GlobalKTable<Object, Object> tbl_ne = builder.globalTable("inventory_ne_input");

		// KStream<Object, Object> stream_ne = builder.stream("inventory_ne_input");

		KStream<Object, Object> stream_port = builder.stream("inventory_port_input");

		KeyValueMapper<? super Object, ? super Object, ? extends Object> keyValueMapper = new KeyValueMapper<Object, Object, Object>() {

			@Override
			public Object apply(Object key, Object value) {
				// TODO Auto-generated method stub
				JsonNode parseJson = parseJson(new String((byte[]) value), JsonNode.class);
				return parseJson.path("1002").asText().getBytes();
			}
		};
		ValueJoiner<? super Object, ? super Object, ? extends Object> joiner = new ValueJoiner<Object, Object, Object>() {

			@Override
			public Object apply(Object value1, Object value2) {

				ObjectNode port_value = parseJson(new String((byte[]) value1), ObjectNode.class);
				JsonNode ne_value = parseJson(new String((byte[]) value2), JsonNode.class);

				port_value.put("1003", ne_value.path("1001").asText());
				return port_value.toString().getBytes();
			}
		};
		KStream<Object, Object> stream_output_port = stream_port.join(tbl_ne, keyValueMapper, joiner);
		stream_output_port.to("inventory_port_output");

		final Topology topology = builder.build();
		System.out.println(topology.describe());

		final KafkaStreams streams = new KafkaStreams(topology, props);

		streams.start();
		//
		// final CountDownLatch latch = new CountDownLatch(1);
		//
		// // attach shutdown handler to catch control-c
		// Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
		// @Override
		// public void run() {
		// streams.close();
		// latch.countDown();
		// }
		// });
		//
		// try {
		// streams.start();
		// latch.await();
		// } catch (Throwable e) {
		// System.exit(1);
		// }
		// System.exit(0);
	}

	public static void initTestData() {
		KafkaProducer<String, String> producer = getProducer("test");
		for (int i = 0; i < 50000; i++) {
			String id = Integer.toString(i) + "";

			ObjectNode ne_node = JsonNodeFactory.instance.objectNode();
			ne_node.put("100", id);
			ne_node.put("1001", "ne_" + id);
			String ne_value = ne_node.toString();

			// System.out.println(ne_value);
			producer.send(new ProducerRecord<String, String>("inventory_ne_input", id, ne_value));
			for (int j = 0; j < 5; j++) {
				String port_id = id + "_" + Integer.toString(j) + "";

				ObjectNode port_node = JsonNodeFactory.instance.objectNode();
				port_node.put("100", port_id);
				port_node.put("1001", "port_" + port_id);
				port_node.put("1002", id);

				String port_value = port_node.toString();
				// System.out.println(port_value);
				producer.send(new ProducerRecord<String, String>("inventory_port_input", port_id, port_value));
			}
		}
		producer.flush();
		System.out.println("init done");

	}

	public static void sendTestData(int count) {
		KafkaProducer<String, String> producer = getProducer("test");

		// System.out.println(port_value);

		for (int i = 0; i < count; i++) {

			String id = Math.round(Math.random() * 50000 % 50000) + "";
			String port_id = id + "_" + Long.toString(System.currentTimeMillis()) + "";

			ObjectNode port_node = JsonNodeFactory.instance.objectNode();
			port_node.put("100", port_id);
			port_node.put("1001", "port_" + port_id);
			port_node.put("1002", id);

			String port_value = port_node.toString();

			producer.send(new ProducerRecord<String, String>("inventory_port_input", port_id, port_value));
		}
		producer.flush();

	}

	public static void main(String[] args) throws IOException {
		// initTopic();
		// initTestData();
		// ConsumerRecords<String, String> readMessage = ReadMessage("test",
		// "inventory_port_input", 2000);
		// System.out.println(readMessage.count());
		// ShowRecords(readMessage);

		while (true) {
			int rkey = System.in.read();
			// System.out.println("read key : " + rkey);
			switch (rkey) {
			case 96:
				initStream();
				break;
			case 50:
				sendTestData(1000);
				break;
			case 51:
				sendTestData(10000);
				break;
			case 52:
				sendTestData(100000);
				break;
			case 53:
				sendTestData(1000000);
				break;
			case 49:
				ListenMessage(UUID.randomUUID().toString(), "inventory_port_input");
				break;
			}
		}

	}

}
