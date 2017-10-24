package com.futuredata.hbase;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Increment;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.util.Bytes;

import scala.Tuple2;

//import com.google.common.base.Optional;
import com.google.common.collect.Lists;

public class App {

	private static final Pattern SPACE = Pattern.compile(" ");

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {

		String tableName = "test";// args[3];
		String columnFamily = "f";// args[4];


		String zkQuorum = "ubuntu:2181";//////////////TODO

		String group = "1";

		String topicss = "OsoyooData";
		String numThread = "2";


		// SparkConf sparkConf = new SparkConf().setAppName("App");
		SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount")
				.setMaster("local[2]");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				new Duration(5000));

		final Broadcast<String> broadcastTableName = jssc.sparkContext()
				.broadcast(tableName);
		final Broadcast<String> broadcastColumnFamily = jssc.sparkContext()
				.broadcast(columnFamily);

		// JavaDStream<SparkFlumeEvent> flumeStream = sc.flumeStream(host,
		// port);

		int numThreads = Integer.parseInt(numThread);
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		String[] topics = topicss.split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}

		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils
				.createStream(jssc, zkQuorum, group, topicMap);

		JavaDStream<String> lines = messages
				.map(new Function<Tuple2<String, String>, String>() {
					/**
			 *
			 */
					private static final long serialVersionUID = 1L;

					public String call(Tuple2<String, String> tuple2) {
						return tuple2._2();
					}
				});

		JavaDStream<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					/**
			 *
			 */
					private static final long serialVersionUID = 1L;

					public Iterable<String> call(String x) {
						return Lists.newArrayList(SPACE.split(x));
					}
				});

		JavaPairDStream<String, Integer> lastCounts = messages
				.map(new Function<Tuple2<String, String>, String>() {
					/**
			 *
			 */
					private static final long serialVersionUID = 1L;

					public String call(Tuple2<String, String> tuple2) {
						return tuple2._2();
					}
				}).flatMap(new FlatMapFunction<String, String>() {
					/**
			 *
			 */
					private static final long serialVersionUID = 1L;

					public Iterable<String> call(String x) {
						return Lists.newArrayList(SPACE.split(x));
					}
				}).mapToPair(new PairFunction<String, String, Integer>() {
					/**
			 *
			 */
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {

					/**
			 *
			 */
					private static final long serialVersionUID = 1L;

					public Integer call(Integer x, Integer y) throws Exception {
						return x.intValue() + y.intValue();
					}
				});
		lastCounts.print();

		lastCounts.foreach(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {

					/**
		 *
		 */
					private static final long serialVersionUID = 1L;

					public Void call(JavaPairRDD<String, Integer> values,
							Time time) throws Exception {

						values.foreach(new VoidFunction<Tuple2<String, Integer>>() {

							/**
				 *
				 */
							private static final long serialVersionUID = 1L;

							public void call(Tuple2<String, Integer> tuple)
									throws Exception {
								HBaseCounterIncrementor incrementor = HBaseCounterIncrementor
										.getInstance(
												broadcastTableName.value(),
												broadcastColumnFamily.value());
								incrementor.incerment("Counter", tuple._1(),
										tuple._2());
								// HbaseTest.insertData("Counter", tuple._1(),
								// tuple._2());
								System.out.println("Counter:" + tuple._1() + "," + tuple._2());
							}

						});

						return null;
					}
				});

		jssc.start();
		jssc.awaitTermination();

	}
}