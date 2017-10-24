package com.futuredata.hbase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * Checkpoint example
 *
 * @author
 * @date
 */
public class CheckpointTest {

    private static String CHECKPOINT_DIR = "/tmp/checkpoint";

    public static void main(String[] args) {

        // get javaStreamingContext from checkpoint dir or create from sparkconf
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(CHECKPOINT_DIR, new JavaStreamingContextFactory() {
            public JavaStreamingContext create() {
                return createContext();
            }
        });

        jssc.start();
        jssc.awaitTermination();

    }

    public static JavaStreamingContext createContext() {

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("tachyon-test-consumer");

        Set<String> topicSet = new HashSet<String>();
        topicSet.add("uuu");

        HashMap<String, String> kafkaParam = new HashMap<String, String>();
//        kafkaParam.put("metadata.broker.list", "test1:9092,test2:9092");
        //kafkaParam.put("metadata.broker.list", "localhost:2181");
        kafkaParam.put("bootstrap.servers", "node5:9092");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        // do checkpoint metadata to hdfs
        jssc.checkpoint(CHECKPOINT_DIR);

        JavaPairInputDStream<String, String> message =
                KafkaUtils.createDirectStream(
                        jssc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParam,
                        topicSet
                );

        JavaDStream<String> valueDStream = message.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._2();
            }
        });
        valueDStream.count().print();

        return jssc;
    }
}