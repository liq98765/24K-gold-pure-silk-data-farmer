import java.util.Properties
import java.io.{File, FileInputStream, IOException, InputStream}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.mutable.AbstractBuffer
import java.text.SimpleDateFormat
import java.util.Date

import scala.io.Source

import java.util.Properties
import java.io.{File, FileInputStream, IOException, InputStream}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.mutable.AbstractBuffer
import java.text.SimpleDateFormat
import java.util.Date

import scala.io.Source
import kafka.utils.ZKStringSerializer

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.ZkConnection

import kafka.admin.AdminUtils
import kafka.admin.RackAwareMode
import kafka.utils.ZkUtils
import kafka.admin.AdminUtils
import kafka.admin.TopicCommand
import kafka.admin.AdminUtils
import kafka.server.ConfigType
import java.util.Properties
class kafkaProducerMsgnew() extends Runnable{

//  private val BROKER_LIST = "localhost:9092" //"master:9092,worker1:9092,worker2:9092"
//  private val TARGET_TOPIC = topic //"new"
//  private val TARGET_TOPIC = "test" //"new"
//  private val DIR = "D:\\iot-test\\data"

  import kafka.utils.ZKStringSerializer

//  import kafka.admin.AdminUtils
//  import kafka.admin.RackAwareMode
//  import kafka.utils.ZKStringSerializer
//  import kafka.utils.ZkUtils
//  import org.I0Itec.zkclient.ZkClient
//  import org.I0Itec.zkclient.ZkConnection

//  val zookeeperConnect = "zkserver1:2181,zkserver2:2181"
//  val sessionTimeoutMs: Int = 10 * 1000
//  val connectionTimeoutMs: Int = 8 * 1000
//
//  val topic = "my-topic"
//  val partitions = 1
//  val replication = 1
//  val topicConfig = new Nothing // add per-topic configurations settings here
//
//  // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
//  // createTopic() will only seem to work (it will return without error).  The topic will exist in
//  // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
//  // topic.
//  val zkClient = new ZkClient(zookeeperConnect, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer.MODULE$)
//
//  // Security for Kafka was added in Kafka 0.9.0.0
//  val isSecureKafkaCluster = false
//
//  val zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster)
//  AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig, RackAwareMode.Enforced$.MODULE$)
//  zkClient.close
//
//  val zkClient = new Nothing("localhost:2181", 10000, 10000, ZKStringSerializer.MODULE$)
//  AdminUtils.createTopic(zkClient, "myTopic", 10, 1, new Nothing)

//  val arguments = new Array[String](8)
//  arguments(0) = "--zookeeper"
//  arguments(1) = "10.***.***.***:2181"
//  arguments(2) = "--replica"
//  arguments(3) = "1"
//  arguments(4) = "--partition"
//  arguments(5) = "1"
//  arguments(6) = "--topic"
//  arguments(7) = "test-topic-Biks"

  //CreateTopicCommand.main(arguments)

//  val sessionTimeoutMs = 10000
//    val connectionTimeoutMs = 10000
//    val (zkClient, zkConnection) = ZkUtils.createZkClientAndConnection(
//      "localhost:2181", sessionTimeoutMs, connectionTimeoutMs)
//    val zkUtils = new ZkUtils(zkClient, zkConnection, false)
//    val numPartitions = 1
//    val replicationFactor = 1
//    val topicConfig = new Properties
//    val topic = "my-topic"
//    AdminUtils.createTopic(zkUtils, topic, numPartitions, replicationFactor, topicConfig)
val zkUtils = ZkUtils("localhost:2181", 10000, 10000, false)
  // Create a topic named 'test-topic' with 1 partition and 1 replica
  AdminUtils.createTopic(zkUtils, "test-topic", 1, 1)

    val props1: Properties = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, "test")

  /**
    * 1、配置属性
    * metadata.broker.list : kafka集群的broker，只需指定2个即可
    * serializer.class : 如何序列化发送消息
    * request.required.acks : 1代表需要broker接收到消息后acknowledgment,默认是0
    * producer.type : 默认就是同步sync
    */
  val postgprop = new Properties()
  //val ipstream: InputStream = this.getClass().getResourceAsStream("/application.properties")
  //val ipstream: InputStream = classOf[Any].getResourceAsStream("/test.properties")
  val ipstream = new FileInputStream("D:\\iot-test\\src\\main\\resources\\application.properties")
  postgprop.load(ipstream)
  ipstream.close()

  //postgprop.load(ipstream)
  private val props = new Properties()
  //props.put("metadata.broker.list", BROKER_LIST)
  //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST)
  props.put("metadata.broker.list", postgprop.getProperty("BROKER_LIST"))
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, postgprop.getProperty("BROKER_LIST"))
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  /**
    * 2、创建Producer
    */
  //private val config = new ProducerConfig(this.props)
  val producer = new KafkaProducer[String, String](props)
  /**
    * 3、获得文件夹下文件
    */
  def ls(dir: String) : Seq[File] = {
    new File(dir).listFiles.flatMap {
      case f if f.isDirectory => ls(f.getPath)
      case x => List(x)
    }
  }
  /**
    * 3、产生并发送消息
    * 搜索目录dir下的所有包含“transaction”的文件并将每行的记录以消息的形式发送到kafka
    *
    */
  //車両のVIN
  var vinArray = Array("1J9FN78S5VL123456","1J9FN78S5VL123457","1J9FN78S5VL123458","1J9FN78S5VL123459"
    ,"1J9FN78S5VL123460","1J9FN78S5VL123461","1J9FN78S5VL123462")
  def run() : Unit = {
    while(true){
      val now: Date = new Date()
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
      val date = dateFormat.format(now)
      //val files = Path(this.DIR).walkFilter(p => p.isFile && p.name.contains("transaction"))
      //val files = Path(this.DIR).walkFilter(p => p.isFile && p.extension.contains("txt"))
      //val files = ls(this.DIR).filter(_.getPath.endsWith(".txt"))
      val file = postgprop.getProperty("DIR")
      try{
        //for(file <- files){
          val reader = Source.fromFile(file.toString(), "UTF-8")
          //ローカルでVINを決めるために乱数を生成して、一台の車両のVINを選択する
          val randomVin = (new util.Random).nextInt(3)
          for(line <- reader.getLines()){
            val tARGET_TOPIC = vinArray(randomVin).substring(14)
            val lineForMessage = vinArray(randomVin) + " " + line + "" + date
            //producer.send(new ProducerRecord(postgprop.getProperty("tes"), lineForMessage))
            producer.send(new ProducerRecord("test-topic", lineForMessage))
            //            val message = new KeyedMessage[String, String]("test", lineForMessage)
            //            producer.send(message)
          }

          //produce完成后，将文件copy到另一个目录，之后delete
          //          val fileName = file.toFile.name
          //          file.toFile.copyTo(Path("C:\\intellijPro\\iot-vehicle-producer\\completed" +fileName + ".completed"))
          //          file.delete()
       // }
      }catch{
        case e : Exception => println(e)
      }

      try{
        //sleep for 3 seconds after send a micro batch of message
        Thread.sleep(3000)
      }catch{
        case e : Exception => println(e)
      }
    }
    println(postgprop.getProperty("BROKER_LIST"))
  }

}

object ProduceMsg {
  def main(args : Array[String]): Unit ={
    /*if(args.length < 2){
      println("Usage : ProduceMsg master:9092,worker1:9092 new")

      System.exit(1)
    }*/

    new Thread(new kafkaProducerMsgnew()).start()
  }
}