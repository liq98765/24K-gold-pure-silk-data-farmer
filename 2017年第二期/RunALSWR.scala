

package ch03

object RunALSWR  {
  import org.apache.spark.mllib.linalg._
  import org.apache.spark.mllib.regression._
  import org.apache.spark.mllib.recommendation.{ALS, Rating}
  import org.apache.spark._
  import org.apache.spark.{SparkContext, SparkConf}
  import SparkContext._
  import scala.util.Properties

  import org.apache.spark.mllib.evaluation._
  import org.apache.spark.mllib.clustering._
  import org.apache.spark.rdd._
  /**
 * Spark 数据挖掘实战 案例音乐推荐
 * 注意：ALS 限制每一个用户产品对必须有一个 ID，而且这个 ID 必须小于 Integer.MAX_VALUE
 * Created by clebeg.xie on 2015/10/29.
 */
  val rootDir = "D:\\Spark\\eclipse\\workplace\\Demo\\src\\ch03\\";
   //val conf = new SparkConf().setMaster("local").setAppName("RunALSWR")
      //val sc = new SparkContext(conf)
  //本地测试
  def main(args: Array[String]) {
      val conf = new SparkConf().setMaster("local").setAppName("RunALSWR")
      val sc = new SparkContext(conf)
      val rawUserArtistData = sc.textFile(rootDir + "user_artist_data_14903.txt")
      //检查数据集是否超过最大值，对于计算密集型算法，原始数据集最好多分块
    //println(rawUserArtistData.first())
    //println(rawUserArtistData.map(_.split(' ')(0).toDouble).stats())
    //println(rawUserArtistData.map(_.split(' ')(1).toDouble).stats())
    //艺术家ID和名字对应
      val artistById = artistByIdFunc(sc)
    //艺术家名字重复
    val aliasArtist = artistsAlias(sc)
    aslModelTest(sc, aliasArtist, rawUserArtistData, artistById)
    //查看一下 2093760 这个用户真正听的歌曲
    val existingProducts = rawUserArtistData.map(_.split(' ')).filter {
      case Array(userId, _, _) => userId.toInt == 2093760
    }.map{
      case Array(_, artistId, _) => {
        aliasArtist.getOrElse(artistId.toInt, artistId.toInt)
      }
    }.collect().toSet

    artistById.filter {
      line => line match {
        case Some((id, name)) => existingProducts.contains(id)
        case None => false
      }

    }.collect().foreach(println)
  }
    /**
   * 获取艺术家名字和ID的对应关系
   * 有些艺术家名字和ID没有按 \t 分割，错误处理就是放弃这些数据
   * @param sc
   * @return
   */
  def artistByIdFunc(sc: SparkContext): RDD[Option[(Int, String)]] = {
    val rawArtistData = sc.textFile(rootDir + "artist_data.txt")
    val artistByID = rawArtistData.map {
      line =>
        //span 碰到第一个不满足条件的开始划分， 少量的行转换不成功， 数据质量问题
        val (id, name) = line.span(_ != '\t')
        if (name.isEmpty) {
          None
        } else {
          try {
            //特别注意Some None缺失值处理的方式，Scala 中非常给力的一种方法
            Some((id.toInt, name.trim))
          } catch {
            case e: NumberFormatException =>
              None
          }
        }
    }
    artistByID
  }
    
    /**
   * 通过文件 artist_alias.txt 得到所有艺术家的别名
   * 文件不大，每一行按照 \t 分割包含一个拼错的名字ID 还有一个正确的名字ID
   * 一些行没有第一个拼错的名字ID，直接跳过
   * @param sc Spark上下文
   * @return
   */
  def artistsAlias(sc: SparkContext) = {
    val rawArtistAlias = sc.textFile(rootDir + "artist_alias.txt")
    val artistAlias = rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()
    artistAlias
  }

  def aslModelTest(sc: SparkContext,
                   aliasArtist: scala.collection.Map[Int, Int],
                   rawUserArtistData: RDD[String],
                   artistById: RDD[Option[(Int, String)]] ) = {
    //将对应关系广播出去，因为这个数据量不大，Spark广播变量类似于 hive 的 mapjoin
    val bArtistAlias = sc.broadcast(aliasArtist)
    //转换重复的艺术家的ID为同一个ID，然后将
    val trainData = rawUserArtistData.map{
      line =>
        val Array(userId, artistId, count) = line.split(' ').map(_.toInt)
        val finalArtistID = bArtistAlias.value.getOrElse(artistId, artistId)
        Rating(userId, finalArtistID, count)
    }.cache()
    //模型训练
    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
    //模型建立之后，为某个用户给出一个具体的推荐列表
    val recommendations = model.recommendProducts(2093760, 5) //为ID为2093760的用户推荐5个产品
    recommendations.foreach(println)
    val recommendedProductIDs = recommendations.map(_.product).toSet
    //输出推荐的艺术家的名字
    artistById.filter {
      line => line match {
        case Some((id, name)) => recommendedProductIDs.contains(id)
        case None => false
      }
    }.collect().foreach(println)
  
 /*val rawArtistData = sc.textFile("D:/Spark/eclipse/workplace/Demo/src/ch03/artist_data.txt")
val artistByID = rawArtistData.flatMap { line =>
    val (id, name) = line.span(_ != '\t')
    if (name.isEmpty) {
        None
    } else {
        try {
            Some((id.toInt, name.trim))
        } catch {
            case e: NumberFormatException => None
        }
    }
}*/
    
    
  }
  
  
}