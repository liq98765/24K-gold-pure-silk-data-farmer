

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
  
  import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.broadcast.Broadcast

//import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
  
  
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
    //输出推荐的艺术家的名字
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
    //转换重复的艺术家的ID为同一个ID
   /* val trainData = rawUserArtistData.map{
      line =>
        val Array(userId, artistId, count) = line.split(' ').map(_.toInt)
        val finalArtistID = bArtistAlias.value.getOrElse(artistId, artistId)
        Rating(userId, finalArtistID, count)
    }.cache()*/
    
     // 构建排名模型
  def buildRatings(
      rawUserArtistData: RDD[String],
      bArtistAlias: Broadcast[Map[Int,Int]]) = {
    rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      // 根据badID获取goodID，如果根据badID没有对应的goodID，就默认为badID
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      // 构建Rating模型(org.apache.spark.mllib.recommendation._)
      Rating(userID, finalArtistID, count)
    }
  }
    
     // 所有数据构建排名模型(allData: org.apache.spark.rdd.RDD[org.apache.spark.mllib.recommendation.Rating])
    val allData = buildRatings(rawUserArtistData, bArtistAlias)
    
    // 90%用于训练模型，10%用作检验集(def randomSplit(weights: Array[Double], seed: Long): Array[org.apache.spark.rdd.RDD[T]])
    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
     // 将训练集和检验集都缓存在内存
    trainData.cache()
    cvData.cache()
    
    //模型训练
    val model = ALS.trainImplicit(trainData, 3, 5, 0.01, 1.0)
    //模型建立之后，为某个用户给出一个具体的推荐列表
    val recommendations = model.recommendProducts(2093760, 5) //为ID为2093760的用户推荐5个产品
    recommendations.foreach(println)
    val recommendedProductIDs = recommendations.map(_.product).toSet
        
    //评价推荐结果
     // 获取所有去重后的艺术家ID，收集给驱动程序(Driver Program)
    val allItemIDs = allData.map(_.product).distinct().collect()
    
    // 将所有艺术家ID广播到所有Executor
    val bAllItemIDs = sc.broadcast(allItemIDs)


    // mostListenedAUC: 
    //val mostListenedAUC = areaUnderCurve(cvData, bAllItemIDs, predictMostListened(sc, trainData))
    val mostListenedAUC = areaUnderCurve(cvData, bAllItemIDs, model.predict)
    println(mostListenedAUC)
    
    /*严格来说，理解超参数的含义其实不是必须的，但知道这些值的典型范围有助于一个合适的参数空间开始搜索，这个空间不宜太大，也不能太小。
     */ 
    
    val evaluations =
      for (rank   <- Array(10,  50);
           lambda <- Array(1.0, 0.0001);
           alpha  <- Array(1.0, 40.0))
      yield {
        val model = ALS.trainImplicit(trainData, rank, 10, lambda, alpha)
        val auc = areaUnderCurve(cvData, bAllItemIDs, model.predict)
        ((rank, lambda, alpha), auc)
      }


    evaluations.sortBy(_._2).reverse.foreach(println)
    
    
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
  
    def areaUnderCurve(
      positiveData: RDD[Rating],
      bAllItemIDs: Broadcast[Array[Int]],
      predictFunction: (RDD[(Int,Int)] => RDD[Rating])) = {
    // What this actually computes is AUC, per user. The result is actually something
    // that might be called "mean AUC".

    // Take held-out data as the "positive", and map to tuples
    val positiveUserProducts = positiveData.map(r => (r.user, r.product))
    // Make predictions for each of them, including a numeric score, and gather by user
    val positivePredictions = predictFunction(positiveUserProducts).groupBy(_.user)

    // BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
    // small AUC problems, and it would be inefficient, when a direct computation is available.

    // Create a set of "negative" products for each user. These are randomly chosen
    // from among all of the other items, excluding those that are "positive" for the user.
    val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions {
      // mapPartitions operates on many (user,positive-items) pairs at once
      userIDAndPosItemIDs => {
        // Init an RNG and the item IDs set once for partition
        val random = new Random()
        val allItemIDs = bAllItemIDs.value
        userIDAndPosItemIDs.map { case (userID, posItemIDs) =>
          val posItemIDSet = posItemIDs.toSet
          val negative = new ArrayBuffer[Int]()
          var i = 0
          // Keep about as many negative examples per user as positive.
          // Duplicates are OK
          while (i < allItemIDs.size && negative.size < posItemIDSet.size) {
            val itemID = allItemIDs(random.nextInt(allItemIDs.size))
            if (!posItemIDSet.contains(itemID)) {
              negative += itemID
            }
            i += 1
          }
          // Result is a collection of (user,negative-item) tuples
          negative.map(itemID => (userID, itemID))
        }
      }
    }.flatMap(t => t)
    // flatMap breaks the collections above down into one big set of tuples

    // Make predictions on the rest:
    val negativePredictions = predictFunction(negativeUserProducts).groupBy(_.user)

    // Join positive and negative by user
    positivePredictions.join(negativePredictions).values.map {
      case (positiveRatings, negativeRatings) =>
        // AUC may be viewed as the probability that a random positive item scores
        // higher than a random negative one. Here the proportion of all positive-negative
        // pairs that are correctly ranked is computed. The result is equal to the AUC metric.
        var correct = 0L
        var total = 0L
        // For each pairing,
        for (positive <- positiveRatings;
             negative <- negativeRatings) {
          // Count the correctly-ranked pairs
          if (positive.rating > negative.rating) {
            correct += 1
          }
          total += 1
        }
        // Return AUC: fraction of pairs ranked correctly
        correct.toDouble / total
    }.mean() // Return mean AUC over users
  }
  
  
}