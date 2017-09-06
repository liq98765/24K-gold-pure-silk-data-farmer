package ch05

object RunKMeans {
  import org.apache.spark.mllib.linalg._
  import org.apache.spark.mllib.regression._
  import org.apache.spark._
  import SparkContext._
  import scala.util.Properties

  import org.apache.spark.mllib.evaluation._
  import org.apache.spark.mllib.clustering._
  import org.apache.spark.rdd._
  def main(args: Array[String]) 
  {
    
    val conf = new SparkConf().setMaster("local").setAppName("RunKMeans")
  val sc = new SparkContext(conf)
  val rawData = sc.textFile("D:/Spark/eclipse/workplace/RunKMeans/src/kddcup.data/kddcup.data.corrected")
  rawData.map(_.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)
  val labelsAndData = rawData.map { line =>
    val buffer = line.split(',').toBuffer
    buffer.remove(1, 3)
    val label = buffer.remove(buffer.length-1)
    val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
    (label,vector)
}
  val data = labelsAndData.values.cache()
  
  val kmeans = new KMeans()
  val model = kmeans.run(data)
  model.clusterCenters.foreach(println)
  //计算两点距离函数：
  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).
        map(p => p._1 - p._2).map(d => d * d).sum)
    //计算数据点到簇质心距离函数：    
    def distToCentroid(datum: Vector, model: KMeansModel) = {
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid, datum)
    }
  //给定k值的模型的平均质心距离函数：
  def clusteringScore(data: RDD[Vector], k: Int) = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
}
  
  //(5 to 40 by 5).map(k => (k, clusteringScore(data, k))).foreach(println)
  
  


/*特征的规范化*/
  val dataAsArray = data.map(_.toArray)
  val numCols = dataAsArray.first().length
  val n = dataAsArray.count()
  val sums = dataAsArray.reduce(
   (a,b) => a.zip(b).map(t => t._1 + t._2)) 
  val sumSquares = dataAsArray.fold(
    new Array[Double](numCols)    
    )(
       (a,b) => a.zip(b).map(t => t._1 + t._2*t._2)    
      )
  val stdevs = sumSquares.zip(sums).map{
  case(sumSq,sum) => math.sqrt(n*sumSq - sum*sum)/n  
  } 
  val means = sums.map(_ / n)
  def normalize(datum: Vector) = {
   val normalizedArray = (datum.toArray,means,stdevs).zipped.map(
       (value,mean,stdev)=>
         if(stdev<=0)(value - mean)else(value - mean)/stdev
         )
         Vectors.dense(normalizedArray)
   }
  val normalizedData = data.map(normalize).cache()
  /*特征的规范化*/
  /*聚类实战*/
  val distances1 = normalizedData.map(datum=>distToCentroid(datum,model))
  val threshold = distances1.top(100).last
  //聚类实战
  
  /**
    * 欧氏距离公式应用到model中
    * KMeansModel.predict方法中调用了KMeans对象的findCloest方法
    * @param datum
    * @param model
    * @return
    */
  def distToCenter(datum: Vector, model: KMeansModel) = {
    //预测样本datum的分类cluster
    val cluster = model.predict(datum)
    //计算质心
    val center = model.clusterCenters(cluster)
    //应用距离公式
    distance(center, datum)
  }

  /**
    *
    * @param data
    * @return
    */
  def buildNormalizationFunction(data: RDD[Vector]): (Vector => Vector) = {
    //将数组缓冲为Array
    val dataAsArray = data.map(_.toArray)
    //数据集第一个元素的长度
    val numCols = dataAsArray.first().length
    //返回数据集的元素个数
    val n = dataAsArray.count()
    //两个数组对应元素相加求和
    val sums = dataAsArray.reduce((a, b) => a.zip(b).map(t => t._1 + t._2))
    //将RDD聚合后进行求平方和操作
    val sumSquares = dataAsArray.aggregate(new Array[Double](numCols))(
      (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2),
      (a, b) => a.zip(b).map(t => t._1 + t._2)
    )

    /* zip函数将传进来的两个参数中相应位置上的元素组成一个pair数组。
      * 如果其中一个参数元素比较长，那么多余的参数会被删掉。
      * 个人理解就是让两个数组里面的元素一一对应进行某些操作
      */
    val stdevs = sumSquares.zip(sums).map {
      case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
    }
    val means = sums.map(_ / n)

    (datum : Vector) => {
      val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
        (value, mean, stdev) =>
          if(stdev <= 0) (value- mean) else (value - mean) /stdev
      )
      Vectors.dense(normalizedArray)
    }
  }
  //* 基于one-hot编码实现类别型变量替换逻辑
   //* @param rawData
    //* @return
   //*
  def buildCategoricalAndLabelFunction(rawData: RDD[String]): (String => (String, Vector))  = {
    val splitData = rawData.map(_.split(","))
    //建立三个特征
    val protocols = splitData.map(_(1)).distinct().collect().zipWithIndex.toMap   //特征值是1，0，0
    val services = splitData.map(_(2)).distinct().collect().zipWithIndex.toMap    //特征值是0，1，0
    val tcpStates = splitData.map(_(3)).distinct().collect().zipWithIndex.toMap   //特征值是0，0，1
    //
    (line: String) => {
      val buffer = line.split(",").toBuffer
      val protocol = buffer.remove(1)
      val service = buffer.remove(1)
      val tcpState = buffer.remove(1)
      val label = buffer.remove(buffer.length - 1)
      val vector = buffer.map(_.toDouble)

      val newProtocolFeatures = new Array[Double](protocols.size)
      newProtocolFeatures(protocols(protocol)) = 1.0
      val newServiceFeatures = new Array[Double](services.size)
      newServiceFeatures(services(service)) = 1.0
      val newTcpStateFeatures = new Array[Double](tcpStates.size)
      newTcpStateFeatures(tcpStates(tcpState)) = 1.0

      vector.insertAll(1, newTcpStateFeatures)
      vector.insertAll(1, newServiceFeatures)
      vector.insertAll(1, newProtocolFeatures)

      (label, Vectors.dense(vector.toArray))
    }
  }
  
  //Detect anomalies(发现异常)
  def bulidAnomalyDetector(data: RDD[Vector], normalizeFunction: (Vector => Vector)): (Vector => Boolean) = {
    val normalizedData = data.map(normalizeFunction)
    normalizedData.cache()

    val kmeans = new KMeans()
    kmeans.setK(150)
    
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedData)

    normalizedData.unpersist()

    //度量新数据点到最近簇质心的距离
    val distances = normalizedData.map(datum => distToCenter(datum, model))
    //设置阀值为已知数据中离中心点最远的第100个点到中心的距离
    val threshold = distances.top(100).last

    //检测，若超过该阀值就为异常点
    (datum: Vector) => distToCenter(normalizeFunction(datum), model) > threshold
  }

  val parseFunction = buildCategoricalAndLabelFunction(rawData)
  val originalAndData = rawData.map(line => (line, parseFunction(line)._2))
    val datarealize = originalAndData.values
    val normalizeFunction = buildNormalizationFunction(datarealize)
    val anomalyDetector = bulidAnomalyDetector(datarealize, normalizeFunction)
    val anomalies = originalAndData.filter {
      case (original, datum) => anomalyDetector(datum)
    }.keys
    //取10个异常点打印出来
    anomalies.take(10).foreach(println)
  
  /*聚类实战*/
  }
  
}