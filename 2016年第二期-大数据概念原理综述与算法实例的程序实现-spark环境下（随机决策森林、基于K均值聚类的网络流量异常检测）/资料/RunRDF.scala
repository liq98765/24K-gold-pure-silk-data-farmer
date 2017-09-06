package ch04

object RunRDF {
  import org.apache.spark.mllib.linalg._
 import org.apache.spark.mllib.regression._
import org.apache.spark._
import SparkContext._
import scala.util.Properties

import org.apache.spark.mllib.evaluation._
 
import org.apache.spark.mllib.tree._
 //引入决策树模型
import org.apache.spark.mllib.tree.model._
//引入随机决策森林
import org.apache.spark.mllib.tree.model.RandomForestModel

import org.apache.spark.SparkConf 
import org.apache.spark.rdd._

val conf = new SparkConf().setMaster("local").setAppName("RunRDF")
val sc = new SparkContext(conf)



val rawData = sc.textFile("D:/Spark/eclipse/workplace/RunRDF/src/covtype.data")
 
val data = rawData.map { line =>
 
         val values = line.split(',').map(_.toDouble)
 
         val featureVector = Vectors.dense(values.init)
 
         val label = values.last - 1
 
         LabeledPoint(label, featureVector)
 
}
  val Array(trainData, cvData, testData) =data.randomSplit(Array(0.8, 0.1, 0.1))
 
  trainData.cache()
 
  cvData.cache()
 
  testData.cache()

  //定义决策树矩阵
  def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]):
 
                   MulticlassMetrics = {
 
         val predictionsAndLabels = data.map(example =>
 
                   (model.predict(example.features), example.label)
 
         )
 
         new MulticlassMetrics(predictionsAndLabels)
 
}
    //定义随机决策森林 
   def getMetricsforest(model: RandomForestModel, data: RDD[LabeledPoint]):
 
                   MulticlassMetrics = {
 
         val predictionsAndLabels = data.map(example =>
 
                   (model.predict(example.features), example.label)
 
         )
 
         new MulticlassMetrics(predictionsAndLabels)
 
}

def main(args: Array[String]) 
{
  
  val model = DecisionTree.trainClassifier(
 
         trainData, 7, Map[Int,Int](), "gini", 4, 100)
 
val metrics = getMetrics(model, cvData)
 /*println( metrics.confusionMatrix)
 println( metrics.precision)*/
 val evaluations =
 
         for (impurity <- Array("gini", "entropy");
 
                   depth <- Array(1, 20);
 
                   bins <- Array(10, 300))
 
         yield {
 
                   val model = DecisionTree.trainClassifier(
 
                            trainData, 7, Map[Int,Int](), impurity, depth, bins)
 
                   val predictionsAndLabels = cvData.map(example =>
 
                            (model.predict(example.features), example.label)
 
                   )
 
                   val accuracy =
 
                            new MulticlassMetrics(predictionsAndLabels).precision
 
                   ((impurity, depth, bins), accuracy)
 
         }
        
        //随机决策森林
         val forest = RandomForest.trainClassifier(
         trainData, 7, Map(10 -> 4, 11 -> 40), 20,"auto", "entropy", 30, 300)
         val metricsforest = getMetricsforest(forest, cvData);
        //第一棵决策树
         println( metrics.confusionMatrix)
         //查看准确度
        println( metrics.precision)
        //决策树调优
        evaluations.sortBy(_._2).reverse.foreach(println)

       /* val input = "2709,125,28,67,23"
 
        val vector = Vectors.dense(input.split(',').map(_.toDouble))
        /*println( metricsforest.confusionMatrix)
        println( metricsforest.precision)*/
         forest.predict(vector) */
        
}



}