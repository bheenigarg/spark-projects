package org.spark.test
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCount {
  
  def main(args:Array[String]): Unit = {
    
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)
    
    val reviewRDD = sc.textFile("/Users/bheeni/Downloads/Iphone7rev.txt")
                      .flatMap(line => line.split(" "))
                      .map(word => (word,1))
                      .reduceByKey(_+_)
    reviewRDD.collect.foreach(println)
    reviewRDD.saveAsTextFile("/Users/bheeni/Downloads/WordCountOutput")
  }
}