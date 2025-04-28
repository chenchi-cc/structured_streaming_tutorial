package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    // 创建 Spark 运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Hello")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD（通过集合）
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // 对数据进行转换
    val rdd2: RDD[Int] = rdd1.map(_ + 1)
    // 打印结果
    rdd2.collect().foreach(println)
    //关闭 Spark
    sc.stop()
  }
}
