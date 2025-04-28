package structured_streaming

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf).master("local[*]").getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 创建一个流式DataFrame，这里从Socket读取数据
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // 对流式DataFrame进行转换，将每一行数据切分成单词
//    val words = lines.as[String].flatMap(_.split(" "))

    // 对单词进行分组和聚合
//    val wordCounts = words.groupBy("value").count()

    lines.createOrReplaceTempView("words")
    val wordCounts = spark.sql(
      s"""
         |select
         |    word,
         |    count(1) as cnt
         |from words
         |lateral view explode(split(value, ' ')) as word
         |group by 1
         |""".stripMargin)

    // 启动查询, 把结果打印到控制台
    val query = wordCounts.writeStream
      .outputMode("complete") // 或者选择其他适合的输出模式
      .format("console")
      .start()

    // 等待应用程序终止
    query.awaitTermination()

    //关闭 Spark
    spark.stop()
  }
}
