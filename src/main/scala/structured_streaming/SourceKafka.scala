package structured_streaming

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SourceKafka {
  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf).master("local[*]").getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 创建一个流式DataFrame，这里从Kafka中读取数据
    val lines: DataFrame = spark.readStream
      .format("kafka") // 设置 kafka 数据源
      .option("kafka.bootstrap.servers", "kafka1:9093,kafka2:9094")
      .option("subscribe", "my-topic") // 也可以订阅多个主题 "topic1,topic2"
      .load

    val wordCounts = lines
      .select("value").as[String] // 将每条消息的 value 取出（不需要key）
      .flatMap(_.split("\\W+")) // 拆分出单词
      .groupBy("value") // 分组
      .count()

    // 启动查询, 把结果打印到控制台
    val query = wordCounts.writeStream
      .outputMode("update") // 使用update输出模式
      .format("console")
      .start()

    // 等待应用程序终止
    query.awaitTermination()

    spark.stop()
  }
}
