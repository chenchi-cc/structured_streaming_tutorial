package structured_streaming

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.sql.Timestamp

/**
 * 使用日志中自带的时间
 */
object EventTime2 {
  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf).master("local[*]").getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 创建一个流式DataFrame，这里从socket中读取数据
    val lines: DataFrame = spark.readStream
      .format("socket") // 设置数据源
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // 解析原始输入数据，假设原始数据格式为"单词1 单词2 单词3,事件时间"
//    val wordsWithTimestamp: DataFrame = lines.as[String]
//      .map(line => {
//        val parts = line.split(",")
//        val words = parts(0).split(" ")
//        val timestamp = Timestamp.valueOf(parts(1).trim())
//        (words, timestamp)
//      })
//      .flatMap { case (words, timestamp) =>
//        words.map(word => (word, timestamp))
//      }
//      .toDF("word", "timestamp")
//
//    // 按照窗口和单词分组, 并且计算每组的单词的个数
//    val wordCounts: Dataset[Row] = wordsWithTimestamp.groupBy(
//      // 调用 window 函数, 返回的是一个 Column，参数（df中表示时间戳的列、窗口长度、滑动步长）
//      window($"timestamp", "10 minutes", "5 minutes"), $"word"
//    ).count().orderBy($"window") // 计数, 并按照窗口排序

    /**
     * 2024-11-13 10:01:00,hello
     * 2024-11-13 10:02:30,world
     * 2024-11-13 10:03:45,hello
     * 2024-11-13 10:06:00,world
     */
    lines.as[String]
      .map { line =>
        val Array(event_time, word) = line.split(",")
        (event_time.trim, word.trim)
      }
      .toDF("event_time", "word")
      .createOrReplaceTempView("words")

    // complete模式可以用order by排序
    // update模式不能用order by排序，但是可以聚合
    // append模式必须要watermark
    val wordCounts: DataFrame = spark.sql(
      s"""
         |select
         |    window(event_time, '2 minute') as window,
         |    count(1) as pv,
         |    approx_count_distinct(word) as uv
         |from words
         |group by 1
         |order by 1
         |--Sorting is not supported on streaming DataFrames/Datasets, unless it is on aggregated DataFrame/Dataset in Complete output mode
         |--Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark
         |""".stripMargin)

    // 启动查询, 把结果打印到控制台
    val query = wordCounts.writeStream
      .outputMode("update") // 使用complete输出模式
      .format("console")
      .option("truncate", "false") // 不截断.为了在控制台能看到完整信息, 最好设置为 false
      .start()

    // 等待应用程序终止
    query.awaitTermination()

    spark.stop()
  }
}
