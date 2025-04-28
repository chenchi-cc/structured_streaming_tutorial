package structured_streaming

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.sql.Timestamp

/**
 * 给产生的数据自动添加时间
 */
object EventTime {
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
      .option("includeTimestamp", true) // 给产生的数据自动添加时间
      .load()

    // 把行切割成单词, 保留时间戳
    val words: DataFrame = lines.as[(String, Timestamp)]
      .flatMap(line => {
        line._1.split(" ").map((_, line._2))
      })
      .toDF("word", "timestamp")

    // 按照窗口和单词分组, 并且计算每组的单词的个数
    // 1 minute和 1 minutes均可
//    val wordCounts: Dataset[Row] = words.groupBy(
//      // 调用 window 函数, 返回的是一个 Column，参数（df中表示时间戳的列、窗口长度、滑动步长）
//      // 如果我不加滑动不常，就是滚动窗口！！！
//      window($"timestamp", "1 minutes"), $"word"
//    ).count().orderBy($"window") // 计数, 并按照窗口排序

    words.createOrReplaceTempView("words")
    val wordCounts = spark.sql(
      s"""
         |select
         |    window(timestamp, '1 minute') as window,
         |    window(timestamp, '1 minute').start as window_start,
         |    window(timestamp, '1 minute').end as window_end,
         |    collect_set(word) as words_list,
         |    approx_count_distinct(word) as word_count --不能直接用count(distinct word)
         |from words
         |group by 1, 2, 3
         |""".stripMargin)

    // 启动查询, 把结果打印到控制台
    val query = wordCounts.writeStream
      .outputMode("complete") // 使用complete输出模式
      .format("console")
      .option("truncate", "false") // 不截断.为了在控制台能看到完整信息, 最好设置为 false
      .start()

    // 等待应用程序终止
    query.awaitTermination()

    spark.stop()
  }
}
