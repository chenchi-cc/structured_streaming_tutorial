package structured_streaming

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

/**
 * watermark主要解决两个问题：
 * 1.处理聚合中的延迟数据
 * 2.减少内存中状态数据
 *
 * 在输出模式是 append 时，必须设置 watermark 才能使用聚合操作。其实，watermark 定义了 append 模式中何时输出聚合结果（状态），并清理过期状态。
 * 在输出模式是 update 时，watermark 主要用于过滤过期数据并及时清理过期状态。
 * 不能是 complete 模式，因为 complete 的时候（必须有聚合），要求每次输出所有的聚合结果。我们使用 watermark 的目的是丢弃一些过时聚合数据，所以 complete 模式使用 watermark 无效也无意义。
 *
 *
 * 可以通过 withWatermark() 来定义 watermark。
 * watermark 计算：watermark = MaxEventTime - Threshhod。
 * 注意：
 * 1.withWatermark 必须使用与聚合操作中的时间戳列是同一列 .df.withWatermark("time", "1 minutes").groupBy("time2").count() 无效
 * 2.withWatermark 必须在聚合之前调用 .df.groupBy("time").count().withWatermark("time", "1 minutes") 无效
 */
object Watermark {
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

    /**
     * 2024-11-13 10:00:59,hello
     * 2024-11-13 10:01:00,hello
     * 2024-11-13 10:02:30,world
     * 2024-11-13 10:03:59,hello
     * 2024-11-13 10:04:00,hello
     * 2024-11-13 10:06:00,world
     * 2024-11-13 10:08:00,spark
     * 2024-11-13 10:09:59,spark
     * 2024-11-13 10:10:00,spark
     *
     * Event time must be defined on a window or a timestamp, but event_time is of type string;
     * 所以这里改成timestamp类型
     */
    lines.as[String]
      .map { line =>
        val Array(event_time, word) = line.split(",")
        (Timestamp.valueOf(event_time.trim), word.trim)
      }
      .toDF("event_time", "word")
      .withWatermark("event_time", "1 minutes") // 添加水印,参数(event_time列名，延迟时间上限）
      .createOrReplaceTempView("words")

    // complete模式可以用order by排序
    // update模式不能用order by排序，但是可以聚合
    // append模式必须要watermark
    val wordCounts: DataFrame = spark.sql(
      s"""
         |select
         |    window(event_time, '1 minutes') as window,
         |    count(1) as pv,
         |    approx_count_distinct(word) as uv
         |from words
         |group by 1
         |--order by 1
         |--Sorting is not supported on streaming DataFrames/Datasets, unless it is on aggregated DataFrame/Dataset in Complete output mode
         |--Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark
         |""".stripMargin)

    // 启动查询, 把结果打印到控制台
    // 在 update 模式下，仅输出与之前批次的结果相比，涉及更新或新增的数据。
    // 在 append 模式下，仅输出新增的数据，且输出后的数据无法变更。
    val query = wordCounts.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
      .option("truncate", "false") // 不截断.为了在控制台能看到完整信息, 最好设置为 false
      .start()

    // 等待应用程序终止
    query.awaitTermination()

    spark.stop()
  }
}
