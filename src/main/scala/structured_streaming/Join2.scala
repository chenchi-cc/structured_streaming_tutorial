package structured_streaming

import org.apache.log4j.{LogManager, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

/**
 * 双流关联
 *
 * Spark 2.3 开始支持 stream-stream join，使用时 Spark 会自动维护两个流的状态，以保障后续流入的数据能够和之前流入的数据发生 join 操作，
 * 但这会导致状态无限增长。因此，在对两个流进行 join 操作时，依然可以用 watermark 机制来消除过期的状态，避免状态无限增长。
 *
 * 1.对 2 个流式数据进行 join 操作，输出模式仅支持 append 模式。
 * 2.内连接是否使用 watermark 均可，但外连接必须使用 watermark
 *
 * 注意：落在watermark边界上的数据join不上
 */
object Join2 {
  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    val logger = LogManager.getLogger("StructuredStreamingWatermark")
    // 日期格式化器
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("Asia/Shanghai"))


    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf).master("local[*]").getOrCreate()

    // 添加监听器
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val watermarkUTC = event.progress.eventTime.get("watermark")
        if (watermarkUTC != null) {
          val watermarkBeijing = formatter.format(Instant.parse(watermarkUTC))
          logger.warn(s"流ID: ${event.progress.id}, 流名称: ${event.progress.name}, 北京时间水位线: $watermarkBeijing")
        }
      }

      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println(s"流已启动，流ID: ${event.id}, 流名称: ${event.name}")
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println(s"流已终止，流ID: ${event.id}")
      }
    })

    // 导入隐式转换
    import spark.implicits._

    // 第 1 个 stream
    /**
     * hangge,male,2023-09-05 10:41:00
     * tom,male,2023-09-05 10:42:00
     * lili,female,2023-09-05 10:43:00
     * jerry,female,2023-09-05 10:31:00
     * jerry,female,2023-09-05 10:41:00
     * jerry,female,2023-09-05 10:42:00
     * jerry,female,2023-09-05 10:42:01
     * jerry,female,2023-09-05 10:43:00
     * jerry,female,2023-09-05 10:43:01
     * jerry,female,2023-09-05 10:44:00
     * jerry,female,2023-09-05 10:45:00
     * jerry,female,2023-09-05 10:55:00
     */
    val nameSexStream: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load
      .as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0), arr(1), Timestamp.valueOf(arr(2)))
      }).toDF("name", "sex", "ts1")
      .withWatermark("ts1", "1 minutes")
    nameSexStream.createTempView("name_sex_tab")

    // 第 2 个 stream
    /**
     * hangge,100,2023-09-05 10:43:00
     * jerry,1,2023-09-05 10:44:00
     * jerry,1,2023-09-05 10:56:00
     * lili,33,2023-09-05 10:45:00
     */
    val nameAgeStream: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 8888)
      .load
      .as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0), arr(1).toInt, Timestamp.valueOf(arr(2)))
      }).toDF("name", "age", "ts2")
      .withWatermark("ts2", "1 minutes")
    nameAgeStream.createTempView("name_age_tab")


    // join 操作
    // Stream-stream LeftOuter join between two streaming DataFrame/Datasets is not supported without a watermark in the join keys, or a watermark on the nullable side and an appropriate range condition;
    val joinResult: DataFrame = spark.sql(
      s"""
         |select
         |  T1.name,
         |  T1.sex,
         |  T1.ts1,
         |  T2.name,
         |  T2.age,
         |  T2.ts2
         |from name_sex_tab T1
         |join
         |name_age_tab T2 on T1.name = T2.name and ts2 >= ts1 and ts2 <= ts1 + interval 10 minutes
         |""".stripMargin)

    // 输出
    // Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets;
    // Join between two streaming DataFrames/Datasets is not supported in Update output mode, only in Append output mode;
    val query = joinResult.writeStream.queryName("join_stream")
      .outputMode("append")
      .format("console")
      .start()

    // 等待应用程序终止
    query.awaitTermination()

    spark.stop()
  }
}
