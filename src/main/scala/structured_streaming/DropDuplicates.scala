package structured_streaming

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp

/**
 * 1.dropDuplicates 方法可以指定一个或多个列作为子集，方法将根据这些列的值来判断行是否重复。如果不指定子集参数，方法将考虑所有列。
 * 2.dropDuplicates 方法不可用在聚合之后，即通过聚合得到的 DataSet/DataFrame 不能调用 dropDuplicates
 * 没有窗口的数据，也支持使用watermark，但是前提是必须有dropDuplicates，不然水位也不起作用
 */
object DropDuplicates {
  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf).master("local[*]").getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 创建一个流式DataFrame，这里从socket中读取数据
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // 解析原始输入数据
    /**
     * 1,hangge,2023-09-14 11:00:00
     * 2,lili,2023-09-14 11:01:00
     * 3,tom,2023-09-14 10:10:00
     * 1,hangge,2023-09-14 10:01:00
     */
    val words: DataFrame = lines.as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0), arr(1), Timestamp.valueOf(arr(2)))
      })
      .toDF("uid", "word", "ts")
      .withWatermark("ts", "1 minutes") // 添加水印
      .dropDuplicates("uid") // 去重重复数据 uid 相同就是重复

    // 查询并输出
    val query = words.writeStream
      .outputMode("append")
      .format("console")
      .start()

    // 等待应用程序终止
    query.awaitTermination()

    spark.stop()
  }
}
