package structured_streaming

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 流批关联
 */
object Join {
  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf).master("local[*]").getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 静态 df
    val arr = Array(("hangge", "male"), ("lili", "female"), ("tom", "male"));
    val staticDF: DataFrame = spark.sparkContext.parallelize(arr).toDF("name", "sex")
    staticDF.createOrReplaceTempView("staticDF")

    // 流式 df
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    /**
     * tom,12
     * hangge,99
     * jerry,10
     */
    val streamDF: DataFrame = lines.as[String].map(line => {
      val arr = line.split(",")
      (arr(0), arr(1).toInt)
    }).toDF("name", "age")
    streamDF.createOrReplaceTempView("streamDF")


    // join
    val joinResult: DataFrame = spark.sql(
      s"""
         |select
         |  T1.name,
         |  T1.age,
         |  T2.sex
         |from streamDF T1
         |left outer join
         |staticDF T2 on T1.name = T2.name
         |""".stripMargin)

    // 输出
    val query = joinResult.writeStream
      .outputMode("append")
      .format("console")
      .start()

    // 等待应用程序终止
    query.awaitTermination()

    spark.stop()
  }
}
