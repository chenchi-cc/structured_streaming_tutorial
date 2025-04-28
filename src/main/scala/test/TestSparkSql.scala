package test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestSparkSql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TestSparkSql")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    spark.sql(
      s"""
         |SELECT
         |    region,
         |    id,
         |    sum(val) as sum_val
         |from
         |(
         |    select 1 as id, 10 as region, 10 as val
         |    union all
         |    select 1 as id, 10 as region, 10 as val
         |    union all
         |    select 2 as id, 10 as region, 10 as val
         |    union all
         |    select 2 as id, 20 as region, 10 as val
         |    union all
         |    select 3 as id, 20 as region, 10 as val
         |    union all
         |    select 3 as id, 20 as region, 10 as val
         |    union all
         |    select 4 as id, 30 as region, 10 as val
         |    union all
         |    select 4 as id, 30 as region, 10 as val
         |    union all
         |    select 5 as id, 30 as region, 10 as val
         |    union all
         |    select 5 as id, 40 as region, 10 as val
         |    union all
         |    select 6 as id, 40 as region, 10 as val
         |) T
         |group by id, region
         |""".stripMargin).show(200, false)

    spark.stop()
  }
}
