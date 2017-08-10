package gov.dol.edrvs.scala_example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UDAF {
  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder().appName("APR-Rules-").master("local[*]").config("spark.sql.warehouse.dir","C:\\spark-warehouse").getOrCreate()
    System.setProperty("hadoop.home.dir","C:\\winutils\\")
    val ids = spark.range(1, 20)
    ids.registerTempTable("ids")
    val df = spark.sql("select id, id % 3 as group_id from ids")
    df.registerTempTable("simple")
    df.show()
    spark.udf.register("gm", new GeometricMean)
    val df1 = spark.sql("select group_id, gm(id) from simple group by group_id")
    df1.show()
  }
}
