package gov.dol.edrvs.scala_example


/**
  * Created by eravelly.shiva.kumar on 6/14/2016.
  */

import java.lang.System.setProperty
import java.text.SimpleDateFormat
import org.apache.spark.sql.{DataFrame, SparkSession}

object AggRules {

  def main(args: Array[String]):Unit = {

    val spark = SparkSession.builder().appName("APR-Rules-").master("local[*]").config("spark.sql.warehouse.dir", "C:\\spark-warehouse").getOrCreate()
    System.setProperty("hadoop.home.dir", "C:\\winutils\\")
    // Create an RDD of Person objects and register it as a table.
    val edrvs = spark.read.load("C:\\Users\\Eravelly.Shiva.Kumar\\Desktop\\TestDataFiles\\971-1076-WIOAAdults-8-201781193330140-UAT_5653_v7csv.csv.parquet")
    edrvs.createOrReplaceTempView("edrvs")

    spark.udf.register("convetToDate", (word: String) => {
      val format = new SimpleDateFormat("yyyyMMdd")
      var date = new java.util.Date()
      try {
        date = format.parse(word)
      } catch {
        case e: Exception => date = new SimpleDateFormat("yyyy-MM-dd").parse("1900-01-01")
      }
      new java.sql.Date(date.getTime())
    })
    val beginOfReportPeriod = "19970513"
    val endOfReportPeriod = "20170512"
    val a= spark.sql(s"""select (datediff(convetToDate($endOfReportPeriod),convetToDate($beginOfReportPeriod))) from edrvs """)
a.show()

  }
  def getResult(num: DataFrame, den: DataFrame): Double = {
    var result = 0.0
    if (!num.rdd.isEmpty && !den.rdd.isEmpty) {
      result = if (den.collect.apply(0).get(0).toString.toDouble != 0) (num.collect.apply(0).get(0).toString.toDouble / den.collect.apply(0).get(0).toString.toDouble) else 0.0;
    }
    return result
  }

}