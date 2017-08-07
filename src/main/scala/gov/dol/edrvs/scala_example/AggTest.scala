package gov.dol.edrvs.scala_example


/**
  * Created by eravelly.shiva.kumar on 6/14/2016.
  */

import java.lang.System.setProperty
import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{Row, SQLContext}

object AggTest {

  def main(args: Array[String]):Unit = {

    val spark = SparkSession.builder().appName("APR-Rules-").master("local[*]").config("spark.sql.warehouse.dir","C:\\spark-warehouse").getOrCreate()
    System.setProperty("hadoop.home.dir","C:\\winutils\\")
    // Create an RDD of Person objects and register it as a table.
    val edrvs = spark.read.load("C:\\Users\\eravelly.shiva.kumar\\Desktop\\2463-4476-JVSG-7-2017515165916337-RPTQLY_PIRLMastercsv.csv.parquet")
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

    val endOfReportPeriod="20160930"
    val beginOfReportPeriod="20160701"

    var c00 = spark.sql(s"""select count( UniqueIndividualIdentifierWIOA) from edrvs where ((length(MostRecentDateReceivedBasicCareerServicesStaffAssi)>0 AND MostRecentDateReceivedBasicCareerServicesStaffAssi!="\u0000") AND length(DateOfFirstIndividualizedCareerService)=0 and ((ReceivedTrainingWIOA = 0 or length(ReceivedTrainingWIOA)=0))) AND convetToDate(DateOfProgramExitWIOA) <= convetToDate($endOfReportPeriod) AND convetToDate(DateOfProgramExitWIOA) >= convetToDate($beginOfReportPeriod)""")
    c00.show()

  }

}