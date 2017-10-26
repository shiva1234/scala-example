package gov.dol.edrvs.scala_example

import java.text.SimpleDateFormat

import gov.dol.edrvs.scala_example.Mapping.getConvertedDate
import gov.dol.edrvs.scala_example.TestCSV.Person
import org.apache.spark.sql.{DataFrame, SparkSession}
object UDFS {
  var result =""
  //case class Person(name: String, age: Int)
  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder().appName("APR-Rules-").master("local[*]").config("spark.sql.warehouse.dir","C:\\spark-warehouse").getOrCreate()
    System.setProperty("hadoop.home.dir","C:\\winutils\\")
    val edrvs = spark.read.load("C:\\Users\\Eravelly.Shiva.Kumar\\Desktop\\TestDataFiles\\3798-6673-DWG-8-2017810152340172-06_PIRL_WIOA_DWG_20164_20170810030135csv.csv.parquet")
    edrvs.createOrReplaceTempView("edrvs")
    getConvertedDate(spark)
    val beginOfReportPeriod="20160701"
    val endOfReportPeriod="20170630"

    var c66_2_num = spark.sql(s"""select count( UniqueIndividualIdentifierWIOA) from edrvs where ( ReceivedTrainingWIOA = 1) and ((EmployedIn2ndQuarterAfterExitQuarterWIOA IN (1,2,3,4,5,6,7,8))) and (convetToDate(DateOfProgramExitWIOA) >= convetToDate($beginOfReportPeriod) and convetToDate(DateOfProgramExitWIOA) <= convetToDate($endOfReportPeriod)) and (OtherReasonsForExitWIOA = 00)""")
    var c66_2_den = spark.sql(s"""select count( UniqueIndividualIdentifierWIOA) from edrvs where ( ReceivedTrainingWIOA = 1) and (convetToDate(DateOfProgramExitWIOA) >= convetToDate($beginOfReportPeriod) and convetToDate(DateOfProgramExitWIOA) <= convetToDate($endOfReportPeriod)) and (OtherReasonsForExitWIOA = 00)""")
    appendValues(c66_2_num)
    appendValues(c66_2_den)
    println(result)

  }
  def appendValues(df: DataFrame) = {
    if (!df.rdd.isEmpty) {
      result+=","+df.collect.apply(0).get(0).toString
    }
  }
}
