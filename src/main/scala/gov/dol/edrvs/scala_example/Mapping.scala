package gov.dol.edrvs.scala_example


/**
  * Created by eravelly.shiva.kumar on 6/14/2016.
  */

import java.lang.System.setProperty

import scala.util.control.Breaks._
import java.sql.Date
import java.text.SimpleDateFormat
import java.util

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Mapping {
  var result: Map[Any, Any] = Map()
  //var grantCount:Any=null
  //var grantNum:Any=null
  val rows=2
  val cols=2
  var z:Array[String]=null
  val a = Array.ofDim[Any](9, 280)
  a(0)(0)="EM-27583-15-60-A-06"
  a(1)(0)="EM-28098-16-60-A-06"
  a(2)(0)="EM-27344-15-60-A-06"
  a(3)(0)="EM-28115-16-60-A-06"
  a(4)(0)="EM-20451-10-60-A-06"
  a(5)(0)="MI-29684-16-60-A-06"
  a(6)(0)="EM-30387-17-60-A-06"
  a(7)(0)="EM-24449-13-60-A-06"
  a(8)(0)="EM-25889-14-60-A-06"

  val beginOfReportPeriod="20160701"
  val endOfReportPeriod="20170630"
  def main(args: Array[String]):Unit = {

    val spark = SparkSession.builder().appName("APR-Rules-").master("local[*]").config("spark.sql.warehouse.dir","C:\\spark-warehouse").getOrCreate()
    System.setProperty("hadoop.home.dir","C:\\winutils\\")
    val edrvs = spark.read.load("C:\\Users\\Eravelly.Shiva.Kumar\\Desktop\\TestDataFiles\\3798-6673-DWG-8-2017810152340172-06_PIRL_WIOA_DWG_20164_20170810030135csv.csv.parquet")
    edrvs.createOrReplaceTempView("edrvs")
    getConvertedDate(spark)

    if(beginOfReportPeriod.length>0){
      getRule001(spark, beginOfReportPeriod, endOfReportPeriod)
    }
    if(beginOfReportPeriod.length>0){
      getRule002(spark, beginOfReportPeriod, endOfReportPeriod)
    }
    if(beginOfReportPeriod.length>0){
      getRule004(spark, beginOfReportPeriod, endOfReportPeriod)
    }
   /* if(beginOfReportPeriod.length>0){
      getRule003(spark, beginOfReportPeriod, endOfReportPeriod)
    }*/


    def getRule001(spark: SparkSession, beginOfReportPeriod: String, endOfReportPeriod: String) = {
      val list = spark.sql(s"""Select DwgGrantNumber, count(UniqueIndividualIdentifierWIOA) as rule1 from edrvs where NationalDislocatedWorkerGrantsDWG=1 and ((length(MostRecentDateReceivedBasicCareerServicesStaffAssi)>0 AND MostRecentDateReceivedBasicCareerServicesStaffAssi!="\u0000") OR (length(DateOfFirstIndividualizedCareerService)>0 AND DateOfFirstIndividualizedCareerService!="\u0000") OR ReceivedTrainingWIOA = 1) AND ((convetToDate(DateOfProgramEntryWIOA) <= convetToDate($endOfReportPeriod)) AND (convetToDate(DateOfProgramExitWIOA) >= convetToDate($beginOfReportPeriod) OR length(DateOfProgramExitWIOA)=0)) group by DwgGrantNumber""").collect()
      val r = 1
      getListedRecords(list,r)

    }
    //for ((k,v) <- result) printf("key: %s, value: %s\n", k, v)




    def getRule002(sqlContext: SparkSession, beginOfReportPeriod: String, endOfReportPeriod: String) = {
      val list1=sqlContext.sql(s"""Select DwgGrantNumber, count(UniqueIndividualIdentifierWIOA) from edrvs where NationalDislocatedWorkerGrantsDWG=1 and ((length(MostRecentDateReceivedBasicCareerServicesStaffAssi)>0 AND MostRecentDateReceivedBasicCareerServicesStaffAssi!="\u0000") OR (length(DateOfFirstIndividualizedCareerService)>0 AND DateOfFirstIndividualizedCareerService!="\u0000") OR ReceivedTrainingWIOA = 1) AND ((convetToDate(DateOfProgramEntryWIOA) <= convetToDate($endOfReportPeriod)) AND (convetToDate(DateOfProgramExitWIOA) >= convetToDate($beginOfReportPeriod) OR length(DateOfProgramExitWIOA)=0)) group by DwgGrantNumber""").collect()
      val r = 2
      getListedRecords(list1,r)
    }

   /* def getRule003(sqlContext: SparkSession, beginOfReportPeriod: String, endOfReportPeriod: String) = {
      val list=sqlContext.sql(s"""Select count(UniqueIndividualIdentifierWIOA), DwgGrantNumber from edrvs where NationalDislocatedWorkerGrantsDWG=1 and ((length(MostRecentDateReceivedBasicCareerServicesStaffAssi)>0 AND MostRecentDateReceivedBasicCareerServicesStaffAssi!="\u0000") OR (length(DateOfFirstIndividualizedCareerService)>0 AND DateOfFirstIndividualizedCareerService!="\u0000") OR ReceivedTrainingWIOA = 1) AND ((convetToDate(DateOfProgramEntryWIOA) <= convetToDate($endOfReportPeriod)) AND (convetToDate(DateOfProgramExitWIOA) >= convetToDate($beginOfReportPeriod) OR length(DateOfProgramExitWIOA)=0)) group by DwgGrantNumber""").rdd.map(r=>(r.get(0),r.get(1))).collect()
      list.foreach(println)
    }*/
   def getRule004(sqlContext: SparkSession, beginOfReportPeriod: String, endOfReportPeriod: String) = {
     val list1=sqlContext.sql(s"""Select DwgGrantNumber, count(UniqueIndividualIdentifierWIOA) from edrvs where NationalDislocatedWorkerGrantsDWG=1 and ((length(MostRecentDateReceivedBasicCareerServicesStaffAssi)>0 AND MostRecentDateReceivedBasicCareerServicesStaffAssi!="\u0000") OR (length(DateOfFirstIndividualizedCareerService)>0 AND DateOfFirstIndividualizedCareerService!="\u0000") OR ReceivedTrainingWIOA = 1) AND ((convetToDate(DateOfProgramEntryWIOA) <= convetToDate($endOfReportPeriod)) AND (convetToDate(DateOfProgramExitWIOA) >= convetToDate($beginOfReportPeriod) OR length(DateOfProgramExitWIOA)=0)) group by DwgGrantNumber""").collect()
     val list2=sqlContext.sql(s"""Select DwgGrantNumber, count(UniqueIndividualIdentifierWIOA) from edrvs where NationalDislocatedWorkerGrantsDWG=1 and ((length(MostRecentDateReceivedBasicCareerServicesStaffAssi)>0 AND MostRecentDateReceivedBasicCareerServicesStaffAssi!="\u0000") OR (length(DateOfFirstIndividualizedCareerService)>0 AND DateOfFirstIndividualizedCareerService!="\u0000") OR ReceivedTrainingWIOA = 1) AND ((convetToDate(DateOfProgramEntryWIOA) <= convetToDate($endOfReportPeriod)) AND (convetToDate(DateOfProgramExitWIOA) >= convetToDate($beginOfReportPeriod) OR length(DateOfProgramExitWIOA)=0)) group by DwgGrantNumber""").collect()
     val r = 3
     getRate(list1,list2,r)
   }
    for (i <- 0 to 8; j <- 0 to 3) println(a(i)(j))

  }

  def getRate(list1:Array[Row], list2:Array[Row], r:Int)= {
    var grantNum1:Any=null
    var grantCount1:Any=null
    list1.foreach(record => {
      grantNum1 = record(0)
      grantCount1 = record(1)
      // result += (grantNum -> grantCount)
      mapList2(grantNum1, grantCount1, list2, r)
    })
    //a.map { x => (x._1, b.find(y => y._1 == x._1).getOrElse(("None", 0))._2 * 1.0 / x._2) }
  }
  def mapList2(grantNum1:Any,grantCount1:Any,list2:Array[Row], r:Int)={
    var grantNum2:Any=null
    var grantCount2:Any=null
    list2.foreach(record => {
      grantNum2 = record(0)
      grantCount2 = record(1)
      if(grantNum2==grantNum1) {
        val rate = getResult(grantCount1.toString.toDouble, grantCount2.toString.toDouble)
        mapArray(grantNum2, rate, r)
      }
    })
  }
  def getResult(num: Double, den: Double): Double = {
    var result = 0.0
      result = if (den!= 0) (num/den) else 0.0;
    return result
  }
  def getListedRecords(list:Array[Row], r:Int)={
    var grantNum:Any=null
    var grantCount:Any=null
    list.foreach(record => {
      grantNum = record(0)
      grantCount = record(1)
      // result += (grantNum -> grantCount)
      mapArray(grantNum, grantCount, r)
    })
  }
  def mapArray(grantNum:Any,grantCount:Any,r:Int)= {
    val j = 0
    for (i <- 0 to 8 if(a(i)(j)==grantNum))  a(i)(r)=grantCount
  }

  def getConvertedDate(spark: SparkSession) = {
    spark.udf.register("convetToDate", (word: String) => {
      val format = new SimpleDateFormat("yyyyMMdd")
      var date = new util.Date()
      try {
        date = format.parse(word)
      } catch {
        case e: Exception => date = new SimpleDateFormat("yyyy-MM-dd").parse("1900-01-01")
      }
      new Date(date.getTime())
    })
  }
  def getResult(num: DataFrame, den: DataFrame): Double = {
    var result = 0.0
    if (!num.rdd.isEmpty && !den.rdd.isEmpty) {
      result = if (den.collect.apply(0).get(0).toString.toDouble != 0) (num.collect.apply(0).get(0).toString.toDouble / den.collect.apply(0).get(0).toString.toDouble) else 0.0;
    }
    return result
  }


}