package gov.dol.edrvs.scala_example


/**
  * Created by eravelly.shiva.kumar on 6/14/2016.
  */

import java.lang.System.setProperty
import java.text.SimpleDateFormat
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object AggTest {

  def main(args: Array[String]): Unit = {

    //val sc = new SparkContext(new SparkConf().setAppName("sql").setMaster("local"))
    val spark = SparkSession.builder().appName("APR-Rules-").master("local[*]").config("spark.sql.warehouse.dir", "C:\\spark-warehouse").getOrCreate()
    val sql = new HiveContext(spark.sparkContext)
    System.setProperty("hadoop.home.dir", "C:\\winutils\\")
    // Create an RDD of Person objects and register it as a table.
    val edrvs = spark.read.load("C:\\Users\\Eravelly.Shiva.Kumar\\Desktop\\TestDataFiles\\3798-6673-DWG-8-2017810152340172-06_PIRL_WIOA_DWG_20164_20170810030135csv.csv.parquet")
    edrvs.createOrReplaceTempView("edrvs")
    spark.udf.register("gm", new GeometricMean)
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
    val beginOfReportPeriod = "20160701"
    val endOfReportPeriod = "20170630"
    //var programsIncluded="(DislocatedWorkerWIOA in (1,2,3) or AdultWIOA in (1,2,3) or AdultEducationWIOA = 1 or VocationalRehabilitationWIOA in (1,2,3) or WagnerPeyserEmploymentServiceWIOA=1)"
    var programsIncluded="(DislocatedWorkerWIOA in (1,2,3) or YouthWIOA in (1,2,3) or AdultEducationWIOA = 1 or VocationalRehabilitationWIOA in (1,2,3) or AdultWIOA in (1,2,3)) "
    var c010_num = sql.sql(s"""select DwgGrantNumber, percentile_approx(EmployedIn2ndQuarterAfterExitQuarterWIOA,0.5)
    				from edrvs
    				where Wages2NdQuarterAfterExitQuarterWIOA>0 and Wages2NdQuarterAfterExitQuarterWIOA< 999999.99
    				and length(Wages2NdQuarterAfterExitQuarterWIOA)>0 and (length(DateOfProgramEntryWIOA)>0)
    				and ((convetToDate(DateOfProgramExitWIOA) <= convetToDate($endOfReportPeriod)
    				AND convetToDate(DateOfProgramExitWIOA) >= convetToDate($beginOfReportPeriod)) and length(DateOfProgramExitWIOA)>0)
    				and (OtherReasonsForExitWIOA = 00) group by DwgGrantNumber """)
    c010_num.show()


    def getResult(num: DataFrame, den: DataFrame): Double = {
      var result = 0.0
      if (!num.rdd.isEmpty && !den.rdd.isEmpty) {
        result = if (den.collect.apply(0).get(0).toString.toDouble != 0) (num.collect.apply(0).get(0).toString.toDouble / den.collect.apply(0).get(0).toString.toDouble) else 0.0;
      }
      return result
    }

  }
}