package gov.dol.edrvs.scala_example


/**
  * Created by eravelly.shiva.kumar on 6/14/2016.
  */
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
object PseudoColumn {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("APR-Rules-").master("local[*]").config("spark.sql.warehouse.dir", "C:\\spark-warehouse").getOrCreate()
    System.setProperty("hadoop.home.dir", "C:\\winutils\\")
    // Create an RDD of Person objects and register it as a table.
    val edrvs = spark.read.load("C:\\Users\\Eravelly.Shiva.Kumar\\Desktop\\TestDataFiles\\3798-6673-DWG-8-2017810152340172-06_PIRL_WIOA_DWG_20164_20170810030135csv.csv.parquet")
    edrvs.createOrReplaceTempView("edrvs")
    val format: String => String = _.replaceAll("-","").substring(0,7)
    val formattedUDF = udf(format)

    val edrvs1=edrvs.withColumn("DwgGrantNumberFormatted",formattedUDF(col("DwgGrantNumber")))
    edrvs1.createOrReplaceTempView("edrvs1")
    edrvs.show()
    var c010_num = spark.sql(s"""select distinct DwgGrantNumberFormatted from edrvs1 where DwgGrantNumberFormatted like 'EM%' or DwgGrantNumberFormatted like'DW%'""")
    c010_num.show()
    var c011_num = spark.sql(s"""select distinct SpecialProjectId1 from edrvs1  where SpecialProjectId1 like 'EM%' or SpecialProjectId1 like'DW%'""")
    c011_num.show()
    var c012_num = spark.sql(s"""select distinct SpecialProjectId2 from edrvs1  where SpecialProjectId2 like 'EM%' or SpecialProjectId2 like'DW%'""")
    c011_num.show()
    var c013_num = spark.sql(s"""select distinct SpecialProjectId3 from edrvs1  where SpecialProjectId3 like 'EM%' or SpecialProjectId3 like'DW%'""")
    c011_num.show()
    c010_num.union(c011_num).union(c012_num).union(c013_num).distinct().show()

  }

}