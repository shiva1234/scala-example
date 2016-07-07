package gov.dol.edrvs.scala_example

  
/**
  * Created by eravelly.shiva.kumar on 6/14/2016.
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.DataFrame
import java.io.File
import java.sql.Date
import java.util.Calendar

object AggParams {

	def foo(x: Array[String]): Unit = x.foldLeft("")((a, b) => a + b)

case class Person(uniqueindividualidentifier: String, firststaffassisted: String, firstindividualizedcareerservice: String, receivedtraining: Integer, dateofprogramexit: String)

def main(args: Array[String]):Unit = {

		println("Hello World!")
		println("concat arguments = " + foo(args))

		var conf = new SparkConf().setAppName("WordCount").setMaster("local");
		var sc = new SparkContext(conf);

		// sc is an existing SparkContext.
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)

		// this is used to implicitly convert an RDD to a DataFrame.
		import sqlContext.implicits._

		// Create an RDD of Person objects and register it as a table.
		val people = sc.textFile("C:\\Users\\eravelly.shiva.kumar\\Desktop\\people.txt").map(_.split(",")).map(p => Person(p(0).toString, p(1).toString, p(2).toString, p(3).toInt, p(4).toString)).toDF()
		people.registerTempTable("edrvs")

		sqlContext.udf.register("convetToDate", (word: String) => {
			val format = new java.text.SimpleDateFormat("yyyymmdd")
			println(word)
			val date = format.parse(word)
			new Date(date.getTime())
		})   

		
for(a <- 1 to 3){
   var beginOfReportPeriod=""
   var endOfReportPeriod=""
   var reportingPeriod=""
   if(a==1){
    beginOfReportPeriod = beginOfReportPrd(4) 
   endOfReportPeriod =  endOfReportPrd(4)
   reportingPeriod = endOfReportPeriod
   }
   if(a==2){
     beginOfReportPeriod = beginOfReportPrd(5) 
   endOfReportPeriod =  endOfReportPrd(5)
   reportingPeriod = endOfReportPeriod
   }
   if(a==3){
     beginOfReportPeriod = beginOfReportPrd(6) 
   endOfReportPeriod =  endOfReportPrd(6)
   reportingPeriod = endOfReportPeriod
   }
		// SQL statements can be run by using the sql methods provided by sqlContext.
		var teenagers= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where length(firststaffassisted)>0 AND length(firstindividualizedcareerservice)=0 and receivedtraining = 0 and convetToDate(dateofprogramexit) < convetToDate($reportingPeriod)""")
		//val teenagers = sqlContext.sql("SELECT age FROM people WHERE age = 15")
		teenagers.registerTempTable("teenagers")

		teenagers.show()
		 if(a==1){
		   saveDfToCsv(teenagers, "b.csv")
		 }
		if(a==2){
		   saveDfToCsv(teenagers, "b1.csv")
		 }
		if(a==3){
		   saveDfToCsv(teenagers, "b3.csv")
		 }
}
	}


	def saveDfToCsv(df: DataFrame, tsvOutput: String,
			sep: String = ",", header: Boolean = false): Unit = {
		val tmpParquetDir = "C:\\temp\\2"
				df.repartition(1).write.
				format("com.databricks.spark.csv").
				option("header", header.toString).
				option("delimiter", sep).
				save(tmpParquetDir)
				val dir = new File(tmpParquetDir)
		val tmpTsvFile = tmpParquetDir + File.separatorChar + "part-00000"
		(new File(tmpTsvFile)).renameTo(new File(tsvOutput))
		dir.listFiles.foreach( f => f.delete )
		dir.delete
	}

	def beginOfReportPrd(choice: Int): String = choice match {
	case 1 =>  "20150701"
	case 2 =>  "20151001"
	case 3 =>  "20160101"
	case 4 =>  "20160401"
	case 5 =>  "20160701"
	case 6 =>  "20161001"
	case 7 =>  "20170101"
	case 8 =>  "20170401"
	case 9 =>  "20170701"
	case 10 => "20171001"
	case 11 => "20180101"
	case 12 => "20180401"
	case 13 => "20180701"
	case 14 => "20181001"
	case 15 => "20190101"
	case 16 => "20190401"
	case 17 => "20190701"
	case 18 => "20191001"
	case 19 => "20200101"
	case 20 => "20200401"
	case 21 => "20200701"
	case 22 => "20201001"
	case 23 => "20210101"
	case 24 => "20210401"  
	case 25 => "20210701"
	case 26 => "20211001"
	case 27 => "20220101"
	case 28 => "20220401"
	case 29 => "20220701"
	case 30 => "20221001"
	case 31 => "20230101"
	case 32 => "20230401"
	case 33 => "20230701"
	case 34 => "20231001"
	case 35 => "20240101"
	case 36 => "20240401"
	case 37 => "20240701"
	case 38 => "20241001"
	case 39 => "20250101"
	case 40 => "20250401"
	case 41 => "20250701"
	case 42 => "20251001"
	case 43 => "20260101"
	case 44 => "20260401"
	}

	def endOfReportPrd(choice: Int): String = choice match {
	case 1 =>  "20150930"
	case 2 =>  "20151231"
	case 3 =>  "20160331"
	case 4 =>  "20160630"
	case 5 =>  "20160930"
	case 6 =>  "20161231"
	case 7 =>  "20170331"
	case 8 =>  "20170630"
	case 9 =>  "20170930"
	case 10 => "20171231"
	case 11 => "20180331"
	case 12 => "20180630"
	case 13 => "20180930"
	case 14 => "20181231"
	case 15 => "20190331"
	case 16 => "20190630"
	case 17 => "20190930"
	case 18 => "20191231"
	case 19 => "20200331"
	case 20 => "20200630"
	case 21 => "20200930"
	case 22 => "20201231"
	case 23 => "20210331"
	case 24 => "20210630"  
	case 25 => "20210930"
	case 26 => "20211231"
	case 27 => "20220331"
	case 28 => "20220630"
	case 29 => "20220930"
	case 30 => "20221231"
	case 31 => "20230331"
	case 32 => "20230630"
	case 33 => "20230930"
	case 34 => "20231231"
	case 35 => "20240331"
	case 36 => "20240630"
	case 37 => "20240930"
	case 38 => "20241231"
	case 39 => "20250331"
	case 40 => "20250630"
	case 41 => "20250930"
	case 42 => "20251231"
	case 43 => "20260331"
	case 44 => "20260630"
	}
}