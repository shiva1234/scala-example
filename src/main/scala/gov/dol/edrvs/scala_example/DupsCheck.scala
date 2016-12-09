package gov.dol.edrvs.scala_example

  
/**
  * Created by eravelly.shiva.kumar on 6/14/2016.
  */

import org.apache.commons.httpclient.util.DateUtil
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.DataFrame
import java.io.File
import java.sql.Date
import java.util.Calendar
import java.util.Calendar

object DupsCheck {

	def foo(x: Array[String]): Unit = x.foldLeft("")((a, b) => a + b)

case class User(id: String, dateofprogramentry: String, coveredpersonentry: String,
		employmentservice: String, dateofprogramexit: String)

		def main(args: Array[String]):Unit = {

		println("Hello World!")
		println("concat arguments = " + foo(args))

		var conf = new SparkConf().setAppName("WordCount").setMaster("local")
		var sc = new SparkContext(conf)

		// sc is an existing SparkContext.
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)

		// this is used to implicitly convert an RDD to a DataFrame.
		import sqlContext.implicits._

		// Create an RDD of Person objects and register it as a table.
		val people = sc.textFile("C:\\DataServices\\stream3.csv").map(_.split(",")).map(w => User(w(0).toString, w(1).toString, w(2).toString, w(3).toString, w(4).toString)).toDF()
		people.registerTempTable("edrvs")

		sqlContext.udf.register("addDays", (word: String) => {
			val format = new java.text.SimpleDateFormat("MM/dd/yyyy")
			val date = format.parse(word)
      println("added date" +word)
      println("converted date" +new Date(date.getTime()))
      println("added date" +new Date(date.getTime() + 1*24*60*60*1000))
      new Date(date.getTime() + 1*24*60*60*1000)

		})
		sqlContext.udf.register("convetToDate", (word: String) => {
			val format = new java.text.SimpleDateFormat("MM/dd/yyyy")
			val date = format.parse(word)
			new Date(date.getTime())
		})
      //will fetch repeated UIDS
      val uIDDups = sqlContext.sql(
        """select
          |uniqueindividualidentifier
          |from wioaips1
          |where length(uniqueindividualidentifier)>0
          |group by uniqueindividualidentifier
          |having count(uniqueindividualidentifier)>1""".stripMargin)
      uIDDups.registerTempTable("uIDDups")//df with repeaed UID column
      //will fetch whole schema with repeated UID's by joining with uIDDups df
      val uIDDupsTable = sqlContext.sql(
        """select a.*
          |from wioaips1 a, uIDDups b
          |where a.uniqueindividualidentifier=b.uniqueindividualidentifier""".stripMargin)
      uIDDupsTable.registerTempTable("uIDDupsTable")//whole df with repeated UID's
      uIDDupsTable.show()

      //df for null checks on &dateofprogramentry and &dateofprogramexit pipelined to dups2
      val preDups2 = sqlContext.sql(
        """select *
          |from uIDDupsTable
          |where length(dateofprogramentry)>0 and length(dateofprogramexit)>0""".stripMargin)
      preDups2.registerTempTable("preDups2")
      preDups2.show()//df filtered on empty dateofprogramentry and dateofprogramexit

      //df for null checks on &dateofprogramentry pipelined to dups3
      val preDups3 = sqlContext.sql(
        """select *
          |from uIDDupsTable
          |where length(dateofprogramentry)>0""".stripMargin)
      preDups3.registerTempTable("preDups3")
      preDups3.show()//df filtered on empty dateofprogramentry

      //Duplicate check 1
      var dups1 = sqlContext.sql(
        """SELECT
          |uniqueindividualidentifier,
          |coveredpersonentry
          |from uIDDupsTable
          |where length(coveredpersonentry)>0
          |group by uniqueindividualidentifier, coveredpersonentry
          |having count(uniqueindividualidentifier) > 1 and count(coveredpersonentry) > 1""".stripMargin)
      dups1.registerTempTable("dups1")
      dups1.show()

      //Duplicate Check 2
      var dups2 = sqlContext.sql(
        """SELECT
          |distinct a.uniqueindividualidentifier,
          |a.dateofprogramentry,
          |a.dateofprogramexit
          |FROM preDups2 a, preDups2 b
          |WHERE a.uniqueindividualidentifier = b.uniqueindividualidentifier AND a.rownum<>b.rownum
          |AND (convetToDate(a.dateofprogramentry) BETWEEN convetToDate(b.dateofprogramentry) AND date_add(convetToDate(a.dateofprogramexit),90))
          |OR (convetToDate(a.dateofprogramexit) BETWEEN convetToDate(b.dateofprogramentry) AND date_add(convetToDate(a.dateofprogramexit),90))""".stripMargin)
      dups2.registerTempTable("dups2")
      dups2.show()

      //Duplicate Check 3 part-1
      var dups3Part1 = sqlContext.sql(
        """select
          |uniqueindividualidentifier,
          |max(dateofprogramentry) AS dateofprogramentry
          |from preDups3
          |group by uniqueindividualidentifier
          |having count(uniqueindividualidentifier) > 1""".stripMargin)
      dups3Part1.registerTempTable("dups3Part1")
      dups3Part1.show()

      //Duplicate Check 3 part-2
      var dups3Part2 = sqlContext.sql(
        """SELECT
          |a.uniqueindividualidentifier,
          |a.coveredpersonentry,
          |a.dateofprogramentry,
          |a.datefirstcasemanagementempservice,
          |a.dateofprogramexit
          |FROM preDups3 a LEFT JOIN dups3Part1 b ON a.uniqueindividualidentifier = b.uniqueindividualidentifier
          |AND a.dateofprogramentry<>b.dateofprogramentry
          |where length(a.dateofprogramexit)=0""".stripMargin)
      dups3Part2.registerTempTable("dups3Part2")

      //Duplicate Check 3 part-3
      var dups3Part3 = sqlContext.sql(
        """select
          |distinct a.uniqueindividualidentifier,
          |a.dateofprogramentry
          |from dups3Part1 a, dups3Part2 b
          |where a.uniqueindividualidentifier=b.uniqueindividualidentifier""".stripMargin)
      dups3Part3.registerTempTable("dups3Part3")


      var joinDups = sqlContext.sql(
        """SELECT
          |a.rownum,
          |a.uniqueindividualidentifier,
          |a.coveredpersonentry,
          |a.dateofprogramentry,
          |a.dateofprogramexit,
          |a.taapetitionnumber,
          |CASE WHEN (a.uniqueindividualidentifier=b.uniqueindividualidentifier AND a.coveredpersonentry=b.coveredpersonentry)
          |THEN
          |'Duplicate'
          |ELSE
          |''
          |END AS Duplicate1,
          |CASE WHEN (a.uniqueindividualidentifier=c.uniqueindividualidentifier AND a.dateofprogramentry=c.dateofprogramentry AND a.dateofprogramexit=c.dateofprogramexit)
          |THEN
          |'Duplicate'
          |ELSE
          |''
          |END AS Duplicate2,
          |CASE WHEN ((a.uniqueindividualidentifier=d.uniqueindividualidentifier AND a.dateofprogramentry=d.dateofprogramentry AND a.coveredpersonentry=d.coveredpersonentry) OR (a.uniqueindividualidentifier=e.uniqueindividualidentifier AND a.dateofprogramentry=e.dateofprogramentry))
          |THEN
          |'Duplicate'
          |ELSE
          |''
          |END AS Duplicate3
          |FROM wioaips1 a
          |LEFT JOIN dups1 b ON a.uniqueindividualidentifier = b.uniqueindividualidentifier AND a.coveredpersonentry=b.coveredpersonentry
          |LEFT JOIN dups2 c ON a.uniqueindividualidentifier=c.uniqueindividualidentifier AND a.dateofprogramentry=c.dateofprogramentry AND a.dateofprogramexit=c.dateofprogramexit
          |LEFT JOIN dups3Part2 d ON a.uniqueindividualidentifier=d.uniqueindividualidentifier AND a.dateofprogramentry=d.dateofprogramentry AND a.coveredpersonentry=d.coveredpersonentry
          |LEFT JOIN dups3Part3 e ON a.uniqueindividualidentifier=e.uniqueindividualidentifier AND a.dateofprogramentry=e.dateofprogramentry""".stripMargin)

      var joinDups1 = sqlContext.sql(
        """SELECT
          |a.rownum,
          |a.uniqueindividualidentifier,
          |a.coveredpersonentry,
          |a.dateofprogramentry,
          |a.dateofprogramexit,
          |a.taapetitionnumber,
          |FROM wioaips1 a
          |RIGHT JOIN dups1 b ON a.uniqueindividualidentifier = b.uniqueindividualidentifier AND a.coveredpersonentry=b.coveredpersonentry
          |RIGHT JOIN dups2 c ON a.uniqueindividualidentifier=c.uniqueindividualidentifier AND a.dateofprogramentry=c.dateofprogramentry AND a.dateofprogramexit=c.dateofprogramexit
          |RIGHT JOIN dups3Part2 d ON a.uniqueindividualidentifier=d.uniqueindividualidentifier AND a.dateofprogramentry=d.dateofprogramentry AND a.coveredpersonentry=d.coveredpersonentry
          |RIGHT JOIN dups3Part3 e ON a.uniqueindividualidentifier=e.uniqueindividualidentifier AND a.dateofprogramentry=e.dateofprogramentry""".stripMargin)

   //var dups1 = sqlContext.sql("SELECT id, dateofprogramentry from edrvs group by id, dateofprogramentry having count(id) > 1 and count(dateofprogramentry) > 1")
			/*var dups1 = sqlContext.sql("SELECT a.id, a.dateofprogramentry from edrvs a INNER JOIN (SELECT id, COUNT(*) as count from edrvs group by id having count(*)>1) dt ON a.id=dt.id")
      dups1.registerTempTable("dups1")
      dups1.show()*/

      /*var dups2 = sqlContext.sql("SELECT distinct a.id, a.dateofprogramentry, a.dateofprogramexit FROM edrvs a, edrvs b WHERE a.id = b.id AND a.rownum<>b.rownum AND ((convetToDate(a.dateofprogramentry) BETWEEN convetToDate(b.dateofprogramentry) AND addDays(b.dateofprogramexit)) OR (convetToDate(a.dateofprogramexit) BETWEEN convetToDate(b.dateofprogramentry) AND addDays(b.dateofprogramexit)))")
      dups2.registerTempTable("dups2")
      dups2.show()*/

     /* var dups2 = sqlContext.sql("SELECT distinct a.id, a.dateofprogramentry, a.dateoffirstcasemanagement, a.dateofprogramexit FROM edrvs a, edrvs b WHERE a.id = b.id AND convetToDate(a.dateofprogramentry) > convetToDate(b.dateofprogramentry) AND convetToDate(a.dateofprogramentry)< addDays(b.dateofprogramexit)")
      dups2.registerTempTable("dups2")
      dups2.show()
      var dups2_2 = sqlContext.sql("SELECT  distinct a.id, a.dateofprogramentry, a.dateoffirstcasemanagement, a.dateofprogramexit FROM edrvs a, edrvs b WHERE a.id = b.id AND convetToDate(a.dateofprogramexit) BETWEEN convetToDate(b.dateofprogramentry) AND addDays(b.dateofprogramexit)")
      dups2_2.registerTempTable("dups2_2")
      dups2_2.show()*/
      /*var joinDups = sqlContext.sql("SELECT a.id, a.dateofprogramentry, a.dateoffirstcasemanagement, a.dateofprogramexit, CASE WHEN ((a.id=b.id AND a.dateofprogramentry=b.dateofprogramentry AND a.dateofprogramexit=b.dateofprogramexit) OR (a.id=c.id AND a.dateofprogramentry=c.dateofprogramentry AND a.dateofprogramexit=c.dateofprogramexit)) THEN 'D' ELSE '' END AS Duplicate2 FROM edrvs a LEFT JOIN dups2 b ON a.id=b.id AND a.dateofprogramentry=b.dateofprogramentry AND a.dateofprogramexit=b.dateofprogramexit LEFT JOIN dups2_2 c ON a.id=c.id AND a.dateofprogramentry=c.dateofprogramentry AND a.dateofprogramexit=c.dateofprogramexit")
      joinDups.show()*/
      var dups3Sub = sqlContext.sql("select id, max(dateofprogramentry) AS dateofprogramentry from edrvs where dateofprogramentry IS NOT NULL group by id having count(id) > 1")
      dups3Sub.registerTempTable("dups3Sub")
      dups3Sub.show()
      var dups3 = sqlContext.sql("SELECT a.id, a.dateofprogramentry, a.dateofprogramexit FROM edrvs a LEFT JOIN dups3Sub b ON a.id = b.id AND a.dateofprogramentry<>b.dateofprogramentry where length(a.dateofprogramexit)=0")
      dups3.registerTempTable("dups3")
      dups3.show()
      var dups3Final = sqlContext.sql("select distinct a.id, a.dateofprogramentry from dups3Sub a, dups3 b where a.id=b.id")
      dups3Final.show()
      /* var joinDups = sqlContext.sql("SELECT a.id, a.dateofprogramentry, a.dateoffirstcasemanagement, a.employmentservice, a.dateofprogramexit, CASE WHEN (a.id=b.id AND a.dateofprogramentry=b.dateofprogramentry) THEN 'D' ELSE '' END AS Duplicate1, CASE WHEN (a.id=c.id AND a.dateofprogramentry=c.dateofprogramentry AND a.dateoffirstcasemanagement=c.dateoffirstcasemanagement) THEN 'D' ELSE '' END AS Duplicate2, CASE WHEN (a.id=d.id AND a.dateofprogramentry=d.dateofprogramentry AND a.dateoffirstcasemanagement=d.dateoffirstcasemanagement) THEN 'D' ELSE '' END AS Duplicate3 FROM edrvs a LEFT JOIN dups1 b ON a.id = b.id AND a.dateofprogramentry=b.dateofprogramentry LEFT JOIN dups2 c ON a.id=c.id AND a.dateofprogramentry=c.dateofprogramentry AND a.dateoffirstcasemanagement=c.dateoffirstcasemanagement LEFT JOIN dups3 d ON a.id=d.id AND a.dateofprogramentry=d.dateofprogramentry AND a.dateoffirstcasemanagement=d.dateoffirstcasemanagement")
       joinDups.show()
 */

		
		//saveDfToCsv(joinDups, "b.csv")
	}

	/*DataFrame.get_value(index, col, takeable=False)
Quickly retrieve single value at passed column and index

Parameters:	
index : row label
col : column label
takeable : interpret the index/col as indexers, default False
Returns:	
value : scalar value*/

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
}