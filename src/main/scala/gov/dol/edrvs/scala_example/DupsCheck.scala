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

object DupsCheck {

	def foo(x: Array[String]): Unit = x.foldLeft("")((a, b) => a + b)

case class User(id: String, dateofprogramentry: String, dateoffirstcasemanagement: String,
		employmentservice: String, dateofprogramexit: String)

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
		val people = sc.textFile("C:\\DataServices\\stream1.csv").map(_.split(",")).map(w => User(w(0).toString, w(1).toString, w(2).toString, w(3).toString, w(4).toString)).toDF()
		people.registerTempTable("edrvs")

		sqlContext.udf.register("addDays", (word: String) => {
			val format = new java.text.SimpleDateFormat("MM/dd/yyyy")
			val date = format.parse(word)
			new Date(date.getTime() + 90*24*60*60*1000)
		})
		sqlContext.udf.register("convetToDate", (word: String) => {
			val format = new java.text.SimpleDateFormat("MM/dd/yyyy")
			val date = format.parse(word)
			new Date(date.getTime())
		})

   var dups1 = sqlContext.sql("SELECT id, dateofprogramentry from edrvs group by id, dateofprogramentry having count(id) > 1 and count(dateofprogramentry) > 1")
      dups1.registerTempTable("dups1")
      dups1.show()
      var dups2 = sqlContext.sql("SELECT distinct a.id, a.dateofprogramentry, a.dateoffirstcasemanagement, a.employmentservice, a.dateofprogramexit FROM edrvs a, edrvs b WHERE a.id = b.id AND convetToDate(a.dateoffirstcasemanagement) BETWEEN convetToDate(b.dateofprogramentry) AND addDays(b.dateofprogramexit) AND convetToDate(a.employmentservice) BETWEEN convetToDate(b.dateofprogramentry) AND addDays(b.dateofprogramexit) AND convetToDate(a.dateofprogramexit) BETWEEN convetToDate(b.dateofprogramentry) AND addDays(b.dateofprogramexit)")
      dups2.registerTempTable("dups2")
      dups2.show()
      var dups3Sub = sqlContext.sql("select id, max(dateofprogramentry) AS dateofprogramentry, max(dateoffirstcasemanagement) AS dateoffirstcasemanagement, max(employmentservice) AS employmentservice from edrvs where dateofprogramentry IS NOT NULL and dateoffirstcasemanagement IS NOT NULL group by id having count(id) > 1")
      dups3Sub.registerTempTable("dups3Sub")
      dups3Sub.show()
      var dups3 = sqlContext.sql("SELECT a.id, a.dateofprogramentry, a.dateoffirstcasemanagement, a.employmentservice, a.dateofprogramexit FROM edrvs a LEFT JOIN dups3Sub b ON a.id = b.id AND a.dateofprogramentry=b.dateofprogramentry AND a.dateoffirstcasemanagement=b.dateoffirstcasemanagement where a.dateofprogramentry<>b.dateofprogramentry AND a.dateoffirstcasemanagement<>b.dateoffirstcasemanagement AND a.employmentservice<>b.employmentservice AND length(a.dateofprogramexit)=0")
      dups3.registerTempTable("dups3")
      dups3.show()
      var joinDups = sqlContext.sql("SELECT a.id, a.dateofprogramentry, a.dateoffirstcasemanagement, a.employmentservice, a.dateofprogramexit, CASE WHEN (a.id=b.id AND a.dateofprogramentry=b.dateofprogramentry) THEN 'D' ELSE '' END AS Duplicate1, CASE WHEN (a.id=c.id AND a.dateofprogramentry=c.dateofprogramentry AND a.dateoffirstcasemanagement=c.dateoffirstcasemanagement) THEN 'D' ELSE '' END AS Duplicate2, CASE WHEN (a.id=d.id AND a.dateofprogramentry=d.dateofprogramentry AND a.dateoffirstcasemanagement=d.dateoffirstcasemanagement) THEN 'D' ELSE '' END AS Duplicate3 FROM edrvs a LEFT JOIN dups1 b ON a.id = b.id AND a.dateofprogramentry=b.dateofprogramentry LEFT JOIN dups2 c ON a.id=c.id AND a.dateofprogramentry=c.dateofprogramentry AND a.dateoffirstcasemanagement=c.dateoffirstcasemanagement LEFT JOIN dups3 d ON a.id=d.id AND a.dateofprogramentry=d.dateofprogramentry AND a.dateoffirstcasemanagement=d.dateoffirstcasemanagement")
      joinDups.show()


		
		saveDfToCsv(joinDups, "b.csv")
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