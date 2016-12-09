package gov.dol.edrvs.scala_example

/**
  * Created by eravelly.shiva.kumar on 6/14/2016.
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.{Column, DataFrame}
import java.io.File
import java.sql.Date
import java.util.Calendar

import scala.collection.mutable

object CreateComplexDF {

  case class SubRecord(name1: String, age1: String)
  case class Person(name: String, age: String, pirl: Array[String])



  def main(args: Array[String]):Unit = {

    println("Hello World!")

    var conf = new SparkConf().setAppName("WordCount").setMaster("local")
    var sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    // Create an RDD of Person objects and register it as a table.
    var people = sc.textFile("C:\\Users\\eravelly.shiva.kumar\\Desktop\\people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toString, Array(p(2).toString, p(3).trim.toString))).toDF()
    people.registerTempTable("people")
val addColumn = people.withColumn("newCol", people("pirl"))
    addColumn.show()
    sqlContext.udf.register("addDays", (word: String) => {
      if (word == 1) 19 else 0
    })
    import org.apache.spark.sql.functions._
    def udfUIDA = udf((name: Seq[String] ) => {
      name.apply(0).toString.toUpperCase
    })
    /*def udfUIDA = udf((name: mutable.WrappedArray[String]) => {
      if(name.apply(0).toString.toUpperCase==name.apply(0).toString.toUpperCase) "y" else "f"
  })*/
    val pp = people.withColumn("name1", udfUIDA(people("pirl")))
    //val pp = people.withColumn("age", addDays(people("age")))
    pp.show()

    val teenagers = sqlContext.sql("SELECT name, age, explode(pirl) FROM people")
    people.select($"name", $"age", $"pirl"(0) as "name1", $"pirl"(1) as "age1").show()
    println(teenagers.count)


    teenagers.registerTempTable("teenagers")


    teenagers.show()
    //saveDfToCsv(teenagers, "b.csv")
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