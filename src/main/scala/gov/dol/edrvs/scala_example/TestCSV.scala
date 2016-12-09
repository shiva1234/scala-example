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

object TestCSV {

  def foo(x: Array[String]): Unit = x.foldLeft("")((a, b) => a + b)

  case class Person(name: String, age: Int)

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
    val people = sc.textFile("C:\\Users\\eravelly.shiva.kumar\\Desktop\\people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
    people.registerTempTable("people")
    var keyword="123456789abcde"
    if (keyword.isEmpty) keyword= "17000620345345"
    val sliced = keyword.slice(0,8)
    println(keyword.slice(0,8))
      val param = "19"
    sqlContext.udf.register("addDays", (word: Integer) => {
       if (word == 12345678) 19 else 0
      })
      

val format = new java.text.SimpleDateFormat("yyyymmdd")
val mnth = Calendar.getInstance().getTime().getMonth()+1
val year = Calendar.getInstance().getTime().getYear()
println(year)
val beginOfReportPeriod = format.parse(beginOfReportPrd(mnth))
val endOfReportPeriod = format.parse(endOfReportPrd(mnth))
println(beginOfReportPeriod)

       sqlContext.udf.register("addDays", (word: Integer) => {
       if (word == 19) 19 else 0
      })
    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age < 17")
    println(teenagers.collect().apply(0).get(0))
    println(teenagers.count)
    var median=0.00
    if(teenagers.count>0){
      val coll: Array[Any] =teenagers.rdd.map(r => r(1)).collect()
      val intArr:Array[Int] = coll.map(_.toString).map(_.toInt)
      println(intArr.length)
if((intArr.length/2)*2==intArr.length){
  println("is even")
  median=intArr.apply(intArr.length/2)

}else {
  val a=((intArr.length/2)+0.5).toInt
  val b=((intArr.length/2)-0.5).toInt
  median=(intArr.apply(a)+intArr.apply(b))/2
  println("is odd")
}
      println(median)
    }
    /*val coll: Array[Any] =teenagers.rdd.map(r => r(1)).collect()
    val intArr:Array[Int] = coll.map(_.toString).map(_.toInt)
    intArr.foreach(println)*/
    //val sorted = intArr.sort
    //val median = (sorted(array.length/2) + sorted(array.length - array.length/2)) / 2
    //println(median)
    //println(teenagers.count)
    teenagers.registerTempTable("teenagers")
    /*var x = teenagers.collect().map(r => r.mkString(",")).head+","+
    		         teenagers.collect().map(r => r.mkString(",")).head
    println(x)*/
    val arrayed =  Array(teenagers.collect().apply(0).get(0), teenagers.collect().apply(1).get(0), teenagers.collect().apply(2).get(0), teenagers.collect().apply(0).get(0))
      //println(arrayed.mkString(","))
    arrayed.foreach(println)
    //arrayed.foreach(println)
    //val alternative = arrayed.reduce((s1, s2) => s1 + ", " + s2)
    //println(alternative)
  
   // val rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = alternative.rdd
 //val df = alternative.unionAll(alternative)
    //val rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = teenagers.rdd
   
     //teenagers.first._2._1
    //rows.union(rows).collect().foreach(println)
    
    //teenagers.show()
 teenagers.show()
    saveDfToCsv(teenagers, "b.csv")
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
  
  def beginOfReportPrd(choice: Int): String = choice match {
    case 1 | 2 | 3 => "20160101"
    case 4 | 5 | 6 => "20160401"
    case 7 | 8 | 9 => "20160701"
    case 10 | 11 | 12 => "20161001"
  }
  
  def endOfReportPrd(choice: Int): String = choice match {
    case 1 | 2 | 3 => "20160331"
    case 4 | 5 | 6 => "20160630"
    case 7 | 8 | 9 => "20160930"
    case 10 | 11 | 12 => "20161231"
  }
}