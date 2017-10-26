package gov.dol.edrvs.scala_example

import org.apache.spark.sql.{Row, SparkSession}

object ListToDF {
  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder().appName("APR-Rules-").master("local[*]").config("spark.sql.warehouse.dir","C:\\spark-warehouse").getOrCreate()
    System.setProperty("hadoop.home.dir","C:\\winutils\\")
    val map = scala.collection.mutable.Map[Integer,Array[Int]]()
    val n=3

    var values=List[List[String]]()
    values = List(
      List("Current", "Number Served (Reportable Individual)", "2016-07-01 00:00:00", "2016-12-31 00:00:00"),
      List("Current", "Funds Expended", "2016-07-01 00:00:00", "2016-12-31 00:00:00"),
      List("Current", "Number Served (Participant)", "2016-07-01 00:00:00", "2016-12-31 00:00:00"),
      List("Current", "Measurable Skill Gains", "2016-07-01 00:00:00", "2016-12-31 00:00:00"),
      List("Current", "Veterans Priority of Service", "2016-07-01 00:00:00", "2016-12-31 00:00:00"),
      List("Current", "Number Exited (Reportable Individual)", "2016-07-01 00:00:00", "2016-09-30 00:00:00"),
      List("Current", "Number Exited (Participant)", "2016-07-01 00:00:00", "2016-09-30 00:00:00"),
      List("Current", "Effectiveness in Serving Employers", "2016-07-01 00:00:00", "2016-09-30 00:00:00"),
      List("Previous", "Number Served (Reportable Individual)", "2016-07-01 00:00:00", "2016-09-30 00:00:00"),
      List("Previous", "Funds Expended", "2016-07-01 00:00:00", "2016-09-30 00:00:00"),
      List("Previous", "Number Served (Participant)", "2016-07-01 00:00:00", "2016-09-30 00:00:00"),
      List("Previous", "Measurable Skill Gains", "2016-07-01 00:00:00", "2016-09-30 00:00:00"),
      List("Previous", "Veterans Priority of Service", "2016-07-01 00:00:00", "2016-09-30 00:00:00")
    )
    val rows = values.map{x => Row(x:_*)}
    val rdd = spark.sparkContext.makeRDD(rows)
    import spark.implicits._
    val a=values.toDF()
   // a.coalesce(1).write.json("s3://ske-dev-incoming/Aggs/Shiva")

    val z="Current,Number Served (Reportable Individual),2016-07-01 00:00:00,2016-12-31 00:00:00;Current,Funds Expended,2016-07-01 00:00:00,2016-12-31 00:00:00;Current,Number Served (Participant),2016-07-01 00:00:00,2016-12-31 00:00:00;Current,Measurable Skill Gains,2016-07-01 00:00:00,2016-12-31 00:00:00;Current,Veterans Priority of Service,2016-07-01 00:00:00,2016-12-31 00:00:00;Current,Number Exited (Reportable Individual),2016-07-01 00:00:00,2016-09-30 00:00:00;Current,Number Exited (Participant),2016-07-01 00:00:00,2016-09-30 00:00:00;Current,Effectiveness in Serving Employers,2016-07-01 00:00:00,2016-09-30 00:00:00;Previous,Number Served (Reportable Individual),2016-07-01 00:00:00,2016-09-30 00:00:00;Previous,Funds Expended,2016-07-01 00:00:00,2016-09-30 00:00:00;Previous,Number Served (Participant),2016-07-01 00:00:00,2016-09-30 00:00:00;Previous,Measurable Skill Gains,2016-07-01 00:00:00,2016-09-30 00:00:00;Previous,Veterans Priority of Service,2016-07-01 00:00:00,2016-09-30 00:00:00"
var zz:Array[String]=null
    zz=z.split(";")
    var values1=List[List[String]]()
    for(i <- 0 to zz.length-1){
      var lil:List[String]=zz(i).toString().split(",").toList
      values1.lastIndexOf(lil)
    }
for(g<-0 to values1.length-1)println(values1(g))

    val rows1 = values1.map{x => Row(x:_*)}
    val rdd1 = spark.sparkContext.makeRDD(rows1)
    import spark.implicits._
    val a1=values1.toDF()

    val valu = Array("a","b","c","d","e").toList
    import spark.implicits._
    val ddf = valu.toDF("ID")
    ddf.show()
  }

  def instantiateArray(n:Int): Unit ={
    /*Map<String, List<Item>> map = new HashMap<String, List<Item>>()
    map.put("d1", new ArrayList<Item>());
    ...*/
  }
}
