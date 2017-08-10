package gov.dol.edrvs.scala_example

object AggregateFunctionScala {
  def main(args: Array[String]):Unit = {
    val x = List(1,2,3,4,5,6)
    val y = x.par.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1), (x,y) => (x._1 + y._1, x._2 + y._2))
    println(y)
  }

}
