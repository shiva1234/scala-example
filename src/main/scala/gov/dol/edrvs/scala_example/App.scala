package gov.dol.edrvs.scala_example

import scala.collection.mutable.ListBuffer

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
   val a=List(List(1,1),List(null,1))
   //println(a.map(_.map(_.toString.replaceAll(null,""))))
println(a.map(_.map(str=> Option(str).getOrElse(""))))
    val list=List(1,2,3)
   val b=List(1,2,3)
    val grades = Map("Kim" -> 90,"Al" -> 85,"Melissa" -> 95,"Emily" -> 91,"Hannah" -> 92)

    //parameter that can take a variable number of arguments, i.e., a varargs
    printAll()
    printAll("foo")
    printAll("foo", "bar")
    printAll("foo", "bar", "baz")
    val fruits = List("apple", "banana", "cherry")
    printAll(fruits: _*)
  }
  def printAll(strings: String*) {
    strings.foreach(println)
  }

}
