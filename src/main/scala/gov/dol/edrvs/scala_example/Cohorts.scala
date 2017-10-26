package gov.dol.edrvs.scala_example

object Cohorts {
  def main(args: Array[String]):Unit = {
    val s3path="s3://ske-dev-incoming/Process/1155-1180-JVSG-8-20171012174242979-8702~JVSG~8~2017101120235343~PIRL_JVSG_bcsv.csv"
    val programName = (s3path.split('/')(4)).split('-')(1)
    val fileName = (s3path.split('/')(4))
    println(programName)
    println(fileName)
  }
}
