package gov.dol.edrvs.scala_example

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.sql.Date
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}


object AggregationRules {
	def main(args: Array[String]) {

		val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount")
				val ssc = new StreamingContext(sparkConf, Seconds(5))
		val lines = ssc.textFileStream("s3://spark-ske/Pre-Process/")
		lines.foreachRDD { (rdd, time) =>
		val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
		val qtr = "20160620abcddfgdf"
		val reportingPeriod = ""
		val beginOfReportPeriod = ""
		val endOfReportPeriod = ""

		import sqlContext.implicits._
		val edrvs = rdd.map(_.split(",")).map(w => PrsSchema1(w(0).toString, w(99).toString, w(122).toString, w(141).toInt, w(64).toString, w(63).toString, w(15).toInt, w(32).toInt, w(232).toInt, w(233).toInt, w(253).toDouble, w(16).toInt, w(17).toInt, w(18).toInt, w(19).toInt, w(20).toInt, w(22).toInt, w(13).toInt, w(167).toInt)).toDF()

		edrvs.registerTempTable("edrvs")
		sqlContext.udf.register("reportingPeriod", (word: String) => {
			val format = new java.text.SimpleDateFormat("yyyymmdd")
			val date = format.parse(word)
			new Date(date.getTime() + 90*24*60*60*1000)
		})
		sqlContext.udf.register("convetToDate", (word: String) => {
			val format = new java.text.SimpleDateFormat("yyyymmdd")
			val date = format.parse(word)

			new Date(date.getTime())
		})
		
		      // A.1-Info Summary - Staff Assisted Basic Career Services - Total Exiters Rule
		var c00= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (length(firststaffassisted)>0 AND length(firstindividualizedcareerservice)=0 and receivedtraining = 0) AND convetToDate(dateofprogramexit) < convetToDate($reportingPeriod)""")
   c00.show()
		// A.1-Info Summary - Individualized Career Services-Youth Services - Total Exiters Rule
		var c01= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where length(firststaffassisted)>0 AND receivedtraining = 0 AND dateofprogramexit < convetToDate($reportingPeriod)""")
		c01.show()
    // A.1-Info Summary - Training Services - Total Exiters Rule
		var c02 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where receivedtraining = 1  and convetToDate(dateofprogramexit) < convetToDate($reportingPeriod)""")
		c02.show()
    // A.1-Info Summary - Previous Period - Total Exiters Rule
		var c03 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (length(firststaffassisted)>0 OR length(firstindividualizedcareerservice)>0 OR receivedtraining = 1) AND convetToDate(dateofprogramexit) < convetToDate($reportingPeriod)""")	
    c03.show()
    // A.1-Info Summary - Current Period - Total Exiters Rule
		var c04 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (length(firststaffassisted)>0 OR length(firstindividualizedcareerservice)>0 OR receivedtraining = 1) AND convetToDate(dateofprogramexit) < convetToDate($reportingPeriod)""")
    c04.show()
		c04.rdd.saveAsTextFile("")
   

		// A.2-Info Summary - Staff Assisted Basic Career Services - Total Participants Served Rule
		var c10 = sqlContext.sql(s"""Select count(distinct uniqueindividualidentifier) AS totalparticipantsserved from edrvs where length(firststaffassisted)>0 AND length(firstindividualizedcareerservice)=0 and receivedtraining = 0 AND dateofprogramentry <= convetToDate($endOfReportPeriod) AND convetToDate(dateofprogramexit) >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0""")
		// A.2-Info Summary - Individualized Career Services-Youth Services - Total Participants Served Rule
		var c11 = sqlContext.sql(s"""Select count(distinct uniqueindividualidentifier) AS totalparticipantsserved from edrvs where length(firstindividualizedcareerservice)>0 AND receivedtraining = 0 AND dateofprogramentry <= convetToDate($endOfReportPeriod) AND dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0""")
		// A.2-Info Summary - Training Services - Total Participants Served Rule
		var c12 = sqlContext.sql(s"""Select count(distinct uniqueindividualidentifier) AS totalparticipantsserved from edrvs where receivedtraining = 1 AND dateofprogramentry <= convetToDate($endOfReportPeriod) AND dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0""")
		// A.2-Info Summary - Previous Period - Total Participants Served Rule
		var c13= sqlContext.sql(s"""Select count(distinct uniqueindividualidentifier) AS totalparticipantsserved from edrvs where length(firststaffassisted)>0 OR length(firstindividualizedcareerservice)>0 OR receivedtraining = 1 AND dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0""")
		// A.2-Info Summary - Current Period - Total Participants Served Rule
		var c14= sqlContext.sql(s"""Select count(distinct uniqueindividualidentifier) AS totalparticipantsserved from edrvs where length(firststaffassisted)>0 OR length(firstindividualizedcareerservice)>0 OR receivedtraining = 1 AND dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0""")



		// Info Summary - Staff Assisted Basic Career Services - Total Exiters-Youth Served Rule
		var c20 = ""
		// Info Summary - Individualized Career Services-Youth Services - Total Exiters-Youth Served Rule
		var c21 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where length(dateoffirstyouthservice)>0 and receivedtraining = 0 and dateofprogramexit < convetToDate($reportingPeriod)""")
		// Info Summary - Training Services - Total Exiters-Youth Served Rule
		var c22 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where length(dateoffirstyouthservice)>0 and receivedtraining = 1 and dateofprogramexit < convetToDate($reportingPeriod)""")
		// Info Summary - Previous Period - Total Exiters-Youth Served Rule
		var c23= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where length(dateoffirstyouthservice)>0 and dateofprogramexit < convetToDate($reportingPeriod)""")
		// Info Summary - Current Period - Total Exiters-Youth Rule
		var c24= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where length(dateoffirstyouthservice)>0 and dateofprogramexit < convetToDate($reportingPeriod)""")


		
		// Info Summary - Staff Assisted Basic Career Services - Total Participants Served-Youth Rule
		var c30 = ""
		// Info Summary - Individualized Career Services-Youth Services - Total Participants Served-Youth Rule
		var c31 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where length(dateoffirstyouthservice)>0 AND receivedtraining = 0 AND  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// Info Summary - Training Services - Total Participants Served-Youth Rule
		var c32 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where length(dateoffirstyouthservice)>0 and receivedtraining = 1 AND dateofprogramentry <= convetToDate($endOfReportPeriod) AND dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0""")
		// Info Summary - Previous Period - Total Participants Served-Youth Rule
		var c33= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where length(dateoffirstyouthservice)>0 AND  dateofprogramentry <= convetToDate($endOfReportPeriod) AND dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0""")
		// Info Summary - Current Period - Total Participants Served-Youth Rule
		var c34= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where length(dateoffirstyouthservice)>0 AND  dateofprogramentry <= convetToDate($endOfReportPeriod) AND dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0""")
		
		
		// A.3-Info Summary - Staff Assisted Basic Career Services - Total Enrollees Rule
		var c40 = ""
		// A.3-Info Summary - Individualized Career Services-Youth Services - Total Enrollees Rule
		var c41 = ""
		// A.3-Info Summary - Training Services - Total Enrollees Rule
		var c42 = ""
		// A.3-Info Summary - Previous Period - Total Enrollees Rule
		var c43= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where firstselfservice IS NOT NULL AND (length(firststaffassisted)=0 AND length(firstindividualizedcareerservice)=0 AND receivedtraining = 0) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0)""")
		// A.3-Info Summary - Current Period - Total Enrollees Rule
		var c44= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where firstselfservice IS NOT NULL AND (length(firststaffassisted)=0 and length(firstindividualizedcareerservice)=0 AND receivedtraining = 0) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0)""")
		
		
			// B1a-Participant Summary and Service Information - Staff Assisted Basic Career Services - Male Participants Rule
		var c50 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (gender = 1 and (length(firststaffassisted)>0 AND length(firstindividualizedcareerservice)=0 AND receivedtraining = 0)) AND  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// B1a-Participant Summary and Service Information - Individualized Career Services-Youth Services - Male Participants Rule
		var c51 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (gender = 1 and (length(firstindividualizedcareerservice)>0 AND receivedtraining = 0)) AND ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// B1a-Participant Summary and Service Information - Training Services - Male Participants Rule
		var c52 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where gender = 1 and (receivedtraining = 1) AND ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// B1a-Participant Summary and Service Information - Previous Period - Male Participants Rule
		var c53= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (gender = 1 and (length(firststaffassisted)>0 OR length(firstindividualizedcareerservice)>0 or receivedtraining = 1)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0)""")
		// B1a-Participant Summary and Service Information - Current Period - Male Participants Rule
		var c54= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (gender = 1 and (length(firststaffassisted)>0 OR length(firstindividualizedcareerservice)>0 OR receivedtraining = 1)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0)""")
		
		
		
			// B1b-Participant Summary and Service Information - Staff Assisted Basic Career Services - Female Participants Rule
		var c60 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (gender = 2 and (length(firststaffassisted)>0 AND length(firstindividualizedcareerservice)=0 and receivedtraining = 0)) AND ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// B1b-Participant Summary and Service Information - Individualized Career Services-Youth Services - Female Participants Rule
		var c61 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (gender = 1 and (length(firstindividualizedcareerservice)>0 AND receivedtraining = 0)) AND ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// B1b-Participant Summary and Service Information - Training Services - Female Participants Rule
		var c62 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where gender = 2 AND (receivedtraining = 1) AND ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// B1b-Participant Summary and Service Information - Previous Period - Female Participants Rule
		var c63= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (gender = 2 AND (length(firststaffassisted)>0 OR length(firstindividualizedcareerservice)>0 OR receivedtraining = 1)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0)""")
		// B1b-Participant Summary and Service Information - Current Period - Female Participants Rule
		var c64= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (gender = 2 and (length(firststaffassisted)>0 OR length(firstindividualizedcareerservice)>0 OR receivedtraining = 1)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0)""")
		
		
		// B1a-Participant Summary and Service Information - Staff Assisted Basic Career Services - Male Participants-Youth Rule
		var c70 = ""
		// B1a-Participant Summary and Service Information - Individualized Career Services-Youth Services - Male Participants-Youth Rule
		var c71 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (gender = 1 AND length(dateoffirstyouthservice)>0 AND receivedtraining = 0) AND  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// B1a-Participant Summary and Service Information - Training Services - Male Participants-Youth Rule
		var c72 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where gender = 1 and (length(dateoffirstyouthservice)>0) AND (receivedtraining = 1) AND ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// B1a-Participant Summary and Service Information - Previous Period - Male Participants-Youth Rule
		var c73= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (gender = 1 AND (length(dateoffirstyouthservice)>0) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// B1a-Participant Summary and Service Information - Current Period - Male Participants-Youth Rule
		var c74= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (gender = 1 AND (length(dateoffirstyouthservice)>0) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		
		
		
		// B.1b-Participant Summary and Service Information - Staff Assisted Basic Career Services - Female Participants-Youth Rule
		var c80 = ""
		// B.1b-Participant Summary and Service Information - Individualized Career Services-Youth Services - Female Participants-Youth Rule
		var c81 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (gender = 2 AND length(dateoffirstyouthservice)>0 AND receivedtraining = 0) AND ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// B.1b-Participant Summary and Service Information - Training Services - Female Participants-Youth Rule
		var c82 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where gender = 2 AND length(dateoffirstyouthservice)>0 AND (receivedtraining = 1) AND ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// B.1b-Participant Summary and Service Information - Previous Period - Female Participants-Youth Rule
		var c83= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (gender = 2 AND length(dateoffirstyouthservice)>0 AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// B.1b-Participant Summary and Service Information - Current Period - Female Participants-Youth Rule
		var c84= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (gender = 2 AND length(dateoffirstyouthservice)>0 AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		
		
		
			// 2a-Participant Summary and Service Information - Staff Assisted Basic Career Services - Hispanic-Latino Participants Rule
		var c90 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((hispanic = 1) AND (length(firststaffassisted)>0 AND length(firstindividualizedcareerservice)=0 AND receivedtraining = 0)) AND ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// 2a-Participant Summary and Service Information - Individualized Career Services-Youth Services - Hispanic-Latino Participants Rule
		var c91 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( hispanic = 1) AND (length(firstindividualizedcareerservice)>0 and receivedtraining = 0)) AND  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// 2a-Participant Summary and Service Information - Training Services - Hispanic-Latino Participants Rule
		var c92 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ( hispanic = 1) AND (receivedtraining = 1) AND ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// 2a-Participant Summary and Service Information - Previous Period - Hispanic-Latino Participants Rule
		var c93= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( hispanic = 1) AND (length(firststaffassisted)>0 OR length(firstindividualizedcareerservice)>0 OR receivedtraining = 1)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0)""")
		// 2a-Participant Summary and Service Information - Current Period - Hispanic-Latino Participants Rule
		var c94= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( hispanic = 1) AND (length(firststaffassisted)>0 OR length(firstindividualizedcareerservice)>0 OR receivedtraining = 1)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0)""")
		
		/*
		 * Yet to script
		 */
		
			// 2a-Participant Summary and Service Information - Staff Assisted Basic Career Services - Hispanic-Latino Rate-Q2 Rule
		//var c10_0 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((hispanic = 1) AND (length(firststaffassisted)>0 AND length(firstindividualizedcareerservice)=0 AND receivedtraining = 0)) AND ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// 2a-Participant Summary and Service Information - Individualized Career Services-Youth Services - Hispanic-Latino Rate-Q2 Rule
		//var c10_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( hispanic = 1) AND (length(firstindividualizedcareerservice)>0 and receivedtraining = 0)) AND  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// 2a-Participant Summary and Service Information - Training Services - Hispanic-Latino Rate-Q2 Rule
		//var c10_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ( hispanic = 1) AND (receivedtraining = 1) AND ((dateofprogramentry <= convetToDate($endOfReportPeriod)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// 2a-Participant Summary and Service Information - Previous Period - Hispanic-Latino Rate-Q2 Rule
		//var c10_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( hispanic = 1) AND (length(firststaffassisted)>0 OR length(firstindividualizedcareerservice)>0 OR receivedtraining = 1)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		// 2a-Participant Summary and Service Information - Current Period - Hispanic-Latino Rate-Q2 Rule
		//var c10_4= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( hispanic = 1) AND (length(firststaffassisted)>0 OR length(firstindividualizedcareerservice)>0 OR receivedtraining = 1)) AND (dateofprogramexit >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		
		
		
			// 2a-Participant Summary and Service Information - Staff Assisted Basic Career Services - Hispanic-Latino Participants Youth Rule
		var c11_0 = ""
		// 2a-Participant Summary and Service Information - Individualized Career Services-Youth Services - Hispanic-Latino Participants Youth Rule
		var c11_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((hispanic = 1) and (length(dateoffirstyouthservice)>0) and receivedtraining = 0) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2a-Participant Summary and Service Information - Training Services - Hispanic-Latino Participants Youth Rule
		var c11_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (hispanic = 1) and (length(dateoffirstyouthservice)>0) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2a-Participant Summary and Service Information - Previous Period - Hispanic-Latino Participants Youth Rule
		var c11_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((hispanic = 1) and (length(dateoffirstyouthservice)>0) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2a-Participant Summary and Service Information - Current Period - Hispanic-Latino Participants Youth Rule
		var c11_4= c11_3
		
		
			/*
		 * Yet to script
		 */
		
		// 2a-Participant Summary and Service Information - Staff Assisted Basic Career Services - Hispanic-Latino Mediam Earnings Youth Rule
		//var c12_0 = ""
		// 2a-Participant Summary and Service Information - Individualized Career Services-Youth Services - Hispanic-Latino Mediam Earnings Youth Rule
		//var c12_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((hispanic = 1) and (length(dateoffirstyouthservice)>0) and receivedtraining = 0) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2a-Participant Summary and Service Information - Training Services - Hispanic-Latino Mediam Earnings Youth Rule
		//var c12_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (hispanic = 1) and (length(dateoffirstyouthservice)>0) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2a-Participant Summary and Service Information - Previous Period - Hispanic-Latino Mediam Earnings Youth Rule
		//var c12_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((hispanic = 1) and (length(dateoffirstyouthservice)>0) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2a-Participant Summary and Service Information - Current Period - Hispanic-Latino Mediam Earnings Youth Rule
		//var c12_4= c12_3
		
		
		
		
		// 2b-Participant Summary and Service Information - Staff Assisted Basic Career Services - American Indian or Alaskan Native Participants Rule
		var c13_0 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( nativeamerican = 1) and (length(firststaffassisted)>0  and  length(firstindividualizedcareerservice)=0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2b-Participant Summary and Service Information - Individualized Career Services-Youth Services - American Indian or Alaskan Native Participants Rule
		var c13_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( nativeamerican = 1) and (length(firstindividualizedcareerservice)>0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2b-Participant Summary and Service Information - Training Services - American Indian or Alaskan Native Participants Rule
		var c13_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ( nativeamerican = 1) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2b-Participant Summary and Service Information - Previous Period - American Indian or Alaskan Native Participants Rule
		var c13_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( nativeamerican = 1) and (length(firststaffassisted)>0 or length(firstindividualizedcareerservice)>0 or receivedtraining = 1)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0)""")
		// 2b-Participant Summary and Service Information - Current Period - American Indian or Alaskan Native Participants Rule
		var c13_4= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( nativeamerican = 1) and (length(firststaffassisted)>0 or length(firstindividualizedcareerservice)>0 or receivedtraining = 1)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0)""")
		
		
		
		// 2b-Participant Summary and Service Information - Staff Assisted Basic Career Services - American Indian or Alaskan Native Participants Youth Rule
		var c14_0 = ""
		// 2b-Participant Summary and Service Information - Individualized Career Services-Youth Services - American Indian or Alaskan Native Participants Youth Rule
		var c14_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((nativeamerican = 1) and (length(dateoffirstyouthservice)>0) and receivedtraining = 0) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2b-Participant Summary and Service Information - Training Services - American Indian or Alaskan Native Participants Youth Rule
		var c14_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (nativeamerican = 1 and (length(dateoffirstyouthservice)>0) and (receivedtraining = 1)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
	 // 2b-Participant Summary and Service Information - Previous Period - American Indian or Alaskan Native Participants Youth Rule
		var c14_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((nativeamerican = 1) and (length(dateoffirstyouthservice)>0) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2b-Participant Summary and Service Information - Current Period - American Indian or Alaskan Native Participants Youth Rule
		var c14_4= c14_3
		
		
		
		// 2c-Participant Summary and Service Information - Staff Assisted Basic Career Services - Asian Participants Rule
		var c15_0 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( asian = 1) and (length(firststaffassisted)>0  and  length(firstindividualizedcareerservice)=0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2c-Participant Summary and Service Information - Individualized Career Services-Youth Services - Asian Participants Rule
		var c15_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( asian = 1) and (length(firstindividualizedcareerservice)>0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2c-Participant Summary and Service Information - Training Services - Asian Participants Rule
		var c15_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ( asian = 1) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2c-Participant Summary and Service Information - Previous Period - Asian Participants Rule
		var c15_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( asian = 1) and (length(firststaffassisted)>0 or length(firstindividualizedcareerservice)>0 or receivedtraining = 1)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0)""")
		// 2c-Participant Summary and Service Information - Current Period - Asian Participants Rule
		var c15_4= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( asian = 1) and (length(firststaffassisted)>0 or length(firstindividualizedcareerservice)>0 or receivedtraining = 1)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0)""")
		
		
		
			// 2c-Participant Summary and Service Information - Staff Assisted Basic Career Services - Asian Participants Youth Rule
		var c16_0 = ""
		// 2c-Participant Summary and Service Information - Individualized Career Services-Youth Services - Asian Participants Youth Rule
		var c16_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( asian = 1) and (length(dateoffirstyouthservice)>0) and receivedtraining = 0) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2c-Participant Summary and Service Information - Training Services - Asian Participants Youth Rule
		var c16_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( asian = 1) and (length(dateoffirstyouthservice)>0) and (receivedtraining = 1)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2c-Participant Summary and Service Information - Previous Period - Asian Participants Youth Rule
		var c16_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( asian = 1) and (length(dateoffirstyouthservice)>0) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2c-Participant Summary and Service Information - Current Period - Asian Participants Youth Rule
		var c16_4= c16_3
		
		
		
		// 2c-Participant Summary and Service Information - Staff Assisted Basic Career Services - Black or African American  Participants Rule
		var c17_0 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((black = 1) and (length(firststaffassisted)>0  and  length(firstindividualizedcareerservice)=0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2d-Participant Summary and Service Information - Individualized Career Services-Youth Services - Black or African American  Participants Rule
		var c17_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((black = 1) and (length(firstindividualizedcareerservice)>0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2d-Participant Summary and Service Information - Training Services - Black or African American  Participants Rule
		var c17_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (black = 1) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2d-Participant Summary and Service Information - Previous Period - Black or African American  Participants Rule
		var c17_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((black = 1) and (length(firststaffassisted)>0 or length(firstindividualizedcareerservice)>0 or receivedtraining = 1)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0)""")
		// 2d-Participant Summary and Service Information - Current Period - Black or African American Participants Rule
		var c17_4= c17_3
		
		
		
		// 2c-Participant Summary and Service Information - Staff Assisted Basic Career Services - Black or African American  Participants Rule
		var c18_0 = ""
		// 2d-Participant Summary and Service Information - Individualized Career Services-Youth Services - Black or African American  Participants Rule
		var c18_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((  black = 1) and (length(dateoffirstyouthservice)>0) and receivedtraining = 0) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2d-Participant Summary and Service Information - Training Services - Black or African American  Participants Rule
		var c18_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((  black = 1) and (length(dateoffirstyouthservice)>0) and (receivedtraining = 1)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2d-Participant Summary and Service Information - Previous Period - Black or African American  Participants Rule
		var c18_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((  black = 1) and (length(dateoffirstyouthservice)>0) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2d-Participant Summary and Service Information - Current Period - Black or African American Participants Rule
		var c18_4= c18_3
		
	
		/*
		 * Yet to be scripted
		 */
		
		// 2c-Participant Summary and Service Information - Staff Assisted Basic Career Services - Asian Participants Rule
		//var c19_0 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( asian = 1) and (length(firststaffassisted)>0  and  length(firstindividualizedcareerservice)=0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2c-Participant Summary and Service Information - Individualized Career Services-Youth Services - Asian Participants Rule
		//var c19_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( asian = 1) and (length(firstindividualizedcareerservice)>0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2c-Participant Summary and Service Information - Training Services - Asian Participants Rule
		//var c19_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ( asian = 1) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2c-Participant Summary and Service Information - Previous Period - Asian Participants Rule
		//var c19_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( asian = 1) and (length(firststaffassisted)>0 or length(firstindividualizedcareerservice)>0 or receivedtraining = 1)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0)""")
		// 2c-Participant Summary and Service Information - Current Period - Asian Participants Rule
		//var c19_4= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( asian = 1) and (length(firststaffassisted)>0 or length(firstindividualizedcareerservice)>0 or receivedtraining = 1)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0)""")
		
		
		
		// 2c-Participant Summary and Service Information - Staff Assisted Basic Career Services - Native Hawaiian or Other Pacific Islander  Participants Rule
		var c20_0 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((pacificislander = 1) and (length(firststaffassisted)>0  and  length(firstindividualizedcareerservice)=0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2e-Participant Summary and Service Information - Individualized Career Services-Youth Services - Native Hawaiian or Other Pacific Islander  Participants Rule
		var c20_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((pacificislander = 1) and (length(firstindividualizedcareerservice)>0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2e-Participant Summary and Service Information - Training Services - Native Hawaiian or Other Pacific Islander  Participants Rule
		var c20_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (pacificislander = 1) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2e-Participant Summary and Service Information - Previous Period - Native Hawaiian or Other Pacific Islander  Participants Rule
		var c20_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((pacificislander = 1) and (length(firststaffassisted)>0 or length(firstindividualizedcareerservice)>0 or receivedtraining = 1)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0)""")
		// 2e-Participant Summary and Service Information - Current Period - Native Hawaiian or Other Pacific Islander Participants Rule
		var c20_4= c20_3
		
		
		
		// 2c-Participant Summary and Service Information - Staff Assisted Basic Career Services - Native Hawaiian or Other Pacific Islander Youth Participants Rule
		var c21_0 = ""
		// 2e-Participant Summary and Service Information - Individualized Career Services-Youth Services - Native Hawaiian or Other Pacific Islander Youth Participants Rule
		var c21_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((  pacificislander = 1) and (length(dateoffirstyouthservice)>0) and receivedtraining = 0) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2e-Participant Summary and Service Information - Training Services - Native Hawaiian or Other Pacific Islander  Youth Participants Rule
		var c21_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (  pacificislander = 1) and (length(dateoffirstyouthservice)>0) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2e-Participant Summary and Service Information - Previous Period - Native Hawaiian or Other Pacific Islander  Youth Participants Rule
		var c21_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((  pacificislander = 1) and (length(dateoffirstyouthservice)>0) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2e-Participant Summary and Service Information - Current Period - Native Hawaiian or Other Pacific Islander Youth Participants Rule
		var c21_4= c21_3
		
		
		
		// 2f-Participant Summary and Service Information - Staff Assisted Basic Career Services - White  Participants Rule
		var c22_0 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((white = 1) and (length(firststaffassisted)>0  and  length(firstindividualizedcareerservice)=0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2f-Participant Summary and Service Information - Individualized Career Services-Youth Services - White  Participants Rule
		var c22_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((white = 1) and (length(firstindividualizedcareerservice)>0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2f-Participant Summary and Service Information - Training Services - White  Participants Rule
		var c22_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (white = 1) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2f-Participant Summary and Service Information - Previous Period - White  Participants Rule
		var c22_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((white = 1) and (length(firststaffassisted)>0 or length(firstindividualizedcareerservice)>0 or receivedtraining = 1)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0)""")
		// 2f-Participant Summary and Service Information - Current Period - White Participants Rule
		var c22_4= c22_3
		
		
		/*
		 * Yet to be scripted More than one race element missing
		 */
		
		// 2c-Participant Summary and Service Information - Staff Assisted Basic Career Services - More than One Race  Participants Rule
		//var c23_0 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((white = 1) and (length(firststaffassisted)>0  and  length(firstindividualizedcareerservice)=0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2g-Participant Summary and Service Information - Individualized Career Services-Youth Services - More than One Race  Participants Rule
		//var c23_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((white = 1) and (length(firstindividualizedcareerservice)>0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2g-Participant Summary and Service Information - Training Services - More than One Race  Participants Rule
		//var c23_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (white = 1) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2g-Participant Summary and Service Information - Previous Period - More than One Race  Participants Rule
		//var c23_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((white = 1) and (length(firststaffassisted)>0 or length(firstindividualizedcareerservice)>0 or receivedtraining = 1)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0)""")
		// 2g-Participant Summary and Service Information - Current Period - More than One Race Participants Rule
		//var c23_4= c23_3
		
		
		
		/*
		 * Yet to be scripted More than one race element missing
		 */
		
		// 2c-Participant Summary and Service Information - Staff Assisted Basic Career Services - More than One Race  Youth Participants Rule
		//var c24_0 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((white = 1) and (length(firststaffassisted)>0  and  length(firstindividualizedcareerservice)=0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2g-Participant Summary and Service Information - Individualized Career Services-Youth Services - More than One Race  Youth Participants Rule
		//var c24_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((white = 1) and (length(firstindividualizedcareerservice)>0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2g-Participant Summary and Service Information - Training Services - More than One Race  Youth Participants Rule
		//var c24_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (white = 1) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 2g-Participant Summary and Service Information - Previous Period - More than One Race  Youth Participants Rule
		//var c24_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((white = 1) and (length(firststaffassisted)>0 or length(firstindividualizedcareerservice)>0 or receivedtraining = 1)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0)""")
		// 2g-Participant Summary and Service Information - Current Period - More than One Youth Participants Rule
		//var c24_4= c24_3
		
		
		
		// 2c-Participant Summary and Service Information - Staff Assisted Basic Career Services - Elligible Veteran Status  Participants Rule
		var c25_0 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( eveteranstatus > 1) and (length(firststaffassisted)>0  and  length(firstindividualizedcareerservice)=0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3a-Participant Summary and Service Information - Individualized Career Services-Youth Services - Elligible Veteran Status  Participants Rule
		var c25_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( eveteranstatus > 1) and (length(firstindividualizedcareerservice)>0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3a-Participant Summary and Service Information - Training Services - Elligible Veteran Status  Participants Rule
		var c25_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ( eveteranstatus > 1) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3a-Participant Summary and Service Information - Previous Period - Elligible Veteran Status  Participants Rule
		var c25_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( eveteranstatus > 1) and (length(firststaffassisted)>0 or length(firstindividualizedcareerservice)>0 or receivedtraining = 1)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0)""")
		// 3a-Participant Summary and Service Information - Current Period - Elligible Veteran Status Participants Rule
		var c25_4= c25_3
		
		
		
		// 2c-Participant Summary and Service Information - Staff Assisted Basic Career Services - Elligible Veteran Status Youth Participants Rule
		var c26_0 = ""
		// 3a-Participant Summary and Service Information - Individualized Career Services-Youth Services - Elligible Veteran Status Youth Participants Rule
		var c26_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( eveteranstatus > 1) and (length(dateoffirstyouthservice)>0) and receivedtraining = 0) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3a-Participant Summary and Service Information - Training Services - Elligible Veteran Status  Youth Participants Rule
		var c26_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ( eveteranstatus > 1) and (length(dateoffirstyouthservice)>0) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3a-Participant Summary and Service Information - Previous Period - Elligible Veteran Status  Youth Participants Rule
		var c26_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( eveteranstatus > 1) and (length(dateoffirstyouthservice)>0) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3a-Participant Summary and Service Information - Current Period - Elligible Veteran Status Youth Participants Rule
		var c26_4= c25_3
		
		
		
		// 3b-Participant Summary and Service Information - Staff Assisted Basic Career Services - Individual with a Disability  Participants Rule
		var c27_0 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((disabilitystatus = 1) and (length(firststaffassisted)>0  and  length(firstindividualizedcareerservice)=0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3b-Participant Summary and Service Information - Individualized Career Services-Youth Services - Individual with a Disability  Participants Rule
		var c27_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( disabilitystatus = 1) and (length(firstindividualizedcareerservice)>0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3b-Participant Summary and Service Information - Training Services - Individual with a Disability  Participants Rule
		var c27_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ( disabilitystatus = 1) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3b-Participant Summary and Service Information - Previous Period - Individual with a Disability  Participants Rule
		var c27_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (( disabilitystatus = 1) and (length(firststaffassisted)>0 or length(firstindividualizedcareerservice)>0 or receivedtraining = 1)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0)""")
		// 3b-Participant Summary and Service Information - Current Period - Individual with a Disability Participants Rule
		var c27_4= c27_3
		
		
		
		// 3b-Participant Summary and Service Information - Staff Assisted Basic Career Services - Individual with a Disability  Youth Participants Rule
		var c28_0 = ""
		// 3b-Participant Summary and Service Information - Individualized Career Services-Youth Services - Individual with a Disability  Youth Participants Rule
		var c28_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((disabilitystatus = 1) and (length(dateoffirstyouthservice)>0) and receivedtraining = 0) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3b-Participant Summary and Service Information - Training Services - Individual with a Disability  Youth Participants Rule
		var c28_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (disabilitystatus = 1) and (length(dateoffirstyouthservice)>0) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3b-Participant Summary and Service Information - Previous Period - Individual with a Disability  Youth Participants Rule
		var c28_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((disabilitystatus = 1) and (length(dateoffirstyouthservice)>0) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3b-Participant Summary and Service Information - Current Period - Individual with a Disability Youth Participants Rule
		var c28_4= c28_3
		
	
		
		
		// 3c-Participant Summary and Service Information - Staff Assisted Basic Career Services - Incumbent Worker  Participants Rule
		var c29_0 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (adverselyaffectedincumbentworker > 0 and (length(firststaffassisted)>0  and  length(firstindividualizedcareerservice)=0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3c-Participant Summary and Service Information - Individualized Career Services-Youth Services - Incumbent Worker  Participants Rule
		var c29_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((adverselyaffectedincumbentworker > 0 ) and (length(firstindividualizedcareerservice)>0 and receivedtraining = 0)) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3c-Participant Summary and Service Information - Training Services - Incumbent Worker  Participants Rule
		var c29_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (adverselyaffectedincumbentworker > 0 ) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3c-Participant Summary and Service Information - Previous Period - Incumbent Worker  Participants Rule
		var c29_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((adverselyaffectedincumbentworker > 0 ) and (length(firststaffassisted)>0 or length(firstindividualizedcareerservice)>0 or receivedtraining = 1)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0)""")
		// 3c-Participant Summary and Service Information - Current Period - Incumbent Worker Participants Rule
		var c29_4= c29_3
		
		
		
		
		// 3c-Participant Summary and Service Information - Staff Assisted Basic Career Services - Incumbent Worker  Youth Participants Rule
		var c30_0 = ""
		// 3c-Participant Summary and Service Information - Individualized Career Services-Youth Services - Incumbent Worker  Youth Participants Rule
		var c30_1 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where adverselyaffectedincumbentworker > 0 AND length(dateoffirstyouthservice)>0 AND receivedtraining = 0 AND  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3c-Participant Summary and Service Information - Training Services - Incumbent Worker  Participants RuleYouth Participants Rule
		var c30_2 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where ((adverselyaffectedincumbentworker) > 0) and (length(dateoffirstyouthservice)>0) and (receivedtraining = 1) and  ((dateofprogramentry <= convetToDate($endOfReportPeriod)) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		// 3c-Participant Summary and Service Information - Previous Period - Incumbent Worker  Youth Participants Rule
		var c30_3= sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where (adverselyaffectedincumbentworker > 0) and (length(dateoffirstyouthservice)>0) and (dateofprogramexit >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0)""")
		// 3c-Participant Summary and Service Information - Current Period - Incumbent Worker Youth Participants Rule
		var c30_4= c30_3
		
		}

		
		/*((hispanic = 1 AND nativeamerican =1)OR (hispanic = 1 AND asian = 1) OR (hispanic = 1 AND black= 1) OR (hispanic= 1 AND pacificislander= 1) OR (hispanic= 1 AND white= 1) OR (nativeamerican= 1 and asian= 1) OR (nativeamerican= 1 and black= 1) OR (nativeamerican= 1 and pacificislander= 1) OR 
(nativeamerican= 1 and white= 1) OR (asian= 1 and black= 1) OR (asian= 1 and pacificislander= 1) OR (asian= 1 and white= 1) OR (black= 1 and pacificislander= 1) OR (black= 1 and white= 1) OR (pacificislander= 1 and white= 1) OR (hispanic= 1 and nativeamerican= 1 and asian= 1) OR 
(hispanic= 1 and nativeamerican= 1 and black= 1) OR (hispanic= 1 and nativeamerican= 1 and pacificislander= 1) OR (hispanic= 1 and nativeamerican= 1 and white= 1) OR (nativeamerican= 1 and asian= 1 and black= 1) OR (nativeamerican= 1 and asian= 1 and pacificislander= 1)
OR (nativeamerican= 1 and asian= 1 and white= 1) OR (asian= 1 and black= 1 and pacificislander= 1) OR (asian= 1 and black= 1 and white= 1) OR (black= 1 and pacificislander= 1 and white= 1) OR (hispanic= 1 and nativeamerican= 1 and asian= 1 and black= 1) OR (hispanic= 1 and nativeamerican= 1 and asian= 1 and pacificislander= 1) OR 
(hispanic= 1 and nativeamerican= 1 and asian= 1  and white= 1) OR (nativeamerican= 1 and asian= 1 and black= 1 and pacificislander= 1) OR (nativeamerican= 1 and asian= 1 and black= 1 and white= 1) OR (hispanic= 1 and nativeamerican= 1 and asian= 1 and black= 1 and pacificislander= 1)
(hispanic= 1 and nativeamerican= 1 and asian= 1 and black= 1 and white= 1) OR (hispanic= 1 and nativeamerican= 1 and asian= 1 and black= 1 and pacificislander= 1 and white= 1))*/
		ssc.start()
		ssc.awaitTermination()
	}

}

case class PrsSchema1(uniqueindividualidentifier: String, firststaffassisted: String, firstindividualizedcareerservice: String,
                     receivedtraining: Integer, dateofprogramexit: String, dateofprogramentry: String, hispanic: Integer, employmentstatus: Integer, employed2ndqtrafterexitqtr: Integer,
                     typeempmatch2ndqtrafterexitqtr: Integer, exitwages2ndqtr: Double, nativeamerican: Integer, asian: Integer, black: Integer, pacificislander: Integer, white: Integer, eveteranstatus: Integer, 
                     disabilitystatus: Integer, adverselyaffectedincumbentworker: Integer)
/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
// scalastyle:on println
