package gov.dol.edrvs.scala_example


/**
	* Created by eravelly.shiva.kumar on 6/14/2016.
	*/

import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.DataFrame
import java.io.File
import java.sql.Date
import java.util.Calendar
import org.apache.spark.sql.{SQLContext, Row}

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
		val edrvs = sc.textFile("C:\\Users\\eravelly.shiva.kumar\\Desktop\\people1.txt")
		val schemaPIRL = "obsnumber,uniqueindividualidentifier,statecoderesidence,countrycoderesidence,zipcoderesidence,econlabormarketareaphyloc,birthdate,sex,disabilitystatus,disabilitycategory,iwdsddas,iwdlsmhas,iwdmhcbss,iwdws,iwdtcesr,iwdfc,hispanic,nativeamerican,asian,black,pacificislander,white,veteranstatus,eveteranstatus,employmentstatus,uceligiblestatus,longtermunemployed,occupationcodebeforeempmt,empindustrycode1stqtrprior,empindustrycode2ndqtrprior,empindustrycode3rdqtrprior,highestgradecompleted,highesteducationallevelcompleted,schoolstatusatparticipation,tanf,ssi_ssdi,snap,otherpublicassistancerecipient,pregnantorparentingyouth,youthadditionalassistance,fostercareyouth,homelessindvidualchildrenyouth,exoffender,lowincome,englishlanguagelearner,lowlevelofliteracy,culturalbarriers,singleparent,displacedhomemaker,migrantseasonalfarmworkerstatus,dateofprogramentry,dateofprogramexit,adult,dislocatedworker,youth,adulteducation,jobcorps,farmworkerjobs,indianprograms,veteranprograms,vocationaleducation,vocationalrehabilitation,wagnerpeyser,youthbuild,seniorcommunityserviceempporgram,etsrsnap,otherwiaornonwiaprograms,otherreasonsofexit,registeredapprenticeshipprogram,accountabilityexitstatus,reroadult,reroyouth,h1b,iwdiepp,ids504p,dateofmrcareerservices,mrdrioaselfservice,workforceinfoselfservice,careerguidance,workforceinfostaffassisted,jobsearch,referredemployment,mrdrfederaltraining,mrdpfederaltraining,mrdrfederaljob,mrdrfederalcontractorjob,mrdefederaljob,mrdefederalcontractorjob,mrdrunemploymentinsuranceclaimassistance,mrdrotherfederalorstateassistance,referredjvsgservices,referreddepartmentvaservices,mrdrstaffassistedbcsother,firstindividualizedcareerservice,mrdrindividualizedcareerservice,dateindividualempoymentplancreated,mrdrinternshiporworkexperienceopp,typeofworkexperience,datereceivedfinancialliteracyservices,daterecievedenglishassecondlangservices,receivedprevocactivities,transistionaljobs,receivedtraining,dateenteredtraining1,typeoftrainingservice1,otccode1,training_completed1,dateexittraining1,dateenteredtraining2,typeoftrainingservice2,otccode2,training_completed2,dateexittraining2,dateentertraining3,typeoftrainingservice3,otccode3,training_completed3,dateexittraining3,establishedita,pellgrantrecipient,waiverfromtrainingreq,casemanagementandreempservice,datewaivertrainingreqissued,currentqtrtrainingexpenditures,totaltrainingexpenditures,amountoftrainingcostsoverpayment,trainingcostsoverpaymentwaiver,distancelearning,parttimetraining,adverselyaffectedincumbentworker,trainingleadingtoassociatedegree,pseducationprogramparticipation,enrolledsecondaryeducationprogram,mrdreas,mrdrasss,mrdrweo,dppeeortplrpc,mrdreocwp,mrdldo,mrdrss,mrdrams,mrdrcgcs,mrdyrest,mrdyrsplmiei,mrdyrpstpa,datecompletionofyoutservices,employed1stqtrafterexitqtr,typeempmatch1stqtrafterexitqtr,employed2ndqtrafterexitqtr,typeempmatch2ndqtrafterexitqtr,employed3rdqtrafterexitqtr,typeempmatch3rdqtrafterexitqtr,employed4thqtrafterexitqtr,typeempmatch4thqtrafterexitqtr,emprelatedtotraining2ndqtrafterexit,occupationalcode,nontraditionalemployment,occupationalcodeemp2ndqtrafterexitqtr,occupationalcodeemp4thqtrafterexitqtr,empmtindustrycodexqtr1,empmtindustrycodexqtr2,empmtindustrycodexqtr3,empmtindustrycodexqtr4,employerretention2ndand4thqtr,priorwages3,priorwages2,priorwages1,exitwages1stqtr,exitwages2ndqtr,exitwages3rdqtr,exitwages4thqtr,recognizedcredentialtype,attainedrecognizedcredential,recognizedcredentialtype2,attainedrecognizedcredential2,recognizedcredentialtype3,attainedrecognizedcredential3,mrmeasurableskillgainseducationalfunc,mrmeasurableskillgainspstranscriptorrc,mrmeasurableskillgainssectranscriptorrc,mrmeasurableskillgainstrainingmilestone,mrmeasureableskillsprogression,mrdeetprecognizedpscredoremployment,schoolstatusexit,youth2ndqtrplacement,youth4thqtrplacement,categoryofassessment,pretestscore,educationalfunclevel,posttestscore,educationalfunclevelposttest,jcui,fipscsjl,fbar,hwp,hww,selfemployment,enteredmilitaryservice,epaorrap,exitcategory,ratrans,rahealthcare,familycareincludingchildcare,rahousginasstservices,ranutritionalasst,ratranslationinterpreationservices,rastaffassisted,rasafetytraining,workexpfundedgrant167,ojtfundedgrant167,ibostfundedgrant167,ostfundedgrant167,bstfundedgrant167,lackstransportation,longtermagriculturalemp,lackssignificantworkhistory,sixmonthspreprogramearningspda,totalpreprogramearnings12monthedp,dependentsunder18,concurparticipationetpdepthud,eligibilitydetermination,familitystatusnjfp,nfjpgrantenrollment,ssn,wibname,officename"
		import org.apache.spark.sql.types.{StructType,StructField,StringType}
		val schema = StructType(schemaPIRL.split(",", -1).map(fieldName => StructField(fieldName, StringType, true)))
		val rowRDD = edrvs.map(_.split(",", -1)).map(p => Row.fromSeq(p))
		val testPRS = sqlContext.createDataFrame(rowRDD, schema)
		testPRS.registerTempTable("edrvs")

		sqlContext.udf.register("convetToDate", (word: String) => {
			val format = new SimpleDateFormat("yyyyMMdd")
			var date = new java.util.Date()
			try {
				date = format.parse(word)
			} catch {
				case e: Exception => date = new SimpleDateFormat("yyyy-MM-dd").parse("1900-01-01")
			}
			new java.sql.Date(date.getTime())
		})

		sqlContext.udf.register("addDays", (word: String) => {
			var dt:Date=null
			if(word.trim.length==8){
				val format = new java.text.SimpleDateFormat("yyyymmdd")

				val date = format.parse(word)

				dt=new Date(date.getTime() + 90 * 24 * 60 * 60 * 1000)
			}
			dt
		})
val reportingPeriod="20160930"
		val endOfReportPeriod="20160930"
		val beginOfReportPeriod="20160701"

		var c00 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where sex = 1 and (receivedtraining = 1) AND ((convetToDate(dateofprogramentry) <= convetToDate($endOfReportPeriod)) AND (convetToDate(dateofprogramexit) >= convetToDate($beginOfReportPeriod) OR length(dateofprogramexit)=0))""")
		//var c01 = sqlContext.sql(s"""select count(distinct uniqueindividualidentifier) from edrvs where employmentstatus in (0,2,3,4)  and receivedtraining = 1 and  ((convetToDate(dateofprogramentry) <= convetToDate($reportingPeriod)) and (convetToDate(dateofprogramexit) >= convetToDate($beginOfReportPeriod) or length(dateofprogramexit)=0))""")
		c00.show()
		//c01.show(120)


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




}