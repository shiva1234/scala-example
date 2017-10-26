package gov.doleta.prs

/**
	* Created by Eravelly.Shiva.Kumar on 1/11/2017.
	*/

import java.text.SimpleDateFormat
import java.util
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.sql.Date
import java.util.{Properties, Calendar}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.log4j.{Level, Logger}
import gov.doleta.prs.APRRules._
import org.joda.time.DateTime
import org.apache.spark.sql._

object AnnualReportStep {
	val spark = SparkSession.builder().appName("DWG-Rules-").getOrCreate()

	val rootLogger = Logger.getRootLogger()
	rootLogger.setLevel(Level.ERROR)
	val prop = new Properties()
	val propFileName = "application.properties"
	val inputStream = getClass.getClassLoader.getResourceAsStream(propFileName)
	prop.load(inputStream)
	var annualId=""
	var aprId=""
	var s3path = "" //the full path to the file in s3
	var fileName = "" //just the file name
	var newFileName=""
	var programName = "" //the program name (parsed from file name)
	var S3_OUTGOING_BUCKET = ""// Output Bucket Name
	var S3_OUTGOING_FOLDER = ""// Output Folder Name
	var participantCharacteristicsStartDate = ""
	var participantCharacteristicsEndDate = ""
	var veteransPriorityServiceStartDate = ""
	var veteransPriorityServiceEndDate = ""
	var numberReportableIndividualsStartDate = ""
	var numberReportableIndividualsEndDate = ""
	var measurableSkillGainsStartDate = ""
	var measurableSkillGainsEndDate = ""
	var effectivenessServingEmployersEmployerPenetrationStartDate = ""
	var effectivenessServingEmployersEmployerPenetrationEndDate = ""
	var effectivenessServingEmployersRepeatBusinessCustomersStartDate = ""
	var effectivenessServingEmployersRepeatBusinessCustomersEndDate = ""
	var exiterCharacteristicsStartDate = ""
	var exiterCharacteristicsEndDate = ""
	var employmentRateSecondQuarterAfterExitStartDate = ""
	var employmentRateSecondQuarterAfterExitEndDate = ""
	var medianEarningsSecondQuarterAfterExitStartDate  = ""
	var medianEarningsSecondQuarterAfterExitEndDate  = ""
	var employmentRateFourthQuarterAfterExitStartDate = ""
	var employmentRateFourthQuarterAfterExitEndDate = ""
	var credentialAttainmentRateStartDate = ""
	var credentialAttainmentRateEndDate = ""
	var effectivenessServingEmployersRetentionwithSameStartDate = ""
	var effectivenessServingEmployersRetentionwithSameEndDate = ""
	var sqlSubString=Array(" ",""" (ReceivedTrainingWIOA = 1 or TypeOfTrainingService2WIOA<>"00" or length(TypeOfTrainingService2WIOA)=0 or TypeOfTrainingService3WIOA<>"00" or length(TypeOfTrainingService3WIOA)=0) and """,""" (TypeOfTrainingService1WIOA="01" or TypeOfTrainingService2WIOA="01" or TypeOfTrainingService3WIOA="01") and """,""" (TypeOfTrainingService1WIOA IN ("03","04","05","06","09") or TypeOfTrainingService2WIOA IN ("03","04","05","06","09") or TypeOfTrainingService3WIOA IN ("03","04","05","06","09")) and """," "," "," "," length(MostRecentDateReceivedSupportiveServices)>0 and "," ReceivedServicesThroughADisasterRecoveryDislocated=1 and "," ReceivedServicesThroughADisasterRecoveryDislocated=2 and "," ReceivedNeedsRelatedPayments=1 and "," (DislocatedWorkerWIOA in (1,2,3) or AdultWIOA in (1,2,3) or RapidResponse=1 or WagnerPeyserEmploymentServiceWIOA=1) and "," DislocatedWorkerWIOA in (1,2,3) and "," AdultWIOA in (1,2,3) and "," RapidResponse=1 and "," WagnerPeyserEmploymentServiceWIOA=1 and "," Sex=1 and "," Sex=2 and "," EthnicityHispanicLatinoWIOA=1 and "," AmericanIndianAlaskaNativeWIOA=1 and "," AsianWIOA=1 and "," BlackAfricanAmericanWIOA=1 and "," NativeHawaiianOtherPacificIslanderWIOA=1 and "," WhiteWIOA=1 and "," ((AmericanIndianAlaskaNativeWIOA+AsianWIOA+BlackAfricanAmericanWIOA+NativeHawaiianOtherPacificIslanderWIOA+WhiteWIOA)=2 or (AmericanIndianAlaskaNativeWIOA+AsianWIOA+BlackAfricanAmericanWIOA+NativeHawaiianOtherPacificIslanderWIOA+WhiteWIOA)=3 or (AmericanIndianAlaskaNativeWIOA+AsianWIOA+BlackAfricanAmericanWIOA+NativeHawaiianOtherPacificIslanderWIOA+WhiteWIOA)=4 or (AmericanIndianAlaskaNativeWIOA+AsianWIOA+BlackAfricanAmericanWIOA+NativeHawaiianOtherPacificIslanderWIOA+WhiteWIOA)=5 ) and "," HighestEducationalLevelCompletedAtProgramEntryWIOA in (1,2) and "," HighestEducationalLevelCompletedAtProgramEntryWIOA=4 and "," HighestEducationalLevelCompletedAtProgramEntryWIOA=5 and "," HighestEducationalLevelCompletedAtProgramEntryWIOA=6 and "," HighestEducationalLevelCompletedAtProgramEntryWIOA=7 and "," HighestEducationalLevelCompletedAtProgramEntryWIOA=8 and "," "," EligibleVeteranStatus>=1 and "," TransitioningServiceMember=1 and "," LongTermUnemployedAtProgramEntryWIOA=1 and ")

	var a:Array[Array[Any]] = null//Array.ofDim[Any](9, 280)
	/*a(0)(0)="EM-27583-15-60-A-06"
  a(1)(0)="EM-28098-16-60-A-06"
  a(2)(0)="EM-27344-15-60-A-06"
  a(3)(0)="EM-28115-16-60-A-06"
  a(4)(0)="EM-20451-10-60-A-06"
  a(5)(0)="MI-29684-16-60-A-06"
  a(6)(0)="EM-30387-17-60-A-06"
  a(7)(0)="EM-24449-13-60-A-06"
  a(8)(0)="EM-25889-14-60-A-06"*/


	def main(args: Array[String]) {
		getStreamingObj(args)
	}

	def getStreamingObj(args: Array[String]): Unit = {

		s3path = args(0)

		S3_OUTGOING_BUCKET = args(1)
		S3_OUTGOING_FOLDER = args(2)
		annualId = args(4)
		aprId=args(3)
		var iD = annualId.toInt
		programName = (s3path.split('/')(4)).split('-')(2)
		fileName = (s3path.split('/')(4))
		newFileName=fileName.replace(fileName.split('-')(0),aprId)
		println("newFileName++++++++++++++++++++++++"+newFileName)

		var values = List[List[String]]()


		if (iD == 1) {
			values = List(
				List("Participant Characteristics", "2016-07-01 00:00:00", "2017-06-30 00:00:00"),
				List("Veterans Priority of Service", "2016-07-01 00:00:00", "2017-06-30 00:00:00"),
				List("Number of Reportable Individuals", "2016-07-01 00:00:00", "2017-06-30 00:00:00"),
				List("Measurable Skill Gains", "2016-07-01 00:00:00", "2017-06-30 00:00:00"),
				List("Effectiveness in Serving Employers: Employer Penetration", "2016-07-01 00:00:00", "2017-06-30 00:00:00"),
				List("Exiter Characteristics", "2016-07-01 00:00:00", "2017-03-31 00:00:00")
			)
		}
		val schemaCohorts = "Measure,CoveredDataStartDate,CoveredDataEndDate"
		val rows = values.map { x => Row(x: _*) }
		val rdd = spark.sparkContext.makeRDD(rows)
		val schema = StructType(schemaCohorts.split(",", -1).map(fieldName => StructField(fieldName, StringType, true)))
		val periods = spark.createDataFrame(rdd, schema)

		periods.createOrReplaceTempView("periods")



		val participantCharacteristics = spark.sql(
			"""
							SELECT CoveredDataStartDate, CoveredDataEndDate
							FROM periods
							WHERE Measure="Participant Characteristics"
							""")

		val
		veteransPriorityService=spark.sql(
			"""
							SELECT CoveredDataStartDate, CoveredDataEndDate
							FROM periods
							WHERE Measure="Veterans Priority of Service"
							""")

		val numberReportableIndividuals=spark.sql(
			"""
							SELECT CoveredDataStartDate, CoveredDataEndDate
							FROM periods
							WHERE Measure="Number of Reportable Individuals"

							""")

		val measurableSkillGains=spark.sql(
			"""
							SELECT CoveredDataStartDate, CoveredDataEndDate
							FROM periods
							WHERE Measure="Measurable Skill Gains"

							""")

		val effectivenessServingEmployersEmployerPenetration=spark.sql(
			"""
							SELECT CoveredDataStartDate, CoveredDataEndDate
							FROM periods
							WHERE Measure="Effectiveness in Serving Employers: Employer Penetration"

							""")

		val effectivenessServingEmployersRepeatBusinessCustomers=spark.sql(
			"""
							SELECT CoveredDataStartDate, CoveredDataEndDate
							FROM periods
							WHERE Measure="Effectiveness in Serving Employers: Repeat Business Customers"

							""")

		val exiterCharacteristics=spark.sql(
			"""
							SELECT CoveredDataStartDate, CoveredDataEndDate
							FROM periods
							WHERE Measure="Exiter Characteristics"

							""")

		val employmentRateSecondQuarterAfterExit=spark.sql(
			"""
							SELECT CoveredDataStartDate, CoveredDataEndDate
							FROM periods
							WHERE Measure="Employment Rate Second Quarter After Exit"

							""")

		val medianEarningsSecondQuarterAfterExit=spark.sql(
			"""
							SELECT CoveredDataStartDate, CoveredDataEndDate
							FROM periods
							WHERE Measure="Median Earnings Second Quarter After Exit"

							""")

		val employmentRateFourthQuarterAfterExit=spark.sql(
			"""
							SELECT CoveredDataStartDate, CoveredDataEndDate
							FROM periods
							WHERE Measure="Employment Rate Fourth Quarter After Exit"

							""")

		val

		credentialAttainmentRate=spark.sql(
			"""
							SELECT CoveredDataStartDate, CoveredDataEndDate
							FROM periods
							WHERE Measure="Credential Attainment Rate"

							""")

		val effectivenessServingEmployersRetentionwithSame=

			spark.sql(
				"""
							SELECT CoveredDataStartDate, CoveredDataEndDate
							FROM periods
							WHERE Measure="Effectiveness in Serving Employers: Retention with the Same"

							""")




		if (!participantCharacteristics.rdd.isEmpty) {
			participantCharacteristicsStartDate = getStartDate(participantCharacteristics)
			participantCharacteristicsEndDate = getEndDate(participantCharacteristics)
		}
		if (!veteransPriorityService.rdd.isEmpty) {
			veteransPriorityServiceStartDate = getStartDate(veteransPriorityService)
			veteransPriorityServiceEndDate = getEndDate(veteransPriorityService)
		}
		if (!numberReportableIndividuals.rdd.isEmpty) {
			numberReportableIndividualsStartDate = getStartDate(numberReportableIndividuals)
			numberReportableIndividualsEndDate = getEndDate(numberReportableIndividuals)
		}
		if (!measurableSkillGains.rdd.isEmpty) {
			measurableSkillGainsStartDate = getStartDate(measurableSkillGains)
			measurableSkillGainsEndDate = getEndDate(measurableSkillGains)
		}
		if (!effectivenessServingEmployersEmployerPenetration.rdd.isEmpty) {
			effectivenessServingEmployersEmployerPenetrationStartDate = getStartDate(effectivenessServingEmployersEmployerPenetration)
			effectivenessServingEmployersEmployerPenetrationEndDate = getEndDate(effectivenessServingEmployersEmployerPenetration)
		}
		if (!effectivenessServingEmployersRepeatBusinessCustomers.rdd.isEmpty) {
			effectivenessServingEmployersRepeatBusinessCustomersStartDate = getStartDate(effectivenessServingEmployersRepeatBusinessCustomers)
			effectivenessServingEmployersRepeatBusinessCustomersEndDate = getEndDate(effectivenessServingEmployersRepeatBusinessCustomers)
		}
		if (!exiterCharacteristics.rdd.isEmpty) {
			exiterCharacteristicsStartDate = getStartDate(exiterCharacteristics)
			exiterCharacteristicsEndDate = getEndDate(exiterCharacteristics)
		}
		if (!employmentRateSecondQuarterAfterExit.rdd.isEmpty) {
			employmentRateSecondQuarterAfterExitStartDate = getStartDate(employmentRateSecondQuarterAfterExit)
			employmentRateSecondQuarterAfterExitEndDate = getEndDate(employmentRateSecondQuarterAfterExit)
		}
		if (!medianEarningsSecondQuarterAfterExit.rdd.isEmpty) {
			medianEarningsSecondQuarterAfterExitStartDate = getStartDate(medianEarningsSecondQuarterAfterExit)
			medianEarningsSecondQuarterAfterExitEndDate = getEndDate(medianEarningsSecondQuarterAfterExit)
		}
		if (!employmentRateFourthQuarterAfterExit.rdd.isEmpty) {
			employmentRateFourthQuarterAfterExitStartDate = getStartDate(employmentRateFourthQuarterAfterExit)
			employmentRateFourthQuarterAfterExitEndDate = getEndDate(employmentRateFourthQuarterAfterExit)
		}
		if (!credentialAttainmentRate.rdd.isEmpty) {
			credentialAttainmentRateStartDate = getStartDate(credentialAttainmentRate)
			credentialAttainmentRateEndDate = getEndDate(credentialAttainmentRate)
		}
		if (!effectivenessServingEmployersRetentionwithSame.rdd.isEmpty) {
			effectivenessServingEmployersRetentionwithSameStartDate = getStartDate(effectivenessServingEmployersRetentionwithSame)
			effectivenessServingEmployersRetentionwithSameEndDate = getEndDate(effectivenessServingEmployersRetentionwithSame)
		}


		var edrvs = spark.read.load(s3path)
		edrvs.
			createOrReplaceTempView("edrvs")
		getConvertedDate(spark)

		var dwgList=edrvs.filter("DwgGrantNumber is not NUll and DwgGrantNumber !=''").select("DwgGrantNumber").distinct().rdd.map(_(0)).collect.toList
		a=Array.ofDim[Any](dwgList.length, 281)
		for(i<- 0 until dwgList.length){
			a(i)(0)=dwgList(i)
		}
		if (participantCharacteristicsStartDate.length()>1) {
			val dfs = Array(
				getRule01(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule02(spark,  participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule03(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule04(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule05(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule06(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule07(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule08(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule09(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule010(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule011(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule012(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule013(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule014(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule015(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule016(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule017(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule018(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule019(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule020(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule021(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule022(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule023(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule024(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule025(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule026(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule027(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule028(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule029(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule030(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule031(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule032(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule033(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule034(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate),
				getRule035(spark, participantCharacteristicsStartDate, participantCharacteristicsEndDate)).par
		}

		if (exiterCharacteristicsStartDate.length()>1) {
				val df1=Array(getRule036(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule037(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule038(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule039(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule040(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule041(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule042(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule043(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule044(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule045(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule046(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule047(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule048(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule049(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule050(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule051(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule052(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule053(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule054(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule055(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule056(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule057(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule058(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule059(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule060(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule061(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule062(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule063(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule064(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule065(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule066(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule067(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule068(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule069(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
			getRule070(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate)).par

		}
if (exiterCharacteristicsStartDate.length()>1) {
val df2=Array(getRule071(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule071(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule072(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule073(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule074(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule075(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule076(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule077(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule078(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule079(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule080(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule081(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule082(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule083(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule084(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule085(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule086(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule087(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule088(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule089(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule090(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule091(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule092(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule093(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule094(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule095(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule096(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule097(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule098(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule099(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule100(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule101(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule102(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule103(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
getRule104(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate),
  getRule105(spark, exiterCharacteristicsStartDate, exiterCharacteristicsEndDate)).par
		}

		if (employmentRateSecondQuarterAfterExitStartDate.length()>1) {


			val df3=Array(getRule106(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
				//getRule05(spark, sqlSubString(a), employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate)
        getRule107(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule108(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule109(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule110(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule111(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule112(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule113(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule114(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule115(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule116(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule117(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule118(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule119(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule120(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule121(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule122(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule123(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule124(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule125(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule126(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule127(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule128(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule129(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule130(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule131(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule132(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule133(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule134(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule135(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule136(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule137(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule138(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule139(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate),
        getRule140(spark, employmentRateSecondQuarterAfterExitStartDate, employmentRateSecondQuarterAfterExitEndDate)).par
		}
		if (employmentRateFourthQuarterAfterExitStartDate.length()>1) {

				val def9=Array(getRule141(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
				//getRule07(spark, sqlSubString(a), employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate)
          getRule142(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule143(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule144(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule145(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule146(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule147(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule148(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule149(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule150(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule151(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule152(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule153(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule154(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule155(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule156(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule157(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule158(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule159(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule160(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule161(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule162(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule163(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule164(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule165(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule166(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule167(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule168(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule169(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule170(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule171(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule172(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule173(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule174(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate),
          getRule175(spark, employmentRateFourthQuarterAfterExitStartDate, employmentRateFourthQuarterAfterExitEndDate)).par

		}
		if (medianEarningsSecondQuarterAfterExitStartDate.length()>1) {

				val df6=Array(getRule176(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule177(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule178(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule179(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule180(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule181(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule182(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule183(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule184(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule185(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule186(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule187(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule188(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule189(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule190(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule191(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule192(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule193(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule194(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule195(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule196(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule197(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule198(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule199(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule200(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule201(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule202(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule203(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule204(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule205(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule206(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule207(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule208(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule209(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate),
          getRule210(spark, medianEarningsSecondQuarterAfterExitStartDate, medianEarningsSecondQuarterAfterExitEndDate)).par

		}
		if (credentialAttainmentRateStartDate.length()>1) {


				val def7= Array(getRule211(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
				//getRule010(spark, sqlSubString(a), credentialAttainmentRateStartDate, credentialAttainmentRateEndDate)
          getRule212(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule213(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule214(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule215(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule216(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule217(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule218(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule219(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule220(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule221(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule222(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule223(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule224(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule225(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule226(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule227(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule228(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule229(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule230(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule231(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule232(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule233(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule234(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule235(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule236(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule237(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule238(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule239(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule240(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule241(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule242(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule243(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule244(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate),
          getRule245(spark, credentialAttainmentRateStartDate, credentialAttainmentRateEndDate)).par






		}
		if (measurableSkillGainsStartDate.length()>1) {


				val def13=Array(getRule245(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
				//getRule012(spark, sqlSubString(a), measurableSkillGainsStartDate, measurableSkillGainsEndDate)
          getRule246(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule247(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule248(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule249(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule250(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule251(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule252(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule253(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule254(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule255(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule256(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule257(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule258(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule259(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule260(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule261(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule262(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule263(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule264(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule265(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule266(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule267(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule268(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule269(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule270(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule271(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule272(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule273(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule274(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule275(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule276(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule277(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule278(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule279(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate),
          getRule280(spark, measurableSkillGainsStartDate, measurableSkillGainsEndDate)).par

		}


		println(newFileName)
		getQPRResults(newFileName)
		println(" [*] WIPS DWG Aggs complete.")
		spark.sparkContext.stop()
	}

	def getQPRResults(newFileName: String): Unit = {
		val rows = a.map(_.map(str=> Option(str).getOrElse(""))).map{x => Row(x:_*)}
		val rdd = spark.sparkContext.makeRDD(rows)
		rdd.repartition(1).saveAsTextFile("s3://" + S3_OUTGOING_BUCKET + "/" + S3_OUTGOING_FOLDER + "/" + newFileName)
		//spark.sparkContext.parallelize(a.toSeq, 1).saveAsTextFile("s3://" + S3_OUTGOING_BUCKET + "/" + S3_OUTGOING_FOLDER + "/" + newFileName)
	}
	def getStartDate(cohortDate: DataFrame): String = {
		return cohortDate.collect().apply(0).get(0).toString.replaceAll("[-]", "").dropRight(9)
	}
	def getEndDate(cohortDate: DataFrame): String = {
		return cohortDate.collect().apply(0).get(1).toString.replaceAll("[-]", "").dropRight(9)
	}


	def getConvertedDate(spark: SparkSession) = {
		spark.udf.register("convetToDate", (word: String) => {
			val format = new SimpleDateFormat("yyyyMMdd")
			var date = new util.Date()
			try {
				date = format.parse(word)
			} catch {
				case e: Exception => date = new SimpleDateFormat("yyyy-MM-dd").parse("1900-01-01")
			}
			new Date(date.getTime())
		})
	}


}
