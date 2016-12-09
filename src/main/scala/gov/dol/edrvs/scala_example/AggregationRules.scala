package gov.dol.edrvs.scala_example


/**
	* Created by eravelly.shiva.kumar on 6/14/2016.
	*/

import java.text.SimpleDateFormat
import java.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.DataFrame
import java.io.File
import java.sql.Date
import java.util.Calendar
import org.apache.spark.sql.{SQLContext, Row}

object AggregationRules {

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
		val schemaPIRL = "rownum,obsnumber,uniqueindividualidentifier,statecoderesidence,countrycoderesidence,zipcoderesidence,econlabormarketareaphyloc,specialprojectid1,specialprojectid2,specialprojectid3,etalocalworkforceboardcode1,etalocalworkforceboardcode2,etalocalworkforceboardcode3,birthdate,sex,disabilitystatus,disabilitycategory,iwdsddas,iwdlsmhas,iwdmhcbss,iwdws,iwdtcesr,iwdfc,hispanic,nativeamerican,asian,black,pacificislander,white,veteranstatus,eveteranstatus,veterancampaign,veterandisabled,militaryseperationdate,veterantransition,coveredpersonentry,homelessveteran,homelessveteranprogramparticipant,homelessveteranprogramgrantee,othersignificantbarrieremployment,employmentstatus,uceligiblestatus,longtermunemployed,occupationcodebeforeempmt,empindustrycode1stqtrprior,empindustrycode2ndqtrprior,empindustrycode3rdqtrprior,highestgradecompleted,highesteducationallevelcompleted,schoolstatusatparticipation,dateofactualdislocation,mostrecentdateofqualifyingseperation,tenurewithemployeratseperation,msfwdesignation,tanf,exhaustingtanfwithin2years,ssi_ssdi,snap,otherpublicassistancerecipient,pregnantorparentingyouth,youthadditionalassistance,fostercareyouth,homelessindvidualchildrenyouth,exoffender,lowincome,englishlanguagelearner,lowlevelofliteracy,culturalbarriers,singleparent,displacedhomemaker,migrantseasonalfarmworkerstatus,dateofprogramentry,dateofprogramexit,datefirstcasemanagementempservice,adult,dislocatedworker,youth,dateoffirstyouthservice,receipientincumbentworkertraining,rapidresponse,rapidresponseadditionalasst,adulteducation,jobcorps,farmworkerjobs,indianprograms,veteranprograms,taapetitionnumber,vocationaleducation,vocationalrehabilitation,wagnerpeyser,youthbuild,seniorcommunityserviceempporgram,etsrsnap,otherwiaornonwiaprograms,otherreasonsofexit,taaapplicationdate,dateoffirsttaabenefitorservice,liableagentstateidentifier,dateofeligibilitydetermination,determinedeligible,benefitunderpriorcertificationlast10years,payforperformance,registeredapprenticeshipprogram,nationaldislocatedworkergrants,dateoffirstdwgservice,rapidresponseeventnumber,accountabilityexitstatus,reroadult,reroyouth,h1b,iwdiepp,ids504p,firstselfservice,firststaffassisted,receivedselfservices,staffassistedservices,dateofmrcareerservices,mrdrsasdvop,datereferreddeptvavrep,mrdrioaselfservice,workforceinfoselfservice,careerguidance,workforceinfostaffassisted,jobsearch,referredemployment,mrdrfederaltraining,mrdpfederaltraining,mrdrfederaljob,mrdrfederalcontractorjob,mrdefederaljob,mrdefederalcontractorjob,mrdrunemploymentinsuranceclaimassistance,mrdrotherfederalorstateassistance,referredjvsgservices,referreddepartmentvaservices,mrdrstaffassistedbcsother,firstindividualizedcareerservice,mrdrindividualizedcareerservice,dateindividualempoymentplancreated,mrdrinternshiporworkexperienceopp,typeofworkexperience,datereceivedfinancialliteracyservices,daterecievedenglishassecondlangservices,receivedprevocactivities,transistionaljobs,mrdricsdvop,mrdrjsadvop,mrdredvop,mrdrfederaltrainingdvop,mrdrfederaljobdvop,mrdrfcjdvop,mrdrosab,mrdrcgsdvop,mrdefjdvop,mrdefcjdvop,receivedtraining,etpnamets1,dateenteredtraining1,typeoftrainingservice1,etpprogramofstudy,etpcipcode,otccode1,training_completed1,dateexittraining1,dateenteredtraining2,typeoftrainingservice2,otccode2,training_completed2,dateexittraining2,dateentertraining3,typeoftrainingservice3,otccode3,training_completed3,dateexittraining3,establishedita,pellgrantrecipient,waiverfromtrainingreq,casemanagementandreempservice,datewaivertrainingreqissued,currentqtrtrainingexpenditures,totaltrainingexpenditures,amountoftrainingcostsoverpayment,trainingcostsoverpaymentwaiver,distancelearning,parttimetraining,adverselyaffectedincumbentworker,trainingleadingtoassociatedegree,pseducationprogramparticipation,enrolledsecondaryeducationprogram,mrdreas,mrdrasss,mrdrweo,dppeeortplrpc,mrdreocwp,mrdldo,mrdrss,mrdrams,mrdrcgcs,mrdryfs,mrdyrest,mrdyrsplmiei,mrdyrpstpa,datecompletionofyoutservices,recievedneedsrelatedpayments,mrdrrrs,mrdrfus,subsistencewhiletraining,taajobsearchallowancecount,taajobsearchallowancecurqtrcosts,taajobsearchallowancetotalcosts,taadaterelocationallowanceapproved,taarelocationallowancecostscurqtr,taarelocationallowancetotalcost,trafirstbasicpaymentreceiveddate,traweekspaidinqtr,tratotalweekspaidcumulative,traamountpaidcurqtr,tratotalamountpaid,firstadditionaltrapayment,weekspaidthisqtradditionaltra,traadditionaltotalweekspaidcum,traadditionalamtpaidthisqtr,traadditionaltotalamtpaid,firstremedialorprereqtrapayment,weekspaidthisqtrremeidalorprereq,totalweekspaidthisqtrremedialorprepreq,traamtpaidthisqtrremedialorprereq,tratotalamtpaidremedialorprereq,firstcompletiontrapayment,tracompletionweekspaidthisqtr,tracompletiontotalweekspaidcum,tracompletionamtpaidcurqtr,tracompletiontotalamtpaid,traoverpayment,amttraoverpayment,traoverpaymentwaiver,firstaorrtaapayment,numaorrtaapaymentscurqtr,curqtraorrtaapayments,numaorrtaapaymentstotal,totalamtpaidaorrtaa,frequencyofpayments,maxaorrtaabenefitreached,aorrtaaoverpaymentcurqtr,amtofaorrtaaoverpayment,aorrtaaoverpaymentwaiver,employed1stqtrafterexitqtr,typeempmatch1stqtrafterexitqtr,employed2ndqtrafterexitqtr,typeempmatch2ndqtrafterexitqtr,employed3rdqtrafterexitqtr,typeempmatch3rdqtrafterexitqtr,employed4thqtrafterexitqtr,typeempmatch4thqtrafterexitqtr,emprelatedtotraining2ndqtrafterexit,recalledbylayoffemployer,occupationalcode,nontraditionalemployment,occupationalcodeemp2ndqtrafterexitqtr,occupationalcodeemp4thqtrafterexitqtr,empmtindustrycodexqtr1,empmtindustrycodexqtr2,empmtindustrycodexqtr3,empmtindustrycodexqtr4,employerretention2ndand4thqtr,priorwages3,priorwages2,priorwages1,exitwages1stqtr,exitwages2ndqtr,exitwages3rdqtr,exitwages4thqtr,recognizedcredentialtype,attainedrecognizedcredential,recognizedcredentialtype2,attainedrecognizedcredential2,recognizedcredentialtype3,attainedrecognizedcredential3,mrmeasurableskillgainseducationalfunc,mrmeasurableskillgainspstranscriptorrc,mrmeasurableskillgainssectranscriptorrc,mrmeasurableskillgainstrainingmilestone,mrmeasureableskillsprogression,mrdeetprecognizedpscredoremployment,schoolstatusexit,youth2ndqtrplacement,youth4thqtrplacement,categoryofassessment,dateofpretestscore,pretestscore,educationalfunclevel,mostrecentposttestscore,posttestscore,educationalfunclevelposttest,dwgservicescompletion,empatdwgservicescompletion,dwggrantnum,servicesreceiveddrdwg,underemployedworker,previousqtrreceivedcms,mrdreceivedassessmentservices,previousqtrreceivedassessmentservices,previousqtrreceivedsupportservices,mrdreceivedspecializedparticipantservices,preqtrreceivedspecializedparticipantservices,preqtrparticipatedworkexperience,ttstrainingactivityprimary1,ttstrainingactivitysecondary1,ttstrainingactivityteritiary1,ttstrainingactivityprimary2,ttstrainingactivitysecondary2,ttstrainingactivityteritiary2,ttstrainingactivityprimary3,ttstrainingactivitysecondary3,ttstrainingactivityteritiary3,dateenteredempdiscretionarygrants,icumbentworkersretainedcurpos,icumbentworkersanpcne1stqtrpc,icumbentworkersrcp2ndqtrpc,icumbentworkersanpcne2ndqtrtpc,icumbentworkersrcp3rdqtrpc,icumbentworkersanpcne3rdqtrtpc,jcui,fipscsjl,fbar,hwp,hww,selfemployment,enteredmilitaryservice,epaorrap,exitcategory,ratrans,rahealthcare,familycareincludingchildcare,rahousginasstservices,ranutritionalasst,ratranslationinterpreationservices,rastaffassisted,rasafetytraining,workexpfundedgrant167,ojtfundedgrant167,ibostfundedgrant167,ostfundedgrant167,bstfundedgrant167,lackstransportation,longtermagriculturalemp,lackssignificantworkhistory,sixmonthspreprogramearningspda,totalpreprogramearnings12monthedp,dependentsunder18,concurparticipationetpdepthud,eligibilitydetermination,familitystatusnjfp,nfjpgrantenrollment,beartrackssoftcurver,tribalaffiliation,publicasstrecipient,inworkreleaseprogram,empstatusatincarceration,citizenshipstatus,authorizedtowork,alchoholordrugabuseatenrollment,significanthealthissues,medicalbenefitssri,mentalhealthtreatment,childsupportobligationenrollment,childsupportobligationamtenrollment,medicalbenefitstypepriortoincarceration,criminaljusticsystemidentifer,incarceratedatprogramentry,exitcorrectionalfacilityincarceration,anticipatedreleasefromincarceration,postreleasestatus,mrtypeoffense,distiiprilocation,priorcriminalhistory,prereleaseservices,releaseconditions,housingstatusatsixmonths,housingstatusenrollment,alchoholdrugabusesixmonthsenrollment,individualdevplangoaltype,expectedduratuionvost,expectedcostvost,hourlywageatempplacement,hoursworkedfirstfullweek,dateenteredpostseceducation,rearrestedwithin12monthreleasenc,rearrestedforpreviouscrime,reincarceratedppovts,notrearrested,datearrestedneworpreviouscrime,convictedforneworpreviouscrime,datereincarcerate,datechargesdropped,dateoffollowup,modeofcontact,successfulfollowup,hourlywageatfollowup,secschoolenrollmentstatusatarrest,secschoolenrollmentstatusatep,youthoffenderstatus,drcorrectionalfacilityorprobabtion,selectiveserviceregistration,voterregistration,driverslicense,servicecode,firstdateofservice,cdwohp,recordsexpunded,recordssealed,hoursperweekvt,receivedmentoringsixormoremonths,initialplacementsubsidizedjob,postseceducationortrainingplacement,fulltimeorparttimeeducation,lastdateofeducation,hourlytrainingwage,dateofreturnorregulathighschool,reached12monthrhsorsepwhs,remainedinregularsecschool,dateentereddegreeorcertificateprogram,datearrestedfornewcrimeafterenrollment,convictedfornewcrimeafterenrollment,typeofcrime,reached12monthreleasecorrectionalfacility,convictedfornewcrimewithin12monthscforpb,incarceratedfprmewcrimeafterenrollment,reasonsleavingprogramearly,employmentandeducationstatusatfollowup,hoursworkedinfirstfullweek,educationalstatusatfollowup,enrolledincontinuationoraltschool,enrolledinhighschoolequivalencycourses,attendingpostsecftorpt,enrolledinvt,hoursattendingvtfirstweek,hourlywagesvt,publicasstleavingprogram,arrestedfornewcrimefollowup,datearrestedfornewcrimefollowup,convictedfornewcrimefollowup,incarceratednewcrimefollowup,housingstatusfollowup,recievedmentoringfollowup,receivedsecschooldiplomaslp,constructionplusgrantee,cohortidentifer,teamidentifer,completedmentaltoughnesscomponent,childrenlivingwithparticipant,otherdependentslivingwithparticipant,migrantyouth,offender,secondaryschooldropout,childofincarceratedparentorlegalguardian,healthissues,occupationalatenrollment,hoursworkedatenrollment,avghourlywageatenrollment,jobstartdate,housingstatus,ssn,wibname,officename,casemanager,userfield1,userfield2"
		import org.apache.spark.sql.types.{StructType,StructField,StringType}
		val schema = StructType(schemaPIRL.split(",", -1).map(fieldName => StructField(fieldName, StringType, true)))
		val rowRDD = edrvs.map(_.split(",", -1)).map(p => Row.fromSeq(p))
		val testPRS = sqlContext.createDataFrame(rowRDD, schema)
		testPRS.registerTempTable("wioaips1")

		sqlContext.udf.register("convetToDate", (word: String) => {
			val format = new SimpleDateFormat("yyyymmdd")
			var date = new util.Date()
			try {
				date = format.parse(word)
			} catch {
				case e: Exception => date = new SimpleDateFormat("yyyy-MM-dd").parse("1900-01-01")
			}
			new Date(date.getTime())
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

    val uIDDups = sqlContext.sql(
      """select
        			|uniqueindividualidentifier
        			|from wioaips1
        			|where length(uniqueindividualidentifier)>0
        			|group by uniqueindividualidentifier
        			|having count(uniqueindividualidentifier)>1""".stripMargin)
    uIDDups.registerTempTable("uIDDups")//df with repeaed UID column

    val uIDDupsTable = sqlContext.sql(
      """select a.rownum, a.uniqueindividualidentifier, a.coveredpersonentry, a.dateofprogramentry, a.dateofprogramexit, a.datefirstcasemanagementempservice, a.taapetitionnumber
        					|from wioaips1 a, uIDDups b
        					|where a.uniqueindividualidentifier=b.uniqueindividualidentifier""".stripMargin)
    uIDDupsTable.registerTempTable("uIDDupsTable")//whole df with repeated UID's


    val preDups2 = sqlContext.sql(
      """select *
        					|from uIDDupsTable
        					|where length(dateofprogramentry)>0 and length(dateofprogramexit)>0""".stripMargin)
    preDups2.registerTempTable("preDups2")//df filtered on empty dateofprogramentry and dateofprogramexit


    val preDups3 = sqlContext.sql(
      """select *
        					|from uIDDupsTable
        					|where length(dateofprogramentry)>0""".stripMargin)
    preDups3.registerTempTable("preDups3")//df filtered on empty dateofprogramentry

    //Duplicate check 1
    var dups1 = sqlContext.sql(
      """SELECT
        					|uniqueindividualidentifier,
        					|coveredpersonentry
        					|from uIDDupsTable
        					|where length(coveredpersonentry)>0
        					|group by uniqueindividualidentifier, coveredpersonentry
        					|having count(uniqueindividualidentifier) > 1 and count(coveredpersonentry) > 1""".stripMargin)
    dups1.registerTempTable("dups1")

    //Duplicate Check 2
    var dups2 = sqlContext.sql(
      """SELECT
        					|distinct a.uniqueindividualidentifier,
        					|a.dateofprogramentry,
        					|a.dateofprogramexit
        					|FROM preDups2 a, preDups2 b
        					|WHERE a.uniqueindividualidentifier = b.uniqueindividualidentifier AND a.rownum<>b.rownum
        					|AND (convetToDate(a.dateofprogramentry) BETWEEN convetToDate(b.dateofprogramentry) AND date_add(convetToDate(a.dateofprogramexit),90))
        					|OR (convetToDate(a.dateofprogramexit) BETWEEN convetToDate(b.dateofprogramentry) AND date_add(convetToDate(a.dateofprogramexit),90))""".stripMargin)
    dups2.registerTempTable("dups2")

    //Duplicate Check 3 part-1
    var dups3Part1 = sqlContext.sql(
      """select
        					|uniqueindividualidentifier,
        					|max(dateofprogramentry) AS dateofprogramentry
        					|from preDups3
        					|group by uniqueindividualidentifier
        					|having count(uniqueindividualidentifier) > 1""".stripMargin)
    dups3Part1.registerTempTable("dups3Part1")

    //Duplicate Check 3 part-2
    var dups3Part2 = sqlContext.sql(
      """SELECT
        					|a.uniqueindividualidentifier,
        					|a.coveredpersonentry,
        					|a.dateofprogramentry,
        					|a.datefirstcasemanagementempservice,
        					|a.dateofprogramexit
        					|FROM preDups3 a LEFT JOIN dups3Part1 b ON a.uniqueindividualidentifier = b.uniqueindividualidentifier
        					|AND a.dateofprogramentry<>b.dateofprogramentry
        					|where length(a.dateofprogramexit)=0""".stripMargin)
    dups3Part2.registerTempTable("dups3Part2")

    //Duplicate Check 3 part-3
    var dups3Part3 = sqlContext.sql(
      """select
        					|distinct a.uniqueindividualidentifier,
        					|a.dateofprogramentry
        					|from dups3Part1 a, dups3Part2 b
        					|where a.uniqueindividualidentifier=b.uniqueindividualidentifier""".stripMargin)
    dups3Part3.registerTempTable("dups3Part3")


    /*var joinDups = sqlContext.sql(
        """SELECT
        |a.rownum,
        |a.uniqueindividualidentifier,
        |a.coveredpersonentry,
        |a.dateofprogramentry,
        |a.dateofprogramexit,
        |a.taapetitionnumber,
        |CASE WHEN (a.uniqueindividualidentifier=b.uniqueindividualidentifier AND a.coveredpersonentry=b.coveredpersonentry)
        |THEN
        |'Duplicate'
        |ELSE
        |''
        |END AS Duplicate1,
        |CASE WHEN (a.uniqueindividualidentifier=c.uniqueindividualidentifier AND a.dateofprogramentry=c.dateofprogramentry AND a.dateofprogramexit=c.dateofprogramexit)
        |THEN
        |'Duplicate'
        |ELSE
        |''
        |END AS Duplicate2,
        |CASE WHEN ((a.uniqueindividualidentifier=d.uniqueindividualidentifier AND a.dateofprogramentry=d.dateofprogramentry AND a.coveredpersonentry=d.coveredpersonentry) OR (a.uniqueindividualidentifier=e.uniqueindividualidentifier AND a.dateofprogramentry=e.dateofprogramentry))
        |THEN
        |'Duplicate'
        |ELSE
        |''
        |END AS Duplicate3
        |FROM uIDDupsTable a
        |LEFT JOIN dups1 b ON a.uniqueindividualidentifier = b.uniqueindividualidentifier AND a.coveredpersonentry=b.coveredpersonentry
        |LEFT JOIN dups2 c ON a.uniqueindividualidentifier=c.uniqueindividualidentifier AND a.dateofprogramentry=c.dateofprogramentry AND a.dateofprogramexit=c.dateofprogramexit
        |LEFT JOIN dups3Part2 d ON a.uniqueindividualidentifier=d.uniqueindividualidentifier AND a.dateofprogramentry=d.dateofprogramentry AND a.coveredpersonentry=d.coveredpersonentry
        |LEFT JOIN dups3Part3 e ON a.uniqueindividualidentifier=e.uniqueindividualidentifier AND a.dateofprogramentry=e.dateofprogramentry""".stripMargin)*/

    var joinDups = sqlContext.sql(
      """SELECT
        |distinct a.rownum,
        |a.uniqueindividualidentifier,
        |a.coveredpersonentry,
        |a.dateofprogramentry,
        |a.dateofprogramexit,
        |a.taapetitionnumber
        |FROM wioaips1 a
        |RIGHT JOIN dups1 b ON a.uniqueindividualidentifier = b.uniqueindividualidentifier AND a.coveredpersonentry=b.coveredpersonentry
        |RIGHT JOIN dups2 c ON a.uniqueindividualidentifier=c.uniqueindividualidentifier AND a.dateofprogramentry=c.dateofprogramentry AND a.dateofprogramexit=c.dateofprogramexit
        |RIGHT JOIN dups3Part2 d ON a.uniqueindividualidentifier=d.uniqueindividualidentifier AND a.dateofprogramentry=d.dateofprogramentry AND a.coveredpersonentry=d.coveredpersonentry
        |RIGHT JOIN dups3Part3 e ON a.uniqueindividualidentifier=e.uniqueindividualidentifier AND a.dateofprogramentry=e.dateofprogramentry""".stripMargin)

    joinDups.registerTempTable("joinDups")


    val taaDups = sqlContext.sql("SELECT a.rownum, a.uniqueindividualidentifier, a.dateofprogramentry, a.dateofprogramexit, a.taapetitionnumber from joinDups a " +
      "INNER JOIN (select taapetitionnumber, count(*) as count from joinDups GROUP BY taapetitionnumber having count(*)>1) taaTable on a.taapetitionnumber=taaTable.taapetitionnumber where length(a.taapetitionnumber)>0 AND length(taaTable.taapetitionnumber)>0")
    println(" [+] - " + taaDups.printSchema())
    println(" [+] - " + taaDups.count())

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