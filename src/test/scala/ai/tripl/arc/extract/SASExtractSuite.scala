package ai.tripl.arc

import java.net.URI
import java.util.UUID

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.util._

class DeltaLakeExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val outputView = "dataset"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    import spark.implicits._
  }

  after {
    session.stop()
  }

  test("SASExtract: end-to-end") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)


    val schema = StructType(
      StructField("SURVYEAR", StringType, true) ::
      StructField("LEAID", StringType, true) ::
      StructField("NAME", StringType, true) ::
      StructField("PHONE", StringType, true) ::
      StructField("LATCOD", DecimalType(9, 6), true) ::
      StructField("LONCOD", DecimalType(11, 6), true) ::
      StructField("PKTCH", DecimalType(8, 2), true) ::
      StructField("AMPKM", LongType, true) :: Nil
    )
    val expected = spark.read.option("header", true).schema(schema).csv(getClass.getResource("/ag121a_supp_sample.csv").toString)

    val conf = s"""{
      "stages": [      
        {
          "type": "SASExtract",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${getClass.getResource("/ag121a_supp_sample.sas7bdat").getPath}",
          "outputView": "${outputView}",
          "options": {
            "inferDecimal": true,
            "inferLong": true,
            "maxSplitSize": 1000000
          }
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val actual = ARC.run(pipeline)(spark, logger, arcContext).get
        assert(TestUtils.datasetEquality(expected, actual))
      }
    }
  }   
  
  test("SASExtract: end-to-end compressed") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val schema = StructType(
      StructField("Date", DateType, true) ::
      StructField("Week", DoubleType, true) ::
      StructField("Month", DoubleType, true) ::
      StructField("Day_of_Month", DoubleType, true) ::
      StructField("Weekday", StringType, true) ::
      StructField("Quarter", DoubleType, true) ::
      StructField("Year", DoubleType, true) ::
      StructField("Agency", StringType, true) ::
      StructField("Separation_Reason", StringType, true) ::
      StructField("Loan_Type", StringType, true) ::
      StructField("State_Type", StringType, true) ::
      StructField("Voluntary_Separation_Units", DoubleType, true) ::
      StructField("Assigned_Units", DoubleType, true) ::
      StructField("HDL_Assigned_Units", DoubleType, true) ::
      StructField("Marketed_Units", DoubleType, true) ::
      StructField("Contact_Units", DoubleType, true) ::
      StructField("HDL_Call_Units", DoubleType, true) ::
      StructField("App_Units", DoubleType, true) ::
      StructField("Lock_Units", DoubleType, true) ::
      StructField("Fund_Units", DoubleType, true) ::
      StructField("GNMA_Fund_Units", DoubleType, true) ::
      StructField("Voluntary_Separation_Balances", DoubleType, true) ::
      StructField("Assigned_Balances", DoubleType, true) ::
      StructField("HDL_Assigned_Balances", DoubleType, true) ::
      StructField("Marketed_Balances", DoubleType, true) ::
      StructField("Contact_Balances", DoubleType, true) ::
      StructField("HDL_Call_Balances", DoubleType, true) ::
      StructField("App_Balances", DoubleType, true) ::
      StructField("Lock_Balances", DoubleType, true) ::
      StructField("Fund_Balances", DoubleType, true) ::
      StructField("GNMA_Fund_Balances", DoubleType, true) ::
      StructField("Origination_Balances", DoubleType, true) ::
      StructField("id", DoubleType, true) :: Nil
    )
    val expected = spark.read.option("header", true).schema(schema).csv(getClass.getResource("/recapture_test_compressed.csv").toString)

    val conf = s"""{
      "stages": [      
        {
          "type": "SASExtract",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${getClass.getResource("/recapture_test_compressed.sas7bdat.gz").getPath}",
          "outputView": "${outputView}",
          "options": {
            "maxSplitSize": 1000000
          }
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val actual = ARC.run(pipeline)(spark, logger, arcContext).get
        assert(TestUtils.datasetEquality(expected, actual))
      }
    }
  }   

}
