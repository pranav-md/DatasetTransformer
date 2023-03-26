package sparkassessment.writing

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import io.github.cdimascio.dotenv.Dotenv
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.mockito.Mockito.{mock, verify}
import org.scalatest.{BeforeAndAfterAll, EitherValues, GivenWhenThen}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.mockito.MockitoSugar

class WriteFileDataTest extends AnyWordSpec with BeforeAndAfterAll with EitherValues with GivenWhenThen with Matchers with MockitoSugar {

  import WriteFileDataTest._

  "Data transformer" should {
    "successfully save dataframe as tsv file" in {

      // Define the number of rows and columns for the DataFrame.
      val numRows = 100

      // Create a sequence of column names.
      val fileDataSchema = StructType(Seq(
        StructField("key", IntegerType, true),
        StructField("value", IntegerType, true)
      ))

      val rows = Seq.tabulate(numRows)(i => Row(i, i * 2))

      val someDF = spark.createDataFrame(
        spark.sparkContext.parallelize(rows),
        StructType(fileDataSchema)
      )
      writeFileData.writeProcessedFile(someDF)
      verify(logger).debug(s"File has successfully uploaded in Amazon S3 in path ${outputPath}")

    }

    "Throws exception if S3 bucket is not found" in {

      // Define the number of rows and columns for the DataFrame.
      val numRows = 100

      // Create a sequence of column names.
      val fileDataSchema = StructType(Seq(
        StructField("key", IntegerType, true),
        StructField("value", IntegerType, true)
      ))

      val rows = Seq.tabulate(numRows)(i => Row(i, i * 2))

      val someDF = spark.createDataFrame(
        spark.sparkContext.parallelize(rows),
        StructType(fileDataSchema)
      )
      writeFileDataWithInvalidBucket.writeProcessedFile(someDF)
      verify(logger).error(s"Got exception while uploading output.tsv on Amazon S3")

    }
  }
}

object WriteFileDataTest {

  // Create a Spark session object.
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkAssessment.com")
    .getOrCreate()

  val AWS_ACCESS_KEY_ID = Dotenv.load().get("AWS_ACCESS_KEY_ID")
  val AWS_SECRET_PRIVATE_KEY = Dotenv.load().get("AWS_SECRET_PRIVATE_KEY")

  val outputPath = "sparkassessment/output files/"
  val outputPathForInvalidBucket = "randombucketname/output files/"
  val awsCredentials = DefaultAWSCredentialsProviderChain.getInstance().getCredentials()

  val s3Client = AmazonS3ClientBuilder.standard().withRegion("eu-north-1")
    .build()

  val transferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build()
  val logger = mock(classOf[Logger])

  val writeFileData = WriteFileData(outputPath, transferManager, logger)

  val writeFileDataWithInvalidBucket = WriteFileData(outputPathForInvalidBucket, transferManager, logger)


}
