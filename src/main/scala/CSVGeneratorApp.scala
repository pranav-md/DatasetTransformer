package sparkassessment

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import sparkassessment.reading.ReadFileData
import transformation.DataTransformer
import writing.WriteFileData

object CSVGeneratorApp extends App {

  /**
   * Input and ouput file path as input parameters
   */
  val inputPath = args(0)
  val outputPath = args(1)

  /**
   * Initialising Spark session
   */
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkAssessment")
    .getOrCreate()


  /**
   * Get .aws/credentials
   */
  val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
  val AWS_ACCESS_KEY_ID = credentials.getAWSAccessKeyId()
  val AWS_SECRET_PRIVATE_KEY = credentials.getAWSSecretKey()

  /**
   * Setting AWS credentials in spark session context to read S3 content
   */
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", AWS_SECRET_PRIVATE_KEY)
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.multiobjectdelete.enable", "false")

  val s3Client = AmazonS3ClientBuilder.standard().withRegion("eu-north-1")
    .build()
  val transferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build()
  val logger = Logger.getLogger(classOf[WriteFileData])

  /**
   * Initialise instances of case classes
   */
  val readFileData = new ReadFileData(spark, inputPath)
  val dataTransformer = new DataTransformer()
  val writeFileData = WriteFileData(outputPath, transferManager, logger)

  writeFileData.writeProcessedFile(dataTransformer.transformData(readFileData.getFileData()))

  spark.stop()

}
