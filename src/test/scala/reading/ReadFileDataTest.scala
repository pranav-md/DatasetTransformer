package reading

import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, EitherValues, GivenWhenThen}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.mockito.MockitoSugar
import sparkassessment.reading.ReadFileData

class ReadFileDataTest extends AnyWordSpec with BeforeAndAfterAll with EitherValues with GivenWhenThen with Matchers with MockitoSugar {

  import ReadFileDataTest._


  "Data transformer" should {
    "return DataFrame of given data" in{

       val output =  readFileData.getFileData()
      output.collect() should contain theSameElementsAs Seq(Row(1, 2), Row(1, 2), Row(1, 2), Row(2, 3), Row(2, 5),
        Row(2, 3), Row(1, 2), Row(1, 1), Row(1, 1), Row(2, 3), Row(2, 3), Row(2, 3),
        Row(3, 2), Row(3, 2), Row(3, 2))
    }
  }

}

object ReadFileDataTest {


  implicit val AWS_ACCESS_KEY_ID = Dotenv.load().get("AWS_ACCESS_KEY_ID")
  implicit val AWS_SECRET_PRIVATE_KEY = Dotenv.load().get("AWS_SECRET_PRIVATE_KEY")

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkAssessment.com")
    .getOrCreate()

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", AWS_SECRET_PRIVATE_KEY)
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

  val readFileData = new ReadFileData(spark, "sparkassessment/inputFiles")

  val fileDataDF = spark.emptyDataFrame

}