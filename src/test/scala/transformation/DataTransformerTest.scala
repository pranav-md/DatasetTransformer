package transformation

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.{BeforeAndAfterAll, EitherValues, GivenWhenThen}
import org.scalatest.matchers.should.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.mockito.MockitoSugar
import schemas.Schemas.FileData

class DataTransformerTest extends AnyWordSpec with BeforeAndAfterAll with EitherValues with GivenWhenThen with Matchers with MockitoSugar {

  import DataTransformerTest._
  override def beforeAll(): Unit = {
    println()
  }

  override def afterAll(): Unit = {


  }

  "Data transformer" should {
    "return DataFrame of given data" in{
      val df = dataTransformer.transformToDataFrame(fileData)
      df.dataFrame.schema.names.toSeq shouldBe Seq("key", "value")
      df.dataFrame.collect() should contain theSameElementsAs fileData.map(d => Row(d.key, d.value))

    }
    "return new dataframe transformed after the required logic" in{
      val df = dataTransformer.transformToDataFrame(fileData).transformData()
      df.schema.names.toSeq shouldBe Seq("key", "value")
      val resultDataFrame = Seq(Row(2, 2), Row(1, 2))
      df.collect() should contain theSameElementsAs resultDataFrame

    }
  }

}

object DataTransformerTest{

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkAssessment.com")
    .getOrCreate()

  var fileData = List(
    FileData(1, 2),
    FileData(1, 2),
    FileData(3, 2),
    FileData(3, 2),
    FileData(4, 2),
    FileData(4, 2),
    FileData(1, 2),
    FileData(2, 2),
  )

  val dataTransformer = new DataTransformer(spark.emptyDataFrame)

  val fileDataDF = spark.emptyDataFrame


}
