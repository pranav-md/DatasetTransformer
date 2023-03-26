package transformation

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, EitherValues, GivenWhenThen}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.mockito.MockitoSugar
import sparkassessment.transformation.DataTransformer

class DataTransformerTest extends AnyWordSpec with BeforeAndAfterAll with EitherValues with GivenWhenThen with Matchers with MockitoSugar {

  import DataTransformerTest._

  "Data transformer" should {
    "return new dataframe transformed after the required logic" in {
      val df = dataTransformer.transformData(sampleDataFrame)
      df.schema.names.toSeq shouldBe Seq("key", "value")
      val resultDataFrame = Seq(Row(2, 3), Row(1, 2), Row(4, 3), Row(4, 2))
      df.collect() should contain theSameElementsAs resultDataFrame

    }
  }

}

object DataTransformerTest {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkAssessment.com")
    .getOrCreate()


  val schema = StructType(
    Seq(
      StructField("key", IntegerType, nullable = false),
      StructField("value", IntegerType, nullable = false)
    )
  )

  val data = Seq(
    Row(1, 2),
    Row(1, 2),
    Row(1, 2),
    Row(1, 3),
    Row(1, 3),
    Row(2, 2),
    Row(2, 2),
    Row(2, 3),
    Row(4, 5),
    Row(4, 5),
    Row(4, 5),
    Row(4, 5),
    Row(4, 2),
    Row(4, 3)
  )

  val sampleDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

  val dataTransformer = new DataTransformer()

}
