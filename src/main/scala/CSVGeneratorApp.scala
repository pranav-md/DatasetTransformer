import org.apache.spark.sql.SparkSession

object CSVGeneratorApp extends App {


  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkAssessment.com")
    .getOrCreate()

}
