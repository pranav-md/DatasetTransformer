package sparkassessment.reading

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import sparkassessment.schemas.Schemas.fileDataSchema

class ReadFileData(spark: SparkSession, inputFilePath: String) {

  val s3InputFilePath = s"s3a://${inputFilePath}"
  val logger = Logger.getLogger(classOf[ReadFileData])




  /**
   *  Method to read CSV and TSV files in S3 bucket under input path
   * @return
   */
  def getFileData(): DataFrame = {

    val csvS3Files = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .schema(fileDataSchema)
      .load(s"${s3InputFilePath}/*.csv")

    val tsvS3Files = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(fileDataSchema)
      .load(s"${s3InputFilePath}/*.tsv")

    logger.debug("Files read successfully!")
    csvS3Files.union(tsvS3Files).filter(col("key") =!= 0 && col("value") =!= 0)
  }


}
