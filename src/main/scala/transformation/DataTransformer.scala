package sparkassessment.transformation

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import sparkassessment.schemas.Schemas.{FileData, fileDataSchema}

class DataTransformer {
  val logger = Logger.getLogger(classOf[DataTransformer])



  /**
   *  Method to transform dataframe combined from all files according to logic required
   *
   * @param dataFrame data frame of the processed data, to be processed
   * @return
   */
  def transformData(dataFrame: DataFrame): DataFrame ={
    logger.debug("Transforming data!")

    dataFrame
      .groupBy("key", "value")
      .agg(count(col("value")).alias("count"))
      .filter(col("count") % 2 === 1)
      .select("key", "value")
  }
}
