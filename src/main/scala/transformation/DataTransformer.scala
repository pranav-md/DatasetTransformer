package transformation

import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import schemas.Schemas.{FileData, fileDataSchema}

class DataTransformer(var dataFrame: DataFrame)(implicit sparkSession: SparkSession) {

  def transformToDataFrame(fileData: List[FileData] = List.empty): DataTransformer ={
    val rdd = sparkSession.sparkContext.parallelize(fileData)
    val df = sparkSession.createDataFrame(rdd.map(p => Row(p.key, p.value)), fileDataSchema)
    new DataTransformer(df)
  }

  def transformData(): DataFrame ={
    dataFrame
      .groupBy("key", "value")
      .agg(count(col("value")).alias("count"))
      .filter(col("count") % 2 === 1)
      .select("key", "value")
  }


}
