package sparkassessment.schemas

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Schemas {

  case class FileData(key: Int, value: Int)

  val fileDataSchema = StructType(Seq(
    StructField("key", IntegerType, true),
    StructField("value", IntegerType, true)
  ))
}
