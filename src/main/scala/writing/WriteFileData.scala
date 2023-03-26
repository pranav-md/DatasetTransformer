package sparkassessment.writing

import java.io.File

import com.amazonaws.services.s3.transfer.TransferManager
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}

case class WriteFileData(outputPath: String, transferManager: TransferManager, logger: Logger) {

  val s3WritePath = s"s3a://${outputPath}"


  val bucket = outputPath.takeWhile(_ != '/')

  val path = outputPath.substring(outputPath.indexOf("/") + 1)


  /**
   *  Method to upload dataframe procesed as TSV file in S3 bucket
   *
   * @param dataFrame data frame of the processed data, to be saved
   * @return
   */
  def writeProcessedFile(dataFrame: DataFrame): Unit = {

    logger.debug(s"Writing file data into S3 bucket $outputPath")

    val filePath = saveDataFrameToTempTsv(dataFrame)
    val file = new File(filePath)

    try {
      val transfer = transferManager.uploadDirectory(bucket, path, file, false)
      transfer.waitForCompletion()
      file.deleteOnExit()
      logger.debug(s"File has successfully uploaded in Amazon S3 in path ${outputPath}")
      println(s"File has successfully uploaded in Amazon S3 in path ${outputPath}")

    } catch {
      case ex: Exception =>
        logger.error(s"Got exception while uploading ${file.getName} on Amazon S3")
        println(s"Got exception while uploading ${file.getName} on Amazon S3")

    }
  }

  private def saveDataFrameToTempTsv(df: DataFrame): String = {
    val tempDir = System.getProperty("java.io.tmpdir")
    val tempFilePath = s"$tempDir/output.tsv"
    df.write.mode(SaveMode.Overwrite).option("delimiter", "\t").option("header", "true").csv(tempFilePath)
    tempFilePath
  }
}
