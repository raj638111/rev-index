package com.cf.revindex

import com.cf.util.Log
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import Transform._

object RevIndexer {

  val log: Logger = Log.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val param = ParamRevIndexer().parse(args)
    val spark = getSparkSession(this.getClass.getName)
    readDocuments(param.sourcePath, spark)
      .addFileName()                           // Assign file name to each row
      .addFileId(param.documentDictionaryPath) // Assign file ID for each file
      .splitLineNexplode()                     // Split the line into words & explode
      .addWordId(param.wordDictionaryPath)     // Assign id for each word
      .assignFileIdsToWordId()                 // Map file IDs to word ID
      .writeResult(param.targetPath)           // Sort & write the result
  }

  /**
   * Read data from input directory
   * @param dir
   * @param spark
   * @return Dataset
   */
  def readDocuments(dir: String, spark: SparkSession): DataFrame = {
    spark.read.textFile(dir).toDF("line")
  }

  /**
   * Create Spark Session
   * @param appName Application name for the spark session
   * @return
   */
  def getSparkSession(appName: String): SparkSession = {
    val ss = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate();
    log.debug("Spark Session Created...")
    ss
  }


}

/**
 * Command line arguments
 * @param sourcePath
 * @param targetPath
 */
case class ParamRevIndexer(
  sourcePath: String = null,
  targetPath: String = null,
  documentDictionaryPath: String = null,
  wordDictionaryPath: String = null){

  val log: Logger = Log.getLogger(this.getClass.getName)

  def parse(args: Array[String]): ParamRevIndexer = {
    val parser =
      new scopt.OptionParser[ParamRevIndexer]("RevIndexer") {
        opt[String]("sourcePath").required().action { (x, c) =>
          c.copy(sourcePath = x)
        }
        opt[String]("targetPath").required().action { (x, c) =>
          c.copy(targetPath = x)
        }
        opt[String]("documentDictionaryPath").optional().action { (x, c) =>
          c.copy(documentDictionaryPath = x)
        }
        opt[String]("wordDictionaryPath").optional().action { (x, c) =>
          c.copy(wordDictionaryPath = x)
        }
      }
    parser.parse(args, ParamRevIndexer()) match {
      case Some(param) => param
      case _ =>
        throw new Exception("Bad arguments")
    }
  }

}
