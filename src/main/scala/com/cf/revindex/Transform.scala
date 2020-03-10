package com.cf.revindex

import java.io.ByteArrayInputStream

import com.cf.revindex.RevIndexer.log
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Transform {

  implicit class TransformUtil(df: DataFrame) {

    val spark = df.sparkSession
    import spark.implicits._

    /**
     * Assign distinct ID for each file
     * @param documentMapPath Path to write Document ID dictionary
     */
    def addFileId(documentMapPath: String = null): DataFrame = {
      val fileIds = df.select("file_name").distinct.collect
        .map( _.getAs[String]("file_name"))
        .sortWith(_ < _).zipWithIndex.toMap match {
        case map =>
          saveDictionary(map, documentMapPath)
          spark.sparkContext.broadcast(map)
      }
      val addFileId = udf((fileName: String) => {
        fileIds.value.get(fileName) match {
          case Some(id) => id
          case None => throw new NoSuchElementException(s"fileName not present: $fileName")
        }
      })
      df.withColumn("file_id", addFileId($"file_name"))
    }

    /**
     * Split line into words & explode the words into multiple records
     * @return
     */
    def splitLineNexplode(): DataFrame = {
      val splitLine = udf((line: String) => {
        val matched = "[a-zA-Z0-9]+".r.findAllMatchIn(line)
        matched.map(_.toString).toSet.toArray
      })
      df.withColumn("words", splitLine($"line"))
        .withColumn("word", explode($"words"))
    }

    /**
     * Add distinct ID for each word
     * @param wordMapPath The path to write the Word Dictionary
     */
    def addWordId(wordMapPath: String = null): DataFrame = {
      val spark = df.sparkSession
      val wordIds = df.select("word").distinct().collect()
        .map(_.getAs[String]("word"))
        .sortWith(_ < _)
        .zipWithIndex.toMap match {
        case map =>
          saveDictionary(map, wordMapPath)
          spark.sparkContext.broadcast(map)
      }
      val addWordId = udf((word: String) => {
        wordIds.value.get(word) match {
          case Some(id) => id
          case None => throw new NoSuchElementException(s"id not present for word: $word")
        }
      })
      df.withColumn("word_id", addWordId($"word"))
    }

    /**
     * Add source file name for each record
     */
    def addFileName(): DataFrame = {
      df.withColumn("file_name", input_file_name())
    }

    /**
     * Map File IDs to word ID
     */
    def assignFileIdsToWordId(): DataFrame = {
      val sortFileIds =
        udf((file_ids: scala.collection.mutable.WrappedArray[String]) => {
        file_ids.sortWith(_ < _)
      })
      val concatenate =
        udf((wordId: String, fieldIds: scala.collection.mutable.WrappedArray[String]) => {
        s"$wordId: [${fieldIds.mkString(",")}]"
      })
      df.select($"file_name", $"file_id", $"word_id")
        .groupBy($"word_id")
        .agg(collect_set($"file_id").as("file_ids"))
        .withColumn("file_ids", sortFileIds($"file_ids"))
        .withColumn("result", concatenate($"word_id", $"file_ids"))
    }

    /**
     * Write the result
     */
    def writeResult(outputDir: String): Unit = {
      df.select("result")
        .sort("result")
        .repartition(1)
        .write
        .format("text")
        .save(outputDir)
    }

    /**
     * Converts the input map into string & save the result in
     * given location
     * @param map
     * @param location
     */
    def saveDictionary(map: Map[String, Int], location: String): Unit = {
      location match {
        case null =>
          log.warn("Path not provided to write Document Dictionary")
        case _ =>
          val str = map.toList.sortWith(_._2 < _._2).map{ case (key, value) =>
            s"$key,$value"
          }.mkString("\n")
          val conf = spark.sparkContext.hadoopConfiguration
          val path = new Path(location)
          val fs = path.getFileSystem(conf)
          val os = fs.create(path)
          val is = new ByteArrayInputStream(str.getBytes)
          IOUtils.copyBytes(is, os, conf)
      }
    }

  }


}
