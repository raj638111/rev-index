package com.cf.revindex

import com.cf.util.Log
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest._
import Transform._


class RevIndexerTest extends FunSuite with Matchers {

  val log: Logger = Log.getLogger(this.getClass.getName)

  test("Read text file"){
    val inputDir = getAbsolutePath("/sample1/")
    val ds = RevIndexer.readDocuments(inputDir, spark())
    // Ensure we are able to read 2 text files from resources directory
    assert(ds.inputFiles.size == 2)
  }

  test("Add file name"){
    val inputDir = getAbsolutePath("/sample1/")
    val fileNames = RevIndexer
      .readDocuments(inputDir, spark())
      .addFileName()
      .select("file_name").distinct.collect
    // Ensure file name is added to dataframe
    assert(fileNames.size == 2)
  }

  test("Assign unique id to all filenames"){
    val inputDir = getAbsolutePath("/sample1/")
    val fileIds = RevIndexer
      .readDocuments(inputDir, spark())
      .addFileName()
      .addFileId()
      .select("file_id")
      .distinct
      .collect().map(_.getAs[Int]("file_id"))
    fileIds should contain theSameElementsAs List(0, 1)
  }

  test("Split line containing words & explode words"){
    val inputDir = getAbsolutePath("/sample1/")
    val words = RevIndexer
      .readDocuments(inputDir, spark())
      .addFileName()
      .addFileId()
      .splitLineNexplode()
      .select("word").distinct
      .collect.map(_.getAs[String]("word"))
    words should contain allOf ("doc", "is", "1", "a")
  }

  test("Add unique ID to each word"){
    val inputDir = getAbsolutePath("/sample1/")
    val idsForWordIs = RevIndexer
      .readDocuments(inputDir, spark())
      .addFileName()
      .addFileId()
      .splitLineNexplode()
      .addWordId()
      .where("word = 'is'")
      .collect.map(_.getAs[Int]("word_id"))
    // Ensure for the word 'is' we have the same id
    assert(idsForWordIs.size == 2)
    assert(idsForWordIs.toSet.size == 1)
  }

  test("Assign file IDs to each Word ID"){
    val inputDir = getAbsolutePath("/sample1/")
    val fileIds = RevIndexer
      .readDocuments(inputDir, spark())
      .addFileName()
      .addFileId()
      .splitLineNexplode()
      .addWordId()
      .assignFileIdsToWordId()
      .where("word_id = 6")
      .collect
      .flatMap(_.getAs[scala.collection.mutable.WrappedArray[String]]("file_ids"))
    // Ensure the fileIds assigned to a word are in the sorted order
    fileIds should contain theSameElementsInOrderAs List("0", "1")
  }

  test("Write result to directory"){
    val inputDir = getAbsolutePath("/sample1/")
    val outputDir: String = getResourcePath() + "output/result"
    val documentMapPath: String = getResourcePath() +
      "output/document_map/document_map.txt"
    val wordMapPath: String = getResourcePath() + "output/word_map/word_map.txt"
    val fileIds = RevIndexer
      .readDocuments(inputDir, spark())
      .addFileName()
      .addFileId(documentMapPath)
      .splitLineNexplode()
      .addWordId(wordMapPath)
      .assignFileIdsToWordId()
      .writeResult(outputDir)
    // Ensure the results are indeed available
    assert(spark.read.textFile(outputDir).collect.size > 3)
  }


  def getAbsolutePath(relPath: String): String = {
    val absolutePath = getClass.getResource(relPath).getPath
    log.info(s"relPath = $relPath, absPath = $absolutePath")
    absolutePath
  }

  def getResourcePath(): String = {
    import java.nio.file.Path
    import java.nio.file.Paths
    getClass.getResource("/").getPath
  }


  def spark(): SparkSession = {
    val ss = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate();
    ss
  }

}

