package com.cf.revindex

import com.cf.util.Log
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.util.matching.Regex

object RevIndexer {

  val log: Logger = Log.getLogger(this.getClass.getName)

  def getDelimiters(df: DataFrame): Unit = {
    val spark = df.sparkSession
  }

  def getDelimiters(str: String): Set[String] = {
    val matched = raw"[^\sa-zA-Z]".r.findAllMatchIn(str)
    matched.map(_.toString).toSet
  }

  def extractWords(str: String): Set[String] = {
    val matched = "[a-zA-Z0-9]+".r.findAllMatchIn(str)
    matched.map(_.toString).toSet
  }

}
