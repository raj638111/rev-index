package com.cf.revindex

import com.cf.util.Log
import org.apache.log4j.Logger
import org.scalatest._

class RevIndexerTest extends FunSuite with Matchers {

  val log: Logger = Log.getLogger(this.getClass.getName)

  test("Delimiter Extractor"){
    val str =
      """
        |; this - is;a test, . wi.th delimiters )
        |and numbers 10 like this 1-5 in next ! line
        |""".stripMargin
    val res = RevIndexer.getDelimiters(str).toList.sortWith(_ < _)
    res should contain theSameElementsInOrderAs
      List("!", ")", ",", "-", ".", "0", "1", "5", ";")
  }

  test("Word extractor"){
    val str =
      """
        |17— 11th England._ _To Mrs.
        |“I thank you,” 1 mutability! away;
        |(sight tremendous and abhorred!)
        |""".stripMargin
    val res = RevIndexer.extractWords(str)
    res should contain theSameElementsAs List("1",
    "11th", "17", "England", "I", "Mrs", "To", "abhorred", "and", "away",
    "mutability", "sight", "thank", "tremendous", "you")
  }


}

