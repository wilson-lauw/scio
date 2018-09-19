package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.avro.{TestRecord, _}
import com.spotify.scio.values.SortMergeBucketFunctions._

import scala.util.Random

class SortMergeBucketExample {}

object SMBWrite {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.parallelize(1 to 100000)
      .map(i =>
        TestRecord.newBuilder()
          .setStringField(s"Left-Record #$i")
          .setIntField(Random.nextInt(5000))
          .build()
      ).writeSMB[Int](args("left"), 5, "int_field")

    sc.parallelize(1 to 10000)
      .map(i =>
        TestRecord.newBuilder()
          .setStringField(s"Right-Record #$i")
          .setIntField(Random.nextInt(5000))
          .build()
      ).writeSMB[Int](args("right"), 5, "int_field")

    sc.close()
  }
}

object SortMergeBucketRead {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.readAndJoin(
      AvroPath[Int, TestRecord](args("left")),
      AvroPath[Int, TestRecord](args("right"))
    ).saveAsTextFile(args("output"))

    sc.close()
  }
}

object ShuffleReadComparison {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val left = sc.avroFile[TestRecord](args("left")).keyBy(_.getIntField.toInt)
    val right = sc.avroFile[TestRecord](args("right")).keyBy(_.getIntField.toInt)

    left.join(right).saveAsTextFile("output")

    sc.close()
  }
}