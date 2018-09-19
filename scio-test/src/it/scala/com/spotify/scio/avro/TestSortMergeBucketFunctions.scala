package com.spotify.scio.avro

import java.io.File

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.ArtisanJoin
import com.spotify.scio.values.SortMergeBucketFunctions._
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfter

class TestSortMergeBucketFunctions extends PipelineSpec with BeforeAndAfter {
  private val record1 = TestRecord.newBuilder().setStringField("K1").setIntField(1).build()
  private val record2 = TestRecord.newBuilder().setStringField("K2").setIntField(2).build()
  private val record3 = TestRecord.newBuilder().setStringField("K2").setIntField(3).build()
  private val record4 = TestRecord.newBuilder().setStringField("K3").setIntField(4).build()

  private val record5 = TestRecord.newBuilder().setStringField("K1").setIntField(5).build()
  private val record6 = TestRecord.newBuilder().setStringField("K1").setIntField(4).build()
  private val record7 = TestRecord.newBuilder().setStringField("K2").setIntField(3).build()

  after {
    FileUtils.deleteDirectory(new File("tmpPath1"))
    FileUtils.deleteDirectory(new File("tmpPath2"))
  }

  "SortMergeBucketFunctions" should "work with String key" in {
    runWithContext { sc =>
      val left = sc.parallelize(Seq(
        record4, record1, record3, record2
      ))

      val right = sc.parallelize(Seq(
        record5, record7, record6
      ))

      left.writeSMB[String]("tmpPath1", numBuckets = 3, "string_field")
      right.writeSMB[String]("tmpPath2", numBuckets = 3, "string_field")
    }

    new File("tmpPath1").listFiles().map(_.getName) shouldEqual
      List("bucket-1-of-3.avro", "bucket-2-of-3.avro", "bucket-3-of-3.avro")

    new File("tmpPath2").listFiles().map(_.getName) shouldEqual
      List("bucket-2-of-3.avro", "bucket-3-of-3.avro")

    val expectedOut = List(
      ("K1", (record5, record1)),
      ("K1", (record6, record1)),
      ("K2", (record7, record2)),
      ("K2", (record7, record3))
    )

    runWithContext { sc =>
      sc.readAndJoin(
        AvroPath[String, TestRecord]("tmpPath1"),
        AvroPath[String, TestRecord]("tmpPath2")
      ) should containInAnyOrder(expectedOut)
    }
  }

  it should "work with Int key" in {
    runWithContext { sc =>
      val left = sc.parallelize(Seq(
        record1, record2, record3, record4
      ))

      val right = sc.parallelize(Seq(
        record5, record6, record7
      ))

      left.writeSMB[Int]("tmpPath1", numBuckets = 3, "int_field")
      right.writeSMB[Int]("tmpPath2", numBuckets = 3, "int_field")
    }

    val expectedOut = List(
      (4, (record6, record4)),
      (3, (record7, record3))
    )

    runWithContext { sc =>
      sc.readAndJoin(
        AvroPath[Int, TestRecord]("tmpPath1"),
        AvroPath[Int, TestRecord]("tmpPath2")
      ) should containInAnyOrder(expectedOut)
    }
  }

  it should "have parity with standard inner join implementation" in {
    val left = (1 to 100)
      .map(i => TestRecord.newBuilder()
        .setStringField(s"Left Record $i")
        .setIntField(i)
        .build())

    val right = (1 to 100)
      .map(i => TestRecord.newBuilder()
        .setStringField(s"Right Record $i")
        .setIntField(i)
        .build())

    runWithContext { sc =>
      sc.parallelize(left).writeSMB[Int]("tmpPath1", numBuckets = 1, "int_field")
      sc.parallelize(right).writeSMB[Int]("tmpPath2", numBuckets = 1, "int_field")
    }

    runWithContext { sc =>
      val smbJoin = sc.readAndJoin(
        AvroPath[Int, TestRecord]("tmpPath1"),
        AvroPath[Int, TestRecord]("tmpPath2")
      )

      val regularJoin = ArtisanJoin("regJoin",
        sc.parallelize(left).keyBy(_.getIntField.toInt),
        sc.parallelize(right).keyBy(_.getIntField.toInt)
      )

      regularJoin.fullOuterJoin(smbJoin)
        .filter { joined => joined._2._1.isEmpty || joined._2._2.isEmpty } should beEmpty
    }
  }
}
