package com.spotify.scio.values

import java.io.File
import java.nio.channels.Channels

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.util.ScioUtil
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileStream, DataFileWriter}
import org.apache.avro.specific._
import org.apache.avro.util.Utf8
import org.apache.beam.sdk.io._
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.GcsUtil.GcsUtilFactory
import org.apache.beam.sdk.util.{GcsUtil, MimeTypes}
import org.apache.beam.sdk.util.gcsfs.GcsPath

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.Try

object SortMergeBucketFunctions {

  case class AvroPath[K, T](filePaths: String*)

  val gcsUtil: GcsUtil = {
    val defaultOpts = PipelineOptionsFactory.create()
    FileSystems.setDefaultPipelineOptions(defaultOpts)
    new GcsUtilFactory().create(defaultOpts)
  }

  implicit class SpecificRecordImplicit(val self: SpecificRecord) extends Serializable {
    def typedKey[K : ClassTag](keyFieldPos: Int): K = {
      val k = self.get(keyFieldPos).asInstanceOf[K]
      if (k.isInstanceOf[Utf8]) {
        k.toString.asInstanceOf[K]
      } else {
        k
      }
    }
  }

  implicit class SortMergeBucketJoin(val self: ScioContext) extends Serializable {

    // scalastyle:off cyclomatic.complexity
    // scalastyle:off method.length
    def readAndJoin[K: ClassTag : Coder, T1 <: SpecificRecord : ClassTag : Coder,
    T2 <: SpecificRecord : ClassTag : Coder](
      path1: AvroPath[K, T1],
      path2: AvroPath[K, T2]
    )(implicit ordering: Ordering[K]): SCollection[(K, (T1, T2))] = {
      val p1 = partitionFiles(getResources(path1.filePaths))
      val p2 = partitionFiles(getResources(path2.filePaths))
      assert(p1.nonEmpty && p2.nonEmpty, "One or more input paths are empty")

      val matches = (p2.toSeq ++ p1.toSeq)
        .groupBy(_._1)
        .filter(_._2.size == 2) // the bucket must exist in both right and left sides
        .map { case (bucket, files) => (files(0)._2, files(1)._2) }

      self.parallelize(matches).flatMap { case (left, right) =>
        val (l, r) = (avroReader[T1](left), avroReader[T2](right))
        val (keyPosL, keyPosR) = (
          l.getMetaLong("keyPos").toInt, r.getMetaLong("keyPos").toInt
        )
        val output = ListBuffer.empty[(K, (T1, T2))]

        var (lGetNext, rGetNext) = (Try(l.next), Try(r.next))
        while (lGetNext.isSuccess && rGetNext.isSuccess) {
          val (leftBlock, rightBlock) = (lGetNext.get, rGetNext.get)
          val (leftKey, rightKey) = (
            leftBlock.typedKey[K](keyPosL), rightBlock.typedKey[K](keyPosR)
          )

          val keyCompare = ordering.compare(leftKey, rightKey)

          if (keyCompare < 0) {
            lGetNext = Try(l.next())
          } else if (keyCompare > 0) {
            rGetNext = Try(r.next())
          } else {
            output.append((leftKey, (leftBlock, rightBlock)))

            while({
              lGetNext = Try(l.next())
              lGetNext.isSuccess && lGetNext.get.typedKey[K](keyPosL) == rightKey
            }) {
              output.append((rightKey, (lGetNext.get, rightBlock)))
            }

            while({
              rGetNext = Try(r.next())
              rGetNext.isSuccess && rGetNext.get.typedKey[K](keyPosR) == leftKey
            }) {
              output.append((leftKey, (leftBlock, rGetNext.get)))
            }
          }
        }
        l.close()
        r.close()
        output.toList
      }
    }
    // scalastyle:on method.length
    // scalastyle:on cyclomatic.complexity

    private def partitionFiles(files: Seq[ResourceId]): Map[Int, ResourceId] = {
      val bucketRegex = ".*bucket-(\\d+)-of-\\d+.avro".r
      files.toList.groupBy { file =>
        bucketRegex
          .findFirstMatchIn(file.getFilename)
          .map(_.group(1).toInt).getOrElse(-1)
      }.mapValues(_.head)
    }

    private def avroReader[V <: SpecificRecord : ClassTag](resource: ResourceId):
    DataFileStream[V] = {
      val schema = extractSchema[V]
      new DataFileStream[V](
        Channels.newInputStream(FileSystems.open(resource)),
        new SpecificDatumReader(schema)
      )
    }
  }

  implicit class SortMergeBucketWrite[V <: SpecificRecord : ClassTag : Coder]
  (val self: SCollection[V]) extends Serializable {

    // @Todo: K must map to Schema.Type enum
    def writeSMB[K : ClassTag : Coder](
                               path: String,
                               numBuckets: Int,
                               keyFieldName: String = "key"
                             )(implicit ord: Ordering[K]) {
      // @Todo handle potentially nested key field
      val keyField = extractSchema[V].getFields.asScala.find(_.name == keyFieldName)
      assert(keyField.isDefined, s"Key field does not exist in value schema")
      val keyFieldPos = keyField.get.pos()

      val bucketSigFig = s"%0${numBuckets.toString.length}d"
      val pathFormatter = s"$path/bucket-%s-of-$numBuckets.avro"

      self
        .groupBy(record => record.get(keyFieldPos).hashCode() % numBuckets)
        .map { case (bucket: Int, elements: Iterable[V]) =>
          val outputResource = getResource(pathFormatter
            .format(bucketSigFig.format(bucket + 1).toString))
          val writer: DataFileWriter[V] = avroWriter(outputResource, extractSchema[V], keyFieldPos)

          elements.toList.sortBy(_.get(keyFieldPos).asInstanceOf[K]).foreach(writer.append(_))

          writer.close()
        }
    }

    private def avroWriter(resource: ResourceId,
                           schema: Schema,
                           keyFieldPos: Long): DataFileWriter[V] = {
      new DataFileWriter[V](
        new SpecificDatumWriter[V](schema))
        .setCodec(CodecFactory.snappyCodec())
        .setMeta("keyPos", keyFieldPos)
        .create(schema, Channels.newOutputStream(FileSystems.create(resource, MimeTypes.BINARY)))
    }
  }

  private def extractSchema[T <: SpecificRecord : ClassTag]: Schema = {
    ScioUtil.classOf[T]
      .getMethod("getClassSchema").invoke(null)
      .asInstanceOf[Schema]
  }

  private def getResource(path: String): ResourceId = {
    if (path.startsWith("gs://")) {
      FileSystems.matchNewResource(path, false)
    } else {
      LocalResources.fromString(path, false)
    }
  }

  private def getResources(filePaths: Seq[String]): Seq[ResourceId] = {
    val isLocal = !filePaths.exists(_.startsWith("gs://"))
    if (isLocal) {
      filePaths.flatMap { f =>
        val file = new File(f)
        if (file.isDirectory) {
          file.listFiles().map(g => LocalResources.fromFile(g, false))
        } else {
          List(LocalResources.fromString(f, false))
        }
      }
    } else {
      filePaths.flatMap { path =>
        if (path.endsWith("*.avro")) {
          gcsUtil
            .expand(GcsPath.fromUri(path)).asScala
            .map(gcsPath => FileSystems.matchNewResource(gcsPath.toString, false))
        } else {
          List(FileSystems.matchNewResource(path, false))
        }
      }
    }
  }
}
