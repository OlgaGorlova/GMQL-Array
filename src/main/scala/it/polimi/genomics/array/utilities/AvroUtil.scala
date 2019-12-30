package it.polimi.genomics.array.utilities

import org.apache.avro.Schema
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat, AvroKeyOutputFormat}
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, OutputCommitter, TaskAttemptContext}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by Olga Gorlova on 15/11/2019.
  */
object AvroUtil {

  // just like AvroKeyOutputFormat, but it enables a direct file committer
  class AvroDirectKeyOutputFormat[T] extends AvroKeyOutputFormat[T] {

    private var committer: DirectFileOutputCommitter = null

    override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
      synchronized {
        if (committer == null)
          committer = new DirectFileOutputCommitter(FileOutputFormat.getOutputPath(context), context)

        committer
      }
    }
  }


  trait AvroCompression
  object AvroCompression {
    case class AvroDeflate() extends AvroCompression
    case class AvroSnappy() extends AvroCompression
    case class AvroNone() extends AvroCompression
  }

  // example: AvroUtil.write("file.avfo", MyAvroRecord.getClassSchema, true, rdd)
  def write[T <: SpecificRecord](
                                  path: String,
                                  schema: Schema,
                                  directCommit: Boolean,
                                  compress: AvroCompression,
                                  avroRdd: RDD[T],
                                  job: Job = Job.getInstance()
                                ): Unit = {

    // convert RDD into serializable format
    val intermediateRdd = avroRdd.mapPartitions(
      f = (iter: Iterator[T]) => iter.map(new AvroKey(_) -> NullWritable.get())
      , preservesPartitioning = true
    )

    // configure job parameters
    compress match {
      case AvroCompression.AvroDeflate() =>
        job.getConfiguration.set("avro.output.codec", "deflate")
        job.getConfiguration.set("mapreduce.output.fileoutputformat.compress", "true")
      case AvroCompression.AvroSnappy() =>
        job.getConfiguration.set("avro.output.codec", "snappy")
        job.getConfiguration.set("mapreduce.output.fileoutputformat.compress", "true")
      case AvroCompression.AvroNone() =>
    }

    AvroJob.setOutputKeySchema(job, schema)

    if (directCommit) {
      // save in avro format using a direct committer
      intermediateRdd.saveAsNewAPIHadoopFile(
        path,
        classOf[AvroKey[T]],
        classOf[NullWritable],
        classOf[AvroDirectKeyOutputFormat[T]],
        job.getConfiguration
      )
    } else {
      // save in avro format
      intermediateRdd.saveAsNewAPIHadoopFile(
        path,
        classOf[AvroKey[T]],
        classOf[NullWritable],
        classOf[AvroKeyOutputFormat[T]],
        job.getConfiguration
      )
    }
  }

  // example: AvroUtil.read[MyAvroRecord]("file.avro", sc)
  def read[T: ClassTag](
                         path: String,
                         schema: Schema,
                         sc: SparkContext
                       ): RDD[T] = {

    val conf = sc.hadoopConfiguration
    conf.set("avro.schema.input.key", schema.toString)

    val rdd = sc.newAPIHadoopFile(path,
      classOf[AvroKeyInputFormat[T]],
      classOf[AvroKey[T]],
      classOf[NullWritable],
      conf
    )

    rdd.mapPartitions(
      f = (iter: Iterator[(AvroKey[T], NullWritable)]) => iter.map(_._1.datum())
      , preservesPartitioning = true
    )
  }

}
