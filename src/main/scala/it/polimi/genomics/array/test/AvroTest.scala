package it.polimi.genomics.array.test

import java.io.File
import java.util

import it.polimi.genomics.array.DataTypes._
import it.polimi.genomics.array.implementation.loaders.Import
import it.polimi.genomics.array.utilities.AvroUtil.AvroCompression
import it.polimi.genomics.array.utilities.{ArrayKryoRegistrator, AvroUtil}
import it.polimi.genomics.avro.myavro.{gregion, idsList, repRec, sampleRec}
import it.polimi.genomics.core.{GDouble, GValue}
import org.apache.avro.Schema
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyOutputFormat}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection
import com.sksamuel
import com.sksamuel.avro4s._
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.avro.garray2.{gCoords, idsRec}

/**
  * Created by Olga Gorlova on 15/11/2019.
  */
object AvroTest {

  var sc:SparkContext = _

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("AvroTest")
            .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "128m")
      .set("spark.kryo.registrator", classOf[ArrayKryoRegistrator].getName)
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.executor.heartbeatInterval", "2000s")
      .set("spark.network.timeout", "10000000")
    //      .set("spark.eventLog.enabled", "true")

    sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
//    type REGION = (String,Long,Long,String,Array[(Long,Int)], Array[Array[Array[Double]]])
    import scala.collection.JavaConverters._
    val rdd = Import(args(0), sc).map{x=> GArray2(new GRegionKey2(x._1._1, x._1._2, x._1._3, x._1._4.toString), x._2)}

//    val schema = ScalaReflection.schemaFor[GValue].dataType.asInstanceOf[StructType]
//    schema.printTreeString

    val schema = AvroSchema[GAttributes]

    println(schema.toString(true))

//    rmr(args(1))
    // Saving version 2:
//    val file = new File(args(1))
//    val os = AvroOutputStream.data[GArray2].to(file).build(schema)
//    os.write(rdd.collect())
//    os.close()
//
//
//    val is = AvroInputStream.data[GArray2].from(file).build(schema)
//    val pizzas = is.iterator.toSet
//    is.close()
//
//    println(pizzas.mkString("\n"))

    // delete datafile if it already exists (so that subsequent runs succeed)
//        rmr(args(1))
//
//        val avroRdd = rdd.map { item =>
//
//            val valuesArray = item._2._2.map { v =>
//              v.map { s => s.map(t=> t.asInstanceOf[AnyRef]).toList.asJava }.toList.asJava
//            }.toList.asJava
//
//          it.polimi.genomics.avro.garray2.GArray
//              .newBuilder()
//            .setGRecord(gCoords.newBuilder()
//              .setChr(item._1.chrom)
//              .setStart(item._1.start)
//              .setStop(item._1.stop)
//              .setStrand(item._1.strand.toString)
//              .build()
//            )
//            .setGAttributesRec(it.polimi.genomics.avro.garray2.GAttributes.newBuilder()
//                .setSamples(item._2._1.map { id => idsRec.newBuilder().set1$1(id._1).set2$1(id._2).build() }.toList)
//                .setAtt(valuesArray)
//              .build()
//            )
//              .build()
//          }

    // save RDD to /tmp/data.avro
//        AvroUtil.write(
//          path = args(1),
//          schema = it.polimi.genomics.avro.garray2.GArray.SCHEMA$,
//          directCommit = true, // set to true for S3
//          compress = AvroCompression.AvroDeflate(), // enable deflate compression
//          avroRdd = avroRdd // pass in out dataset
//        )

        // read Avro file into RDD
//        val rddFromFile: RDD[it.polimi.genomics.avro.garray2.GArray] = AvroUtil.read[it.polimi.genomics.avro.garray2.GArray](
//          path = args(1),
//          schema = it.polimi.genomics.avro.garray2.GArray.SCHEMA$,
//          sc = sc
//        ).map(it.polimi.genomics.avro.garray2.GArray.newBuilder(_).build()) // clone after reading from file to prevent buffer reference issues
//
//    // display number of records
//      println(s"Number of records in RDD: ${rddFromFile.count()}")


//    val avroRdd = rdd.map { item =>
//
//        val valuesArray: java.util.List[sampleRec] = item._2._2.map { v =>
//          val rrr = v.map { s =>
//            val list: java.util.List[java.lang.Double] = s.map { r => r.asInstanceOf[GDouble].v }.map(Double.box).toList.asJava
//            repRec
//              .newBuilder()
//              .setRepArray(list)
//              .build()
//          }.toList.asJava
//          sampleRec.newBuilder().setSampleArray(rrr).build()
//        }.toList.asJava
//
//        gregion
//          .newBuilder()
//          .setChr(item._1.chrom)
//          .setStart(item._1.start)
//          .setStop(item._1.stop)
//          .setStrand(item._1.strand.toString)
//          .setIdsList(item._2._1.map { id => idsList.newBuilder().setId(id._1).setRep(id._2).build() }.toList)
//          .setValuesArray(
//            valuesArray
//          )
//          .build()
//      }

//    val job = new Job(sc.hadoopConfiguration)
//    AvroJob.setOutputKeySchema(job, PageViewEvent.SCHEMA$)
//
//
//    val output = s"/avro/${date.toString(dayFormat)}"
//    rmr(output)
//    rdd.coalesce(64).map(x => (new AvroKey(x._1), x._2))
//      .saveAsNewAPIHadoopFile(
//        output,
//        classOf[PageViewEvent],
//        classOf[org.apache.hadoop.io.NullWritable],
//        classOf[AvroKeyOutputFormat[PageViewEvent]],
//        job.getConfiguration)

//    val schemaAvro = new Schema.Parser().parse(new File("/Users/olha/WORK/BENCHMARKS/Filter/avro_schema.json"))


//    val job = Job.getInstance
//    val schema = Schema.create(Schema.Type.STRING)
//    AvroJob.setOutputKeySchema(job, schemaAvro)

//    val ds = rdd.map(item => (new AvroKey((item._1.chrom, item._1.start, item._1.stop,item._1.strand.toString, item._2._1, item._2._2.map(_.map(_.map(_.asInstanceOf[GDouble].v))))),
//      NullWritable.get()))

//    ds.collect().foreach(println(_))
//    ds.saveAsNewAPIHadoopFile(
//      args(1),
//      classOf[AvroKey[REGION]],
//      classOf[NullWritable],
//      classOf[AvroKeyOutputFormat[REGION]],
//      job.getConfiguration)

    // delete datafile if it already exists (so that subsequent runs succeed)
//    rmr(args(1))

    // save RDD to /tmp/data.avro
//    AvroUtil.write(
//      path = args(1),
//      schema = schema,
//      directCommit = true, // set to true for S3
//      compress = AvroCompression.AvroDeflate(), // enable deflate compression
//      avroRdd = rdd // pass in out dataset
//    )

//    // read Avro file into RDD
//    val rddFromFile: RDD[gregion] = AvroUtil.read[gregion](
//      path = args(1),
//      schema = gregion.getClassSchema,
//      sc = sc
//    ).map(gregion.newBuilder(_).build()) // clone after reading from file to prevent buffer reference issues

    // display number of records
//    println(s"Number of records in RDD: ${rddFromFile.count()}")
//
//    val arrayRddFromFile = rddFromFile.map{r=>
//      val gRegionKey: GRegionKey = new GRegionKey(r.getChr.toString, r.getStart, r.getStop, r.getStrand.charAt(0))
//      val ids: Array[(Long, Int)] = r.getIdsList.map(id => (id.getId,id.getRep)).toArray
//      val values: Array[Array[Array[GValue]]] = r.getValuesArray.map{ v=>
//        v.getSampleArray.map{s=> val sss:Array[GValue] = s.getRepArray.map(r=> GDouble(r)).toArray; sss}.toArray
//      }.toArray
//      val gAttributes = new GAttributes(ids, values)
//
//      (gRegionKey, gAttributes)
//    }

    // display number of records
//    println(s"Number of records in original RDD: ${rdd.count()}")

    // display number of records
//    println(s"Number of records in array RDD: ${arrayRddFromFile.count()}")

  }

  // method that deletes a file if it exists
  def rmr(path: String): Unit = {
    try {
      val fs = FileSystem.newInstance(sc.hadoopConfiguration)
      fs.delete(new Path(path), true)
      fs.close()
    } catch {
      case e: Throwable =>
    }
  }

}
