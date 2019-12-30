package it.polimi.genomics.array.implementation.loaders

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.array.test.AvroTest.rmr
import it.polimi.genomics.array.utilities.AvroUtil
import it.polimi.genomics.array.utilities.AvroUtil.AvroCompression
import it.polimi.genomics.avro.myavro.{gregion, idsList, repRec, sampleRec}
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.{GDouble, GRecordKey, GValue}
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import it.polimi.genomics.avro.myavro.{gregion, idsList, repRec, sampleRec}

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object Export {

  private final val logger = Logger.getLogger(this.getClass);

  def apply(executor: GMQLArrayExecutor,inputDataset:RegionOperator,location:String,sc:SparkContext):RDD[GARRAY]={
    logger.info("----------------Export executing...")
    //    Store(toDecompressed(in), location, sc)
    //    println("Number of regions: " + decodeExtended(in).count())
    //    sc.emptyRDD[GARRAY]

    val input = executor.implement_rd(inputDataset, sc)
//    println("Export: " + input.count())

    input
  }

  def apply(executor: GMQLArrayExecutor,path:String,value: RegionOperator, associatedMeta:MetaOperator, schema : List[(String, PARSING_TYPE)], sc:SparkContext):RDD[GARRAY]={
    logger.info("----------------Export executing...")
    sc.emptyRDD[GARRAY]

  }

  def writeAvro(rdd: RDD[GARRAY], path:String, sc:SparkContext): Unit ={

    import scala.collection.JavaConversions._
    val avroRdd: RDD[gregion] = rdd.map { item =>
      val valuesArray: java.util.List[sampleRec] = item._2._2.map { v =>
        val samplesArray = v.map { s =>
          val list: java.util.List[java.lang.Double] = s.map { r => if (r.isInstanceOf[GDouble]) r.asInstanceOf[GDouble].v else 0 }.map(Double.box).toList
          repRec
            .newBuilder()
            .setRepArray(list)
            .build()
        }.toList
        sampleRec.newBuilder().setSampleArray(samplesArray).build()
      }.toList

      gregion
        .newBuilder()
        .setChr(item._1.chrom)
        .setStart(item._1.start)
        .setStop(item._1.stop)
        .setStrand(item._1.strand.toString)
        .setIdsList(item._2._1.map { id => idsList.newBuilder().setId(id._1).setRep(id._2).build() }.toList)
        .setValuesArray(
          valuesArray
        )
        .build()
    }

    // delete datafile if it already exists (so that subsequent runs succeed)
    rmr(path)

    // save RDD to /tmp/data.avro
    AvroUtil.write(
      path = path,
      schema = gregion.SCHEMA$,
      directCommit = true, // set to true for S3
      compress = AvroCompression.AvroDeflate(), // enable deflate compression
      avroRdd = avroRdd // pass in out dataset
    )

  }

  def toRow(rdd: RDD[GARRAY]): RDD[(GRecordKey, Array[GValue])] = {
    val out: RDD[(GRecordKey, Array[GValue])] = rdd.flatMap { x =>

      val values = x._2._2.transpose
      var k = -1;
      if (values.nonEmpty) {
        x._2._1.zip(values).flatMap { sid =>
          k += 1;
          val values = sid._2.transpose
          values.map { res =>
            (new GRecordKey(sid._1._1, x._1._1, x._1._2, x._1._3, x._1._4), res)
          }
        }
      }
      else
        x._2._1.map { sid => (new GRecordKey(sid._1, x._1._1, x._1._2, x._1._3, x._1._4), new Array[GValue](0))}

    }

    out
  }

}
