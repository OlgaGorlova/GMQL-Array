package it.polimi.genomics.array.test

import it.polimi.genomics.array.DataTypes.GRegionKey
import it.polimi.genomics.array.implementation.loaders.{Export, Import}
import it.polimi.genomics.array.utilities.Store
import it.polimi.genomics.core.{GDouble, GRecordKey, GValue}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by Olga Gorlova on 27/12/2019.
  */
object CreateTestData {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Create Test DS")
//      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "128")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.executor.heartbeatInterval", "2000s")
      //      .set("spark.eventLog.dir ", "D:/spark/spark-logs/")
      //      .set("spark.history.fs.logDirectory", "D:/spark/spark-logs/")
      .set("spark.eventLog.enabled", "true")
      .set("spark.network.timeout", "10000000")


    val arrayPath = if (args.isEmpty) "/Users/olha/WORK/BENCHMARKS/Filter/small_160MB_3att_5samples" else args(1)
    val rowPath = if (args.isEmpty) "/Users/olha/WORK/BENCHMARKS/Serialized_array_spark/small_10rep" else args(0)

    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rChr = new Random()
    val rLength = new Random()
    val rStart = new Random()
    val rStop = new Random()
    val rGv1 = new Random()
    val rGv2 = new Random()
    val rSmpl = new Random()

//    500000000
    val ds: RDD[(GRecordKey, Array[GValue])] = sc.parallelize((100 to 2000000000 by 10).map{ x =>
//      val start = 1
//      val end   = 23
//      (start + rChr.nextInt((end - start) + 1))
      val chr = "chr" + (1 + rChr.nextInt(23))
      val length = 50 + rLength.nextInt(451)
      val start = /*(x - (1 + rStart.nextInt(1000))).toLong*/ x - length/2
      val stop = /*(x + (1 + rStop.nextInt(1000))).toLong*/ x + length/2

      new GRecordKey(1l, chr, start, stop, '*')
    }, 10000)
      .flatMap{x=>
        (1 to 10).map { v =>
          val id = (1 + rSmpl.nextInt(1000))
          (new GRecordKey(id, x.chrom, x.start, x.stop, x.strand), Array[GValue](GDouble(Math.abs(rGv1.nextDouble())), GDouble(Math.abs(rGv2.nextDouble()))))
        }
      }

    val formatter = java.text.NumberFormat.getIntegerInstance
    //Array[GValue](GDouble(Math.abs(rGv1.nextDouble())), GDouble(Math.abs(rGv2.nextDouble())))
    println("Regions in row: "+formatter.format(ds.count()))

    Store(ds, rowPath, sc)

    val array = Import.toArray(ds)

    println("Regions in array: "+formatter.format(array.count()))

    Export.writeAvro(array, arrayPath, sc)

  }
}
