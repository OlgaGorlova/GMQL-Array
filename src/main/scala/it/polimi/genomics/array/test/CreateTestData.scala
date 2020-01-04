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
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "128")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.executor.heartbeatInterval", "2000s")
      //      .set("spark.eventLog.dir ", "D:/spark/spark-logs/")
      //      .set("spark.history.fs.logDirectory", "D:/spark/spark-logs/")
      .set("spark.eventLog.enabled", "true")
      .set("spark.network.timeout", "10000000")


//    val arrayPath = if (args.isEmpty) "/Users/olha/WORK/BENCHMARKS/Filter/small_160MB_3att_5samples" else args(1)
//    val rowPath = if (args.isEmpty) "/Users/olha/WORK/BENCHMARKS/Serialized_array_spark/small_10rep" else args(0)

    val mainPath = if (args.isEmpty) "/Users/olha/WORK/BENCHMARKS/Serialized_array_spark/small_10rep" else args(0)

    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rChr = new Random()
    val rLength = new Random()
    val rStart = new Random()
    val rStop = new Random()
    val rGv1 = new Random()
    val rGv2 = new Random()
    val rSmpl = new Random()

////    500000000
//    val ds: RDD[(GRecordKey, Array[GValue])] = sc.parallelize((100 to 2090 by 10).map{ x =>
////      val start = 1
////      val end   = 23
////      (start + rChr.nextInt((end - start) + 1))
//      val chr = "chr" + (1 + rChr.nextInt(23))
//      val length = 50 + rLength.nextInt(451)
//      val start = /*(x - (1 + rStart.nextInt(1000))).toLong*/ x - length/2
//      val stop = /*(x + (1 + rStop.nextInt(1000))).toLong*/ x + length/2
//
//      new GRecordKey(1l, chr, start, stop, '*')
//    }, 10000)
//      .flatMap{x=>
//        (1 to 10).map { v =>
//          val id = (1 + rSmpl.nextInt(1000))
//          (new GRecordKey(id, x.chrom, x.start, x.stop, x.strand), Array[GValue](GDouble(Math.abs(rGv1.nextDouble())), GDouble(Math.abs(rGv2.nextDouble()))))
//        }
//      }

//    //    500000000
//    val ds_prep: RDD[GRecordKey] = sc.parallelize((100 to 10000090 by 10),10000).flatMap{ x =>
//      //      val start = 1
//      //      val end   = 23
//      //      (start + rChr.nextInt((end - start) + 1))
//
//      (1 to 1000).map { v =>
//        val chr = "chr" + (1 + rChr.nextInt(23))
//        val length = 50 + rLength.nextInt(451)
//        val start = /*(x - (1 + rStart.nextInt(1000))).toLong*/ x - length/2
//        val stop = /*(x + (1 + rStop.nextInt(1000))).toLong*/ x + length/2
////        val id = (1 + rSmpl.nextInt(1000))
//        new GRecordKey(v, chr, start, stop, '*')
//      }
//    }.cache()

//    val count = ds_prep.count()

//    //    rep2
//    val ds2 = sc.parallelize((1 to 500000), 10000).flatMap{ x =>
//      (1 to 1000).map { i =>
//        val chr = "chr" + (1 + rChr.nextInt(23))
//        val length = 50 + rLength.nextInt(451)
//        val start = /*(x - (1 + rStart.nextInt(1000))).toLong*/ x - length / 2
//        val stop = /*(x + (1 + rStop.nextInt(1000))).toLong*/ x + length / 2
//        new GRecordKey(i, chr, if (start < 0) 0 else start, stop, '*')
//      }
//    }.flatMap{x=>
//      (1 to 2).map(v=> (x,Array[GValue](GDouble(Math.abs(rGv1.nextDouble())), GDouble(Math.abs(rGv2.nextDouble())))))
//    }
//
//    //    rep4
//    val ds4 = sc.parallelize((1 to 250000), 10000).flatMap{ x =>
//      (1 to 1000).map { i =>
//        val chr = "chr" + (1 + rChr.nextInt(23))
//        val length = 50 + rLength.nextInt(451)
//        val start = /*(x - (1 + rStart.nextInt(1000))).toLong*/ x - length / 2
//        val stop = /*(x + (1 + rStop.nextInt(1000))).toLong*/ x + length / 2
//        new GRecordKey(i, chr, if (start < 0) 0 else start, stop, '*')
//      }
//    }.flatMap{x=>
//      (1 to 4).map(v=> (x,Array[GValue](GDouble(Math.abs(rGv1.nextDouble())), GDouble(Math.abs(rGv2.nextDouble())))))
//    }
//
//    //    rep8
//    val ds8 = sc.parallelize((1 to 125000),10000).flatMap{ x =>
//      (1 to 1000).map { i =>
//        val chr = "chr" + (1 + rChr.nextInt(23))
//        val length = 50 + rLength.nextInt(451)
//        val start = /*(x - (1 + rStart.nextInt(1000))).toLong*/ x - length / 2
//        val stop = /*(x + (1 + rStop.nextInt(1000))).toLong*/ x + length / 2
//        new GRecordKey(i, chr, if (start < 0) 0 else start, stop, '*')
//      }
//    }.flatMap{x=>
//      (1 to 8).map(v=> (x,Array[GValue](GDouble(Math.abs(rGv1.nextDouble())), GDouble(Math.abs(rGv2.nextDouble())))))
//    }
//
//    //    rep16
//    val ds16 = sc.parallelize((1 to 62500), 10000).flatMap{ x =>
//      (1 to 1000).map { i =>
//        val chr = "chr" + (1 + rChr.nextInt(23))
//        val length = 50 + rLength.nextInt(451)
//        val start = /*(x - (1 + rStart.nextInt(1000))).toLong*/ x - length/2
//        val stop = /*(x + (1 + rStop.nextInt(1000))).toLong*/ x + length/2
//        new GRecordKey(i, chr, if (start < 0) 0 else start, stop, '*')
//      }
//    }.flatMap{x=>
//      (1 to 16).map(v=> (x,Array[GValue](GDouble(Math.abs(rGv1.nextDouble())), GDouble(Math.abs(rGv2.nextDouble())))))
//    }

//    test.map(_._1).distinct().foreach(println(_))
//    println(ds16.map(_._1).distinct().count())

//    val ds2 = sc.parallelize(ds_prep.take((count / 2).toInt),10000).flatMap{x=>
//      (1 to 2).map { v =>
//        (x, Array[GValue](GDouble(Math.abs(rGv1.nextDouble())), GDouble(Math.abs(rGv2.nextDouble()))))
//      }
//    }

//    val ds4 = sc.parallelize(ds_prep.take((count / 4).toInt),10000).flatMap{x=>
//      (1 to 4).map { v =>
//        (x, Array[GValue](GDouble(Math.abs(rGv1.nextDouble())), GDouble(Math.abs(rGv2.nextDouble()))))
//      }
//    }

//    val ds8 = sc.parallelize(ds_prep.take((count / 8).toInt),10000).flatMap{x=>
//      (1 to 8).map { v =>
//        (x, Array[GValue](GDouble(Math.abs(rGv1.nextDouble())), GDouble(Math.abs(rGv2.nextDouble()))))
//      }
//    }

//    val ds16 = sc.parallelize(ds_prep.take((count / 16).toInt),10000).flatMap{x=>
//      (1 to 16).map { v =>
//        (x, Array[GValue](GDouble(Math.abs(rGv1.nextDouble())), GDouble(Math.abs(rGv2.nextDouble()))))
//      }
//    }


    //    refDS
    val ref = sc.parallelize((1 to 500000), 10000).flatMap{ x =>
      //      if (i2 < 1000) i2 +=1 else i2 = 1
      (1 to 20).map { i =>
        val chr = "chr" + (1 + rChr.nextInt(23))
        val length = 50 + rLength.nextInt(451)
        val start = /*(x - (1 + rStart.nextInt(1000))).toLong*/ x - length / 2
        val stop = /*(x + (1 + rStop.nextInt(1000))).toLong*/ x + length / 2
        new GRecordKey(i, chr, if (start < 0) 0 else start, stop, '*')
      }
    }

    var i =0
    val regionDS = sc.parallelize((1 to 100000).map{x=>i+=1;(new GRecordKey(x%20,"Chr"+(x%23 + 1),x,x+200,'*'),Array[GValue](GDouble(i)) )})
//      .flatMap{x=>
//      (1 to 2).map(v=> (x,Array[GValue](GDouble(Math.abs(rGv1.nextDouble())), GDouble(Math.abs(rGv2.nextDouble())))))
//    }

    val formatter = java.text.NumberFormat.getIntegerInstance
    println(formatter.format(regionDS.count()))
    println(regionDS.map(_._1._1).distinct().count())
    println(regionDS.map(_._1._2).distinct().collect().mkString("; "))
//    println("Regions in ds2 row: "+formatter.format(ds2.count()))
//    println("Regions in ds4 row: "+formatter.format(ds4.count()))
//    println("Regions in ds8 row: "+formatter.format(ds8.count()))
//    println("Regions in ds16 row: "+formatter.format(ds16.count()))
//    println("Samples in ds_prep row: "+formatter.format(ds_prep.map(_._1).distinct().count()))
//
//       Store(ds2, mainPath+"rep2_row", sc)
//       Store(ds4, mainPath+"rep4_row", sc)
//       Store(ds8, mainPath+"rep8_row", sc)
//       Store(ds16, mainPath+"rep16_row", sc)

//        val ds2_array = Import.toArray(ds2)
//        val ds4_array = Import.toArray(ds4)
//        val ds8_array = Import.toArray(ds8)
//        val ds16_array = Import.toArray(ds16)
////

//    println("Regions in ds2 array: "+formatter.format(ds2_array.count()))
//    println("Regions in ds4 array: "+formatter.format(ds4_array.count()))
//    println("Regions in ds8 array: "+formatter.format(ds8_array.count()))
//    println("Regions in ds16 array: "+formatter.format(ds16_array.count()))
//
//    Export.writeAvro(ds2_array, mainPath+"rep2_avro", sc)
//        Export.writeAvro(ds4_array, mainPath+"rep4_avro", sc)
//        Export.writeAvro(ds8_array, mainPath+"rep8_avro", sc)
//        Export.writeAvro(ds16_array, mainPath+"rep16_avro", sc)

  }
}
