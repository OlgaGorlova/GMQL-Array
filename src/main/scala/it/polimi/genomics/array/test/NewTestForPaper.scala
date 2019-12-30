package it.polimi.genomics.array.test

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GAttributes, GRegionKey}
import it.polimi.genomics.array.implementation.RegionsOperators.SelectRegions.StoreArrayRD
import it.polimi.genomics.array.implementation.RegionsOperators.UnionRD
import it.polimi.genomics.array.implementation.loaders.{Export, Import}
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.{GValue, ParsingType}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

/**
  * Created by Olga Gorlova on 20/12/2019.
  */
object NewTestForPaper {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Prep DS with 10x rep")
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


    val mainPath = if (args.isEmpty) "/Users/olha/WORK/BENCHMARKS/Filter/small_160MB_3att_5samples" else args(0)
    val outputPath = if (args.isEmpty) "/Users/olha/WORK/BENCHMARKS/Serialized_array_spark/small_10rep" else args(1)

    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")


    implicit def orderGrecord: Ordering[GARRAY] = Ordering.by{s => val e = s._1;(e._1,e._2,e._3,e._4)}
    val rdd = Import.readAvro(outputPath, sc)
//    Export.writeAvro(Import(mainPath, sc), outputPath, sc)

    println(Export.toRow(rdd).count())
//    val union1 = UnionRD(List(0,1,2),rdd,rdd,sc)
////    val union2 = UnionRD(List(0,1,2),union1,union1,sc)
////    val union: RDD[GARRAY] = UnionRD(List(0,1,2),union2,rdd,sc)
//
//    val schema: List[(String, ParsingType.PARSING_TYPE)] = List(
//      ("att1", ParsingType.DOUBLE),
//      ("att2", ParsingType.DOUBLE),
//      ("att3", ParsingType.DOUBLE))
//
//    val save = Export.writeAvro(union1, outputPath, sc)


  }

}
