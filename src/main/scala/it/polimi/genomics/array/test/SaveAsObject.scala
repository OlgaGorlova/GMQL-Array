package it.polimi.genomics.array.test

import it.polimi.genomics.array.DataTypes.GArray
import it.polimi.genomics.array.implementation.loaders.{Export, Import}
import it.polimi.genomics.array.utilities.{ArrayKryoRegistrator, KryoFile}
import it.polimi.genomics.core.{GDouble, GRecordKey, GValue}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Olga Gorlova on 11/11/2019.
  */
object SaveAsObject {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SaveAsObject")
//      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "128")
      .set("spark.kryo.registrator", classOf[ArrayKryoRegistrator].getName)
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.executor.heartbeatInterval", "2000s")
      .set("spark.network.timeout", "10000000")
          .set("spark.eventLog.enabled", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

//    val kryoPath = args(1) /*"/Users/olha/WORK/BENCHMARKS/Filter/small_kryo/"*/
//    KryoFile.saveAsKryoObjectFile(Import(args(0)/*"/Users/olha/WORK/BENCHMARKS/Filter/small_140MB_1att_5samples/"*/, sc), kryoPath)

//    Import(args(0), sc).saveAsObjectFile(args(1))

//    val rdd = Import.toArray(sc.parallelize(Seq(
//      (new GRecordKey(1l, "chr1", 10, 20, '*'), Array[GValue](GDouble(4), GDouble(1))),
//      (new GRecordKey(3l, "chr1", 10, 20, '*'), Array[GValue](GDouble(4), GDouble(1))),
//      (new GRecordKey(2l, "chr1", 20, 30, '*'), Array[GValue](GDouble(4), GDouble(1))),
//      (new GRecordKey(3l, "chr1", 30, 40, '*'), Array[GValue](GDouble(1), GDouble(1))),
//      (new GRecordKey(1l, "chr1", 30, 40, '*'), Array[GValue](GDouble(3), GDouble(1))),
//      (new GRecordKey(2l, "chr1", 50, 60, '*'), Array[GValue](GDouble(4), GDouble(1)))
//    )))

//    Export.writeAvro(rdd,"/Users/olha/WORK/BENCHMARKS/Filter/small_test_avro/", sc)

    Export.writeAvro(Import(args(0), sc), args(1), sc)
  }

}
