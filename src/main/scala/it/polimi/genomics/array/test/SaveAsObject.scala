package it.polimi.genomics.array.test

import it.polimi.genomics.array.DataTypes.GArray
import it.polimi.genomics.array.implementation.loaders.Import
import it.polimi.genomics.array.utilities.{ArrayKryoRegistrator, KryoFile}
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
    //      .set("spark.eventLog.enabled", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

//    val kryoPath = args(1) /*"/Users/olha/WORK/BENCHMARKS/Filter/small_kryo/"*/
//    KryoFile.saveAsKryoObjectFile(Import(args(0)/*"/Users/olha/WORK/BENCHMARKS/Filter/small_140MB_1att_5samples/"*/, sc), kryoPath)

    Import(args(0), sc).map(x=> GArray(x._1, x._2)).saveAsObjectFile(args(1))
  }

}
