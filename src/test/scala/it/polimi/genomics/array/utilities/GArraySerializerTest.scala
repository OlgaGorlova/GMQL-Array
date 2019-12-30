//package it.polimi.genomics.array.utilities
//
//import java.io.{FileInputStream, FileOutputStream}
//
//import com.esotericsoftware.kryo.Kryo
//import com.esotericsoftware.kryo.io.{Input, Output}
//import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
//import it.polimi.genomics.array.implementation.loaders.Import
//import it.polimi.genomics.core.{GDouble, GRecordKey, GValue}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.scalatest.FunSuite
//
///**
//  * Created by Olga Gorlova on 31/10/2019.
//  */
//class GArraySerializerTest extends FunSuite {
//
//  val conf = new SparkConf()
//    .setAppName("GArraySerializerTest")
//    .setMaster("local[*]")
//    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    .set("spark.kryoserializer.buffer", "128")
//    .set("spark.kryo.registrator", classOf[ArrayKryoRegistrator].getName)
//    .set("spark.driver.allowMultipleContexts", "true")
//    .set("spark.sql.tungsten.enabled", "true")
//    .set("spark.executor.heartbeatInterval", "2000s")
//    .set("spark.network.timeout", "10000000")
//  //      .set("spark.eventLog.enabled", "true")
//
//  var sc: SparkContext = new SparkContext(conf)
//
//  val region = Import.toArray(sc.parallelize(Seq(
//    (new GRecordKey(1l, "chr1", 10, 35, '*'), Array[GValue](GDouble(4), GDouble(1))),
//    (new GRecordKey(3l, "chr1", 10, 35, '*'), Array[GValue](GDouble(4), GDouble(1))),
//    (new GRecordKey(2l, "chr1", 25, 45, '*'), Array[GValue](GDouble(4), GDouble(1))),
//    (new GRecordKey(3l, "chr1", 25, 45, '*'), Array[GValue](GDouble(1), GDouble(1))),
//    (new GRecordKey(1l, "chr1", 30, 40, '*'), Array[GValue](GDouble(3), GDouble(1))),
//    (new GRecordKey(2l, "chr1", 30, 40, '*'), Array[GValue](GDouble(4), GDouble(1))),
//    (new GRecordKey(3l, "chr1", 30, 40, '*'), Array[GValue](GDouble(4), GDouble(1))),
//    (new GRecordKey(1l, "chr1", 50, 65, '*'), Array[GValue](GDouble(2), GDouble(1))),
//    (new GRecordKey(2l, "chr1", 50, 65, '*'), Array[GValue](GDouble(4), GDouble(1))),
//    (new GRecordKey(3l, "chr1", 50, 65, '*'), Array[GValue](GDouble(4), GDouble(1))),
//    (new GRecordKey(1l, "chr1", 55, 75, '*'), Array[GValue](GDouble(4), GDouble(1))),
//    (new GRecordKey(2l, "chr1", 55, 75, '*'), Array[GValue](GDouble(4), GDouble(1))),
//    (new GRecordKey(2l, "chr1", 60, 70, '*'), Array[GValue](GDouble(4), GDouble(1)))
//  ))).first()
//
//
//  val kryo = new Kryo()
//
//  val output = new Output(new FileOutputStream("file.dat"))
//  val input = new Input(new FileInputStream("file.dat"))
//
//  kryo.register(classOf[GARRAY], GArraySerializer)
//
//  kryo.writeObject(output, region)
//
//  output.close()
//
//  val readRegion = kryo.readObject(input, classOf[GARRAY])
//  input.close()
//
//  test("GArraySerializer compare region coordinates"){
//
//    assert(region._1 === readRegion._1)
//  }
//
//  test("GArraySerializer compare values"){
//
//    assert(region._2 === readRegion._2)
//  }
//
//  test("GArraySerializer compare both, coordinates and values"){
//
//    assert(region === readRegion)
//  }
//
//}
