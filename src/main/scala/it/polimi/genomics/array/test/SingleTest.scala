package it.polimi.genomics.array.test

import it.polimi.genomics.GMQLServer.{DefaultRegionExtensionFactory, DefaultRegionsToMetaFactory, DefaultRegionsToRegionFactory}
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.GArray
import it.polimi.genomics.array.implementation.RegionsOperators._
import it.polimi.genomics.array.implementation.loaders.Import
import it.polimi.genomics.array.utilities.ArrayKryoRegistrator
import it.polimi.genomics.avro.myavro.gregion
import it.polimi.genomics.core.DataStructures.CoverParameters.{ANY, CoverFlag, N}
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder
import it.polimi.genomics.core.DataStructures.RegionAggregate.RESTART
import it.polimi.genomics.core.DataStructures.RegionCondition.ChrCondition
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Olga Gorlova on 11/11/2019.
  */
object SingleTest {

  var sc: SparkContext = _

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Single Op test")
//      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "128")
      .set("spark.kryo.registrator", classOf[ArrayKryoRegistrator].getName)
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.executor.heartbeatInterval", "2000s")
      .set("spark.network.timeout", "10000000")
    //      .set("spark.local.dir", "/tmp")
          .set("spark.eventLog.enabled", "true")

    sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

//    sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAJNZFCQRQNYHI7JZA")
//    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")

    //    val mainPath = if (args.isEmpty) "/Users/olha/WORK/BENCHMARKS/STQL/Serialized_array_spark/medium_900MB_5att_5samples/" else args(1)
    //    val op = if (args.isEmpty) "union" else args(0)
    //    val alg = if (args.isEmpty) "array" else args(2)

    if (args.nonEmpty) {
      args(0).toLowerCase() match {
        case "select" => select(args(1)) //path, alg
        case "project" => project(args(1)) //path, alg
        case "merge" => merge(args(1)) //path, alg
        case "group" => group(args(1)) //path, alg
        case "extend" => extend(args(1)) //path, alg
        case "cover" => cover(args(1), args(2), args(3).toLong) //path, alg, flag, bin
        case "join" => join(args(1), args(2), args(3), args(4).toLong) //ref, exp, alg, builder, bin
        case "map" => map(args(1), args(2), args(3).toLong) //ref, exp, alg, bin
        case "difference" => difference(args(1), args(2), args(3).toLong) //ref, exp, alg, bin
        case "union" => union(args(1), args(2)) //ref, exp, alg
        case "cover1" => cover1(args(1), args(2), args(3).toLong) //path, alg, flag, bin

        case _ => println("Please choose a query!")
      }
    }
    else println("Please provide an argument")

  }

  def select(path: String): Unit = {
    sc.getConf.setAppName("SELECT")
    //    val fun = Predicate(0, REG_OP.GT, 2)
    val fun = ChrCondition("chr13")
    val startTime = System.currentTimeMillis()
//    val ref = Import(path, true, sc)
    val ref = Import.readAsAvro(path, sc)
//    println("Load: " + ref.count() + " reg. " + (System.currentTimeMillis() - startTime) / 1000 + " sec.")
    val res = SelectRD.applyAvro(Some(fun), ref, sc)
    println("SELECT: " + res.count())
    println("Execution time for SELECT array-based: " + (System.currentTimeMillis() - startTime) / 1000)

  }

  def project(path: String): Unit = {
    sc.getConf.setAppName("PROJECT")

    val fun = DefaultRegionExtensionFactory.get(RESTART(), Left("old_start"))

    val startTime = System.currentTimeMillis()
//    val ref = Import(path, true, sc)
    val ref: RDD[gregion] = Import.readAsAvro(path, sc)
//    println("Load: " + ref.count() + " reg. " + (System.currentTimeMillis() - startTime) / 1000 + " sec.")
    val res = ProjectRD.applyAvro(None, Some(List(fun)), ref, sc)
    println("PROJECT: " + res.count())
    println("Execution time for PROJECT array-based: " + (System.currentTimeMillis() - startTime) / 1000)
  }

  def merge(path: String): Unit = {
    sc.getConf.setAppName("MERGE")

    val startTime = System.currentTimeMillis()
//    val ref = Import(path, true, sc)
    val ref = Import.readAvro(path, sc)
//    println("Load: " + ref.count() + " reg. " + (System.currentTimeMillis() - startTime) / 1000 + " sec.")
    val res = MergeRD(ref, sc)
    println("MERGE: " + res.count())
    println("Execution time for MERGE array-based: " + (System.currentTimeMillis() - startTime) / 1000)
  }

  def group(path: String): Unit = {
    sc.getConf.setAppName("GROUP")

    val fun = DefaultRegionsToRegionFactory.get("COUNT", Some("count"))

    val startTime = System.currentTimeMillis()
//    val ref = Import(path, true, sc)
    val ref = Import.readAsAvro(path, sc)
//    println("Load: " + ref.count() + " reg. " + (System.currentTimeMillis() - startTime) / 1000 + " sec.")
    val res = GroupRD.applyAvro(None, Some(List(fun)), ref, sc)
    println("GROUP: " + res.count())
    println("Execution time for GROUP array-based: " + (System.currentTimeMillis() - startTime) / 1000)
  }

  def extend(path: String): Unit = {
    sc.getConf.setAppName("EXTEND")

    val fun = DefaultRegionsToMetaFactory.get("COUNT", Some("region_count"))
    val startTime = System.currentTimeMillis()
//    val ref = Import(path, true, sc)
    val ref = Import.readAvro(path, sc)
//    println("Load: " + ref.count() + " reg. " + (System.currentTimeMillis() - startTime) / 1000 + " sec.")
    val res = ExtendRD(ref, List(fun), sc)
    println("EXTEND: " + res.count())
    println("Execution time for EXTEND array-based: " + (System.currentTimeMillis() - startTime) / 1000)
  }

  def cover(path: String, flag: String, bin: Long): Unit = {
    sc.getConf.setAppName("COVER")

    val coverFlag = flag.toLowerCase() match {
      case "cover" => CoverFlag.COVER
      case "flat" => CoverFlag.FLAT
      case "summit" => CoverFlag.SUMMIT
      case "histogram" => CoverFlag.HISTOGRAM
    }
    implicit def orderGrecord: Ordering[GARRAY] = Ordering.by{s => val e = s._1;(e._1,e._2,e._3,e._4)}
    val fun = DefaultRegionsToRegionFactory.get("COUNT", Some("count"))
    val startTime = System.currentTimeMillis()
//    val ref = Import(path, true, sc)
    import com.softwaremill.quicklens._
    val ref = Import.readAvro(path, sc)//.map(x=> (x._1.modify(_.stop).using(_ + 5), x._2))
//    println("Load: " + ref.count() + " reg. " + (System.currentTimeMillis() - startTime) / 1000 + " sec.")
    val res = GenometricCover(coverFlag, new N {
      override val n = 1
    }, new ANY {},  ref, bin, List(fun), sc)
    println(s"${coverFlag.toString}: " + res.count())
    println("Execution time for COVER array-based: " + (System.currentTimeMillis() - startTime) / 1000)
  }

  def cover1(path: String, flag: String, bin: Long): Unit = {
    sc.getConf.setAppName("COVER")

    val coverFlag = flag.toLowerCase() match {
      case "cover" => CoverFlag.COVER
      case "flat" => CoverFlag.FLAT
      case "summit" => CoverFlag.SUMMIT
      case "histogram" => CoverFlag.HISTOGRAM
    }
    implicit def orderGrecord: Ordering[GARRAY] = Ordering.by{s => val e = s._1;(e._1,e._2,e._3,e._4)}
    val fun = DefaultRegionsToRegionFactory.get("COUNT", Some("count"))
    val startTime = System.currentTimeMillis()
    //    val ref = Import(path, true, sc)
    import com.softwaremill.quicklens._
    val ref = Import.readAvro(path, sc)//.map(x=> (x._1.modify(_.stop).using(_ + 5), x._2))
    val res = GenometricCover_v2(coverFlag, new N {
      override val n = 1
    }, new ANY {}, List(fun),  ref, bin, sc)
    println(s"${coverFlag.toString}: " + res.count())
    println("Execution time for COVER array-based: " + (System.currentTimeMillis() - startTime) / 1000)
  }

  def join(pathRef: String, pathExp: String, builder: String, bin: Long): Unit = {
    sc.getConf.setAppName("JOIN")

    val regionBuilder = builder.toLowerCase() match {
      case "left" => RegionBuilder.LEFT
      case "right" => RegionBuilder.RIGHT
      case "int" => RegionBuilder.INTERSECTION
      case "contig" => RegionBuilder.CONTIG
    }
    val startTime = System.currentTimeMillis()
    val ref = Import.readAvro(pathRef, sc)
    var i = 0;
    val exp = Import.readAvro(pathExp, sc).map{x=> if (i == 4) i = 0 else i +=1; (x, i)}.filter(_._2 == 4).map(_._1)
//    println("Load ref: " + ref.count() + " reg. " + (System.currentTimeMillis() - startTime) / 1000 + " sec.")
//    println("Load exp: " + exp.count() + " reg. " + (System.currentTimeMillis() - startTime) / 1000 + " sec.")
    val res = GenometricJoin(ref, exp, regionBuilder, None, bin, sc)
    println(s"JOIN ${regionBuilder.toString}: " + res.count())
    println("Execution time for JOIN array-based: " + (System.currentTimeMillis() - startTime) / 1000)

  }

  def map(pathRef: String, pathExp: String, bin: Long): Unit = {
    sc.getConf.setAppName("MAP")

    val startTime = System.currentTimeMillis()
    val ref = Import.readAvro(pathRef, sc)
    var i = 0;
    val exp = Import.readAvro(pathExp, sc).map{x=> if (i == 4) i = 0 else i +=1; (x, i)}.filter(_._2 == 4).map(_._1)
//    println("Load ref: " + ref.count() + " reg. " + (System.currentTimeMillis() - startTime) / 1000 + " sec.")
//    println("Load exp: " + exp.count() + " reg. " + (System.currentTimeMillis() - startTime) / 1000 + " sec.")
    val res = GenometricMap(ref, exp, bin, sc)
    println("MAP: " + res.count())
    println("Execution time for MAP array-based: " + (System.currentTimeMillis() - startTime) / 1000)

  }

  def difference(pathRef: String, pathExp: String, bin: Long): Unit = {
    sc.getConf.setAppName("DIFF")

    val startTime = System.currentTimeMillis()
    val ref = Import.readAvro(pathRef, sc)
    var i = 0;
    val exp = Import.readAvro(pathExp, sc).map{x=> if (i == 4) i = 0 else i +=1; (x, i)}.filter(_._2 == 4).map(_._1)
//    println("Load ref: " + ref.count() + " reg. " + (System.currentTimeMillis() - startTime) / 1000 + " sec.")
//    println("Load exp: " + exp.count() + " reg. " + (System.currentTimeMillis() - startTime) / 1000 + " sec.")
    val res = GenometricDifference(ref, exp, bin, false, sc)
    println("DIFFERENCE: " + res.count())
    println("Execution time for DIFFERENCE array-based: " + (System.currentTimeMillis() - startTime) / 1000)
  }

  def union(pathRef: String, pathExp: String): Unit = {
    sc.getConf.setAppName("UNION")

    val startTime = System.currentTimeMillis()
    val ref = Import.readAvro(pathRef, sc)
    var i = 0;
    val exp = Import.readAvro(pathExp, sc).map{x=> if (i == 4) i = 0 else i +=1; (x, i)}.filter(_._2 == 4).map(_._1)
//    println("Load ref: " + ref.count() + " reg. " + (System.currentTimeMillis() - startTime) / 1000 + " sec.")
//    println("Load exp: " + exp.count() + " reg. " + (System.currentTimeMillis() - startTime) / 1000 + " sec.")
    val res = UnionRD(List(-1, -1, -1), ref, exp, sc)
    println("UNION: " + res.count())
    println("Execution time for UNION array-based: " + (System.currentTimeMillis() - startTime) / 1000)

  }

}
