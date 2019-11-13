package it.polimi.genomics.array.implementation.loaders

import com.google.common.hash.Hashing
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GArray, GAttributes, GRegionKey}
import it.polimi.genomics.array.test.DataFormatTest.sc
import it.polimi.genomics.array.utilities.KryoFile
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core._
import it.polimi.genomics.spark.implementation.loaders.{CustomParser, Loaders}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object Import {
  private final val logger = Logger.getLogger(this.getClass);

  def apply(path: String, sc:SparkContext):RDD[GARRAY] ={
    logger.info(s"----------------Import($path) executing...")
    toArray(loadDataset(path,sc))
  }

  def apply(rdd: RDD[GRECORD], sc:SparkContext):RDD[GARRAY] ={
    logger.info(s"----------------Import(${rdd.name}) executing...")
    toArray(rdd)
  }

  def apply(path: String, isArray:Boolean, sc:SparkContext):RDD[GArray] ={
    logger.info(s"----------------Import($path, object) executing...")
//    implicit def orderGrecord: Ordering[GARRAY] = Ordering.by{s => val e = s._1;(e._1,e._2,e._3,e._4)}
    val ds = loadArray2(path,sc)/*.repartition(100)*/
//    println("Import: "+ds.count())
    ds/*.persist()*/

//    val input = sc.textFile(path)
//    val firstLine = input.filter(_.startsWith("#")).first()
//    val schema: Array[(String, String)] = firstLine.substring(1, firstLine.length-1).split("__").map{ l=>
//        val values = l.split("=")
//        (values(0),values(1))
//      }
//
//    val ds = input.filter(_.startsWith(">")).flatMap{x=>
//      if (x.startsWith(">")) {
//        val line = x.substring(1, x.length-1).split(";").toIterator
//        val coord = line.next().split("__")
//        val ids = line.next().split("__").map{ v =>
//          val spl = v.split(":")
//          (Hashing.md5().newHasher().putString(path+"%"+spl.head).hash().asLong(), spl.last.toInt)
//        }
//
//        val att = (for (i <- 0 until schema.size) yield {
//          val nextAtt: Array[Array[GValue]] = line.next().split("__").map{ a=>
//            val tt: Array[String] = a.split(":")
//            val ss: Array[GValue] = tt.map{ t=>
//              schema(i)._2.toLowerCase() match {
//                case "int" => GInt(t.toInt)
//                case "double" => GDouble(t.toDouble)
//                case "string" => GString(t)
//                case "long" => GInt(t.toInt)
//                case "null" => GNull()
//                case _ => GNull()
//              }
//            }
//            ss
//          }
//          nextAtt
//        }).toArray
//
//        Some(new GRegionKey(coord(0), coord(1).toLong,coord(2).toLong, coord(3).charAt(0)), new GAttributes(ids, att))
//      }
//      else None
//    }
//
//    ds

  }


  def loadDataset(path: String, sc:SparkContext): RDD[GRECORD] = {
    Loaders.forPath(sc, path).LoadRegionsCombineFiles(new CustomParser().setSchema(path).region_parser)
  }

  private def loadArray(path: String, sc:SparkContext): RDD[GARRAY] = {
    val array: RDD[GARRAY] = sc.objectFile(path) /*KryoFile.objectFile[GARRAY](sc, path, 10)*/
    array
  }

  private def loadArray2(path: String, sc:SparkContext): RDD[GArray] = {
    val array: RDD[GArray] = sc.objectFile(path) /*KryoFile.objectFile[GARRAY](sc, path, 10)*/
    array
  }

  def toArray(rdd: RDD[(GRecordKey, Array[GValue])]): RDD[(GRegionKey, GAttributes)] ={
    val out: RDD[(GRegionKey, GAttributes)] = rdd.groupBy(x => (new GRegionKey(x._1._2, x._1._3, x._1._4, x._1._5)))
      .map { x =>
        val size = x._2.head._2.length
        val sizesInSamples = x._2.map(_._1.id).groupBy(identity).mapValues(_.size).toArray
        val ids = x._2.groupBy(_._1.id).map(_._1).toArray
        val values: Array[Array[Array[GValue]]] = (for (i <- 0 to (size-1)) yield {
          x._2.map{ s=> (s._1._1, s._2(i))}
            .groupBy(_._1)
            .map{ s=> s._2.map(_._2).toArray}.toArray
        }).toArray
        val samples: Array[(Long, Int)] = if (values.nonEmpty) ids.zip(values.head.map(_.length)) else sizesInSamples
        (x._1, new GAttributes(samples, values))
      }
    out
  }

}

