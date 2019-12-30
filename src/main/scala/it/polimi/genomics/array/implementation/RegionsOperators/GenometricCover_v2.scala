package it.polimi.genomics.array.implementation.RegionsOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{CompactBuffer, GAttributes, GRegionKey}
import it.polimi.genomics.core.DataStructures.CoverParameters.CoverFlag.CoverFlag
import it.polimi.genomics.core.DataStructures.CoverParameters._
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToRegion
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GDouble, GValue}
import org.apache.log4j.Logger
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.immutable.HashMap

/**
  * Created by Olga Gorlova on 21/11/2019.
  */
object GenometricCover_v2 {

  type Grecord = (String, Long, Long, Char, Array[GValue])

  private final val logger = Logger.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(coverFlag: CoverFlag, min: CoverParam, max: CoverParam, aggregators: List[RegionsToRegion], /*grouping: Option[MetaGroupOperator], */ inputDataset: RDD[GARRAY], binSize: Long, sc: SparkContext): RDD[GARRAY] = {
    logger.info("----------------Cover executing..")

//    val aggIndexes = aggregators.map(a=> a.index)
//    val prepareAttributes = if (aggregators.nonEmpty) ds.map{x=>
//      (x._1, new GAttributes(x._2._1, x._2._2.zipWithIndex.filter(y=> aggIndexes.contains(y._2)).map(_._1)))
//    } else ds

    // CREATE DATASET RECURSIVELY
    val groups: Option[Broadcast[Map[Long, Long]]] = /*if (grouping.isDefined)
      Some(sc.broadcast(executor.implement_mgd(grouping.get, sc).collectAsMap()))
    else */None

    val groupIds = if (groups.isDefined) {
      groups.get.value.toList.map(_._2).distinct
    } else {
      List(0L)
    }


    //ASSGIN GROUPS TO THE DATA AND IF NOT GROUPED GIVE ID 0 AS THE GROUP ID
    val strandedDs: RDD[(GRegionKey, GAttributes)] =
      assignGroups(inputDataset)

    // EXTRACT START-STOP POINTS
    val extracted: RDD[((String, Int, Char), HashMap[Int, Int])] =
      extractor(strandedDs, binSize)

    // PREPARE COVER PARAMETERS
    // calculate the value only if needed
    val allValue: Int =
    if (min.isInstanceOf[ALL] || max.isInstanceOf[ALL]) {
        val samplesCount = strandedDs.flatMap(x => x._2._1.map(_._1)).distinct().count.toInt
//        groupIds.map(v => ((v, samplesCount))).foldRight(new HashMap[Long, Int])((a, z) => z + a)
      samplesCount
    } else {
      0
    }

    val minimum: Int =
      if (min.isInstanceOf[ALL]) min.fun(allValue)
      else if (min.isInstanceOf[ANY]) 1 /*groupIds.map(v => ((v, 1))).foldRight(new HashMap[Long, Int])((a, z) => z + a)*/
      else min.asInstanceOf[N].n

    val maximum =
      if (max.isInstanceOf[ALL]) max.fun(allValue)
      else if (max.isInstanceOf[ANY]) Int.MaxValue /*groupIds.map(v => ((v, Int.MaxValue))).foldRight(new HashMap[Long, Int])((a, z) => z + a)*/
      else max.asInstanceOf[N].n /*groupIds.map(v => ((v, max.asInstanceOf[N].n))).foldRight(new HashMap[Long, Int])((a, z) => z + a)*/


    // EXECUTE COVER ON BINS
    val ss = extracted
      // collapse coincident point
      .reduceByKey { (a, b) => {
      b.foldLeft(a) { case (m, (k, v2)) =>
        m.updated(k, m.get(k) match {
          case Some(v1) => v1 + v2
          case None => v2
        })
      }
    }
    }

    val binnedPureCover: RDD[Grecord] = ss.flatMap(bin => {
      val points: List[(Int, Int)] = bin._2.toList.sortBy(_._1)
      coverFlag match {
        case CoverFlag.COVER => coverHelper(points.iterator, minimum, maximum, bin._1._1, bin._1._3, bin._1._2, binSize)
        case CoverFlag.HISTOGRAM => histogramHelper(points.iterator, minimum, maximum, bin._1._1, bin._1._3, bin._1._2, binSize)
        case CoverFlag.SUMMIT => summitHelper(points.iterator, minimum, maximum, bin._1._1, bin._1._3, bin._1._2, binSize)
        case CoverFlag.FLAT => coverHelper(points.iterator, minimum, maximum, bin._1._1, bin._1._3, bin._1._2, binSize)
      }
    })

    //SPLIT DATASET -> FIND REGIONS THAT MAY CROSS A BIN
    val valid: RDD[Grecord] =
      binnedPureCover.filter((v) => (v._2 % binSize != 0 && v._3 % binSize != 0))
    val notValid: RDD[Grecord] =
      binnedPureCover.filter((v) => (v._2 % binSize == 0 || v._3 % binSize == 0))

    // JOIN REGIONS THAT
    // ARE IN THE SAME CHROMOSOME
    // HAVE COINCIDENT STOP AND START
    // HAVE SAME INTERSECTION COUNTER
    val joined: RDD[Grecord] =
    notValid
      .groupBy(x => (x._1, x._4))
      .flatMap { x =>
        val i = x._2.toList.sortBy(x => (x._2, x._3)).iterator
        var out: CompactBuffer[(String, Long, Long, Char, Array[GValue])] = CompactBuffer[Grecord]()
        if (i.hasNext) {
          var old: Grecord = i.next()
          while (i.hasNext) {
            val current = i.next()
            if (old._3.equals(current._2) && (!coverFlag.equals(CoverFlag.HISTOGRAM) || old._5(0).asInstanceOf[GDouble].v.equals(current._5(0).asInstanceOf[GDouble].v))) {
              old = (old._1, old._2, current._3, old._4, Array[GValue]() :+ GDouble(Math.max(old._5(0).asInstanceOf[GDouble].v, current._5(0).asInstanceOf[GDouble].v)))
            } else {
              out += old
              old = current
            }
          }
          out += old
        }
        out
      }

    val pureCover: RDD[Grecord] = valid.union(joined)

    val flat: Boolean = coverFlag.equals(CoverFlag.FLAT)
    val summit: Boolean = coverFlag.equals(CoverFlag.SUMMIT)

    GMAP4(aggregators, flat, summit, pureCover, strandedDs, binSize)
  }

  // EXECUTORS

  def parseChrom(chr: String): Byte ={
    val chrId = chr.toLowerCase().stripPrefix("chr")

    chrId match {
      case "x" => 23
      case "y" => 24
      case "m" => 25
      case _ => {
        try{ chrId.toByte } catch { case ex : Exception => 0 }
      }
    }
  }

  /**
    *
    * @param start
    * @param count
    * @param countMax
    * @param recording
    * @param minimum
    * @param maximum
    * @param i
    */
  final def coverHelper(si: Iterator[(Int, Int)], minimum: Int, maximum: Int, chr: String, strand: Char, bin: Int, binSize: Long): List[Grecord] = {
    var start = 0L
    var count = 0
    var countMax = 0
    var recording = false
    si.flatMap { current =>

      val newCount = count + current._2

      val newRecording = (newCount >= minimum && newCount <= maximum && si.hasNext)

      val newCountMax =
        if (!newRecording && recording) {
          0
        } else {
          if (newRecording && newCount > countMax) {
            newCount
          } else {
            countMax
          }
        }
      val newStart: Long =
        if (newCount >= minimum && newCount <= maximum && !recording) {
          val begin = 0
          if (current._1 < begin) {
            begin
          }
          else {
            current._1
          }
        } else {
          start
        }
      val out = if (!newRecording && recording) {
        //output a region
        val end = binSize
        Some(((chr, newStart + bin * binSize, if (current._1 > end) end + bin * binSize else current._1 + bin * binSize, strand, new Array[GValue](0) :+ GDouble(countMax))))
      } else
        None

      count = newCount
      start = newStart
      countMax = newCountMax
      recording = newRecording
      out //::: coverHelper(newStart, newCount, newCountMax, newRecording, minimum, maximum, i, id, chr, strand, bin, binSize)
    }.toList
  }

  /**
    * Tail recursive helper for histogram
    *
    * @param start
    * @param count
    * @param minimum
    * @param maximum
    * @param i
    */
  final def histogramHelper(i: Iterator[(Int, Int)], minimum: Int, maximum: Int, chr: String, strand: Char, bin: Int, binSize: Long): List[Grecord] = {
    var count = 0
    var start = 0L
    val res = i.flatMap { current =>
      val newCount = count + current._2
      //condition just to check the start of the bin
      if (start.equals(current._1)) {
        count = newCount
        None
      } // ALL THE ITERATIONS GOES HERE
      else {
        //IF THE NEW COUNT IS  ACCEPTABLE AND DIFFERENT FROM THE PREVIOUS COUNT = > NEW REGION START
        val newStart: Long = if (newCount >= minimum && newCount <= maximum && newCount != count)
          current._1
        else
          start

        //output a region
        val out = if (count >= minimum && count <= maximum && newCount != count && count != 0) {
          val end = binSize
          val endBin = bin * binSize
          val newStart1 = start + bin * binSize
          val newEnd1 = current._1 + bin * binSize
          Some((chr, start + bin * binSize, current._1 + bin * binSize, strand, Array[GValue](GDouble(count))))
        }
        else None

        start = newStart
        count = newCount
        out
      }
    }.toList

    res
  }

  /**
    *
    * @param start
    * @param count
    * @param minimum
    * @param maximum
    * @param growing
    * @param i
    */
  final def summitHelper(i: Iterator[(Int, Int)], minimum: Int, maximum: Int, chr: String, strand: Char, bin: Int, binSize: Long): List[Grecord] = {
    var start = 0L
    var count = 0
    var valid = false
    var reEnteredInValidZoneDecreasing = false
    var growing = false

    i.flatMap { current =>
      val newCount: Int = count + current._2
      val newValid = newCount >= minimum && newCount <= maximum
      val newGrowing: Boolean = newCount > count || (growing && newCount.equals(count))
      val newReEnteredInValidZoneDecreasing: Boolean = !valid && newValid

      if (start.equals(current._1)) {
        count = newCount
        valid = newValid
        reEnteredInValidZoneDecreasing = newReEnteredInValidZoneDecreasing
        growing = newGrowing
        None
      } else {
        val newStart: Long =
          if (newValid && newCount != count) {
            val begin = 0
            if (current._1 < begin) {
              begin
            }
            else {
              current._1
            }
          } else {
            start
          }

        val out = if (((valid && growing && (!newGrowing || !newValid)) || (reEnteredInValidZoneDecreasing && !newGrowing)) && count != 0) {
          //output a region
          val end = binSize
          Some((chr, start + bin * binSize, if (current._1 > end) end + bin * binSize else current._1 + bin * binSize, strand, Array[GValue]() :+ GDouble(count)))
        } else None

        start = newStart
        count = newCount
        valid = newValid
        reEnteredInValidZoneDecreasing = newReEnteredInValidZoneDecreasing
        growing = newGrowing
        out
      }
    }.toList
  }


  /**
    * Assign Group ID to the regions
    *
    * @param dataset  input dataset
    * @param grouping Group IDs
    * @return RDD with group ids set in each sample.
    */
  def assignGroups(dataset: RDD[GARRAY]): RDD[GARRAY] = {
    val containsStrandUnknown = dataset.map(x => x._1._4 == '*').fold(false)(_ || _)
    dataset.map { x =>
      val strand =
        if (containsStrandUnknown)
          '*'
        else
          x._1._4
      (new GRegionKey(x._1.chrom, x._1.start, x._1.stop, strand), x._2)
    }
  }


  def extractor(dataset: RDD[GARRAY], binSize: Long) = {
    dataset.flatMap { x =>
      val startBin = (x._1.start / binSize).toInt
      //      val stopBin = (x._4 / binSize).toInt
      val stopBin = if ((x._1.stop/binSize).toInt * binSize == x._1.stop) ((x._1.stop/binSize).toInt - 1) else (x._1.stop/binSize).toInt

      if (startBin == stopBin) {
        List(((x._1.chrom, startBin, x._1.strand), HashMap((x._1.start - startBin * binSize).toInt -> 1, (x._1.stop - startBin * binSize).toInt -> -1)))
      } else {
        val map_start = ((x._1.chrom, startBin, x._1.strand), HashMap((if (x._1.start - startBin * binSize != binSize) x._1.start - startBin * binSize else binSize - 1).toInt -> 1, binSize.toInt -> -1))
        val map_stop = ((x._1.chrom, stopBin, x._1.strand), HashMap((if (x._1.stop - stopBin * binSize != 0) x._1.stop - stopBin * binSize else 1).toInt -> -1, 0 -> 1))
        val map_int = for (i <- startBin + 1 to stopBin - 1) yield ((x._1.chrom, i, x._1.strand), HashMap(0 -> 1, binSize.toInt -> -1))

        List(map_start, map_stop) ++ map_int
      }
    }
  }


}
