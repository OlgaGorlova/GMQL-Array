package it.polimi.genomics.array.implementation.RegionsOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GArray, GAttributes, GRegionKey}
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.CoverParameters.CoverFlag.CoverFlag
import it.polimi.genomics.core.DataStructures.CoverParameters.{ANY, CoverFlag, CoverParam, N}
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToRegion
import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.{GDouble, GValue}
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.Map

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
/**
  * Created by Olga Gorlova on 11/03/2019.
  *
  * Limitations (or TODO?):
  * - groupby option
  *
  * TODO:
  * 1) fix SUMMIT
  * 2) check JaccardIndexes, they seem to be not correct
  */
object GenometricCover {
  type Grecord = (Long, String, Long, Long, Char, Array[GValue])

  private final val logger = Logger.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor: GMQLArrayExecutor, coverFlag : CoverFlag, min : CoverParam, max : CoverParam, inputDataset : RegionOperator, binSize : Long, aggregators: List[RegionsToRegion], sc : SparkContext) : RDD[(GRegionKey, GAttributes)] = {
    logger.info("----------------GenometricCover executing..")

    val ds = executor.implement_rd(inputDataset, sc)

    execute(coverFlag, min, max, ds, binSize, aggregators, sc)
  }

  def apply(coverFlag : CoverFlag, min : CoverParam, max : CoverParam, inputDataset : RDD[GArray], binSize : Long, aggregators: List[RegionsToRegion], sc : SparkContext) : RDD[GArray] = {
    logger.info("----------------GenometricCover executing..")

    execute2(coverFlag, min, max, inputDataset, binSize, aggregators, sc)
  }

  def execute2(coverFlag : CoverFlag, min : CoverParam, max : CoverParam, ds : RDD[GArray], binSize : Long, aggregators: List[RegionsToRegion], sc : SparkContext) : RDD[GArray] = {

    val aggIndexes = aggregators.map(a=> a.index)
    val prepareAttributes = if (aggregators.nonEmpty) ds.map{x=>
      GArray(x.key, new GAttributes(x.values._1, x.values._2.zipWithIndex.filter(y=> aggIndexes.contains(y._2)).map(_._1)))
    } else ds

    //ASSGIN GROUPS TO THE DATA AND IF NOT GROUPED GIVE ID 0 AS THE GROUP ID
    val strandedDs: RDD[GArray] =
    //      assignStrand( prepareAttributes )
      assignGroups2( prepareAttributes )

    // BIN INPUT DATASET. IF REGION BELONGS TO SEVERAL BINS, SPLIT IT BY BIN SIZE
    // ((chr, binId, strand), (regionId, start, stop, attributes))
    val binnedDs: RDD[((String, Int, Char), (Long, Long, Long, GAttributes, Int))] =
    extractor2(strandedDs, binSize)

    // PREPARE COVER PARAMETERS
    // calculate the value only if needed
    val minimum = /*if (min.isInstanceOf[ALL]) inputDataset.map(_._2._1.map(_._1)).flatMap(f=>f).distinct().count().toInt
                  else */if (min.isInstanceOf[ANY]) Int.MaxValue
    else min.asInstanceOf[N].n


    val maximum = /*if (min.isInstanceOf[ALL]) inputDataset.map(_._2._1.map(_._1)).flatMap(f=>f).distinct().count().toInt
                  else */if (max.isInstanceOf[ANY]) Int.MaxValue
    else max.asInstanceOf[N].n

    // SPLIT INTERVALS IN EACH BIN
    val splittedDs: RDD[((String, Int, Char), (Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean))] = binnedDs.groupByKey.flatMapValues(bin => {
      val points: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)] =
        bin.toList.map(r=> (r._2, r._3, Map(r._1 -> r._4), r._5, r._2, r._3)).groupBy(g=> (g._1, g._2)).map{ r=>
          if (r._2.length > 1) {
            val newR = r._2.foldLeft((Map[Long, GAttributes](), 0, 0l, 0l)) { (acc, z) => (acc._1 ++ z._3, acc._2 + z._4, z._5, z._6) }
            (r._1._1, r._1._2, newR._1, newR._2, newR._3, newR._4)
          } else
            (r._1._1, r._1._2, r._2.head._3, r._2.head._4, r._2.head._5, r._2.head._6)
        }.toList.sortBy(s=> (s._1,s._2))

      coverFlag match {
        case CoverFlag.COVER => coverHelper(points, minimum, maximum)
        case CoverFlag.HISTOGRAM => histogramHelper(points, minimum, maximum)
        case CoverFlag.SUMMIT => summitHelper(points, minimum, maximum)
        case CoverFlag.FLAT => coverHelper(points, minimum, maximum)
      }
    })

    //regions that are completely inside a bin
    val valid: RDD[((String, Int, Char), (Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean))] =
      splittedDs.filter((v) => (v._2._1 % binSize != 0 && v._2._2 % binSize != 0))

    //regions that are on bin boundaries
    val notValid: RDD[((String, Int, Char), (Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean))] =
      splittedDs.filter((v) => (v._2._1 % binSize == 0 || v._2._2 %  binSize == 0))

    // merge regions between bins
    // JOIN REGIONS THAT
    // ARE IN THE SAME CHROMOSOME
    // HAVE COINCIDENT STOP AND START
    // HAVE SAME INTERSECTION COUNTER
    val mergedDs: RDD[(GRegionKey, Map[Long, GAttributes], Int, Long, Long, Boolean)] = mergeBin(notValid, coverFlag).map{ x=>
      (new GRegionKey(x._1._1, x._2._1, x._2._2, x._1._3), x._2._3, x._2._4, x._2._5, x._2._6, x._2._7)
    }

    //produce final result
    //compute Jaccar Indexes and Aggregations
    val res = valid.map{ x=>
      (new GRegionKey(x._1._1, x._2._1, x._2._2, x._1._3), x._2._3, x._2._4, x._2._5, x._2._6, x._2._7)
    }.union(mergedDs).map{x=>
      val start : Long = if(coverFlag == CoverFlag.FLAT) x._4 else x._1.start
      val end : Long = if (coverFlag == CoverFlag.FLAT) x._5 else x._1.stop

      // Accumulation Index
      val accIndex = Array(Array[GValue](GDouble(x._3)))
      // JaccardResult
      //      val jaccardResult = if(x._5-x._4 != 0){ GDouble(Math.abs((x._1.stop -x._1.start).toDouble/(x._5-x._4 ).toDouble)) } else { GDouble(0) }
      // JaccardIntersect
      //      val jaccardIntersect = if(!x._6) jaccardResult else GDouble(0)
      val preparedValues = x._2.values.foldLeft(new Array[Array[GValue]](0)) { (acc, z) =>
        if (acc.isEmpty)
          z._2.map(v => v.flatten)
        else
          acc.zip(z._2.map(v => v.flatten)).map(v => v._1 ++ v._2)
      }

      val aggregations = aggregators.map { a =>
        val agg = a.funOut(a.fun(preparedValues(a.index).toList), (1, preparedValues(a.index).length))
        Array(Array(agg))
      }.toArray

      //      val newVal = aggregators.map{a=>
      //        x._2.flatMap{s=>
      ////          val notNull = s._2._2(a.index)._1.flatten.filter(f=> !f.isInstanceOf[GNull])
      //          val agg = a.funOut(a.fun(s._2._2(a.index)._1.flatten.toList), (1, x._2.size))
      //          Some(agg)
      //        }.toArray
      //      }.toArray

      val samples = Array((0l, 1))
      val values: Array[Array[Array[GValue]]] = Array(/*(Array(Array(GDouble(sum))), 1),*/ /*(Array(Array(GDouble(avg))), 1),*/ Array(Array(GDouble(x._3))) /*opInChain+*//*, (Array(Array(jaccardResult)), opInChain+1),(Array(Array(jaccardIntersect)), opInChain+1), (Array(Array(GDouble(x._3))), opInChain+1) */)
      val attributes = new GAttributes(samples, (accIndex +: aggregations))
      GArray(new GRegionKey(x._1.chrom, start, end, x._1.strand), attributes)
    }

    res
  }


  def execute(coverFlag : CoverFlag, min : CoverParam, max : CoverParam, ds : RDD[GARRAY], binSize : Long, aggregators: List[RegionsToRegion], sc : SparkContext) : RDD[(GRegionKey, GAttributes)] = {

    val aggIndexes = aggregators.map(a=> a.index)
    val prepareAttributes = if (aggregators.nonEmpty) ds.map{x=>
      (x._1, new GAttributes(x._2._1, x._2._2.zipWithIndex.filter(y=> aggIndexes.contains(y._2)).map(_._1)))
    } else ds

    //ASSGIN GROUPS TO THE DATA AND IF NOT GROUPED GIVE ID 0 AS THE GROUP ID
    val strandedDs: RDD[(GRegionKey, GAttributes)] =
    //      assignStrand( prepareAttributes )
      assignGroups( prepareAttributes )

    // BIN INPUT DATASET. IF REGION BELONGS TO SEVERAL BINS, SPLIT IT BY BIN SIZE
    // ((chr, binId, strand), (regionId, start, stop, attributes))
    val binnedDs: RDD[((String, Int, Char), (Long, Long, Long, GAttributes, Int))] =
    extractor(strandedDs, binSize)

    // PREPARE COVER PARAMETERS
    // calculate the value only if needed
    val minimum = /*if (min.isInstanceOf[ALL]) inputDataset.map(_._2._1.map(_._1)).flatMap(f=>f).distinct().count().toInt
                  else */if (min.isInstanceOf[ANY]) Int.MaxValue
    else min.asInstanceOf[N].n


    val maximum = /*if (min.isInstanceOf[ALL]) inputDataset.map(_._2._1.map(_._1)).flatMap(f=>f).distinct().count().toInt
                  else */if (max.isInstanceOf[ANY]) Int.MaxValue
    else max.asInstanceOf[N].n

    // SPLIT INTERVALS IN EACH BIN
    val splittedDs: RDD[((String, Int, Char), (Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean))] = binnedDs.groupByKey.flatMapValues(bin => {
      val points: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)] =
        bin.toList.map(r=> (r._2, r._3, Map(r._1 -> r._4), r._5, r._2, r._3)).groupBy(g=> (g._1, g._2)).map{ r=>
          if (r._2.length > 1) {
            val newR = r._2.foldLeft((Map[Long, GAttributes](), 0, 0l, 0l)) { (acc, z) => (acc._1 ++ z._3, acc._2 + z._4, z._5, z._6) }
            (r._1._1, r._1._2, newR._1, newR._2, newR._3, newR._4)
          } else
            (r._1._1, r._1._2, r._2.head._3, r._2.head._4, r._2.head._5, r._2.head._6)
        }.toList.sortBy(s=> (s._1,s._2))

      coverFlag match {
        case CoverFlag.COVER => coverHelper(points, minimum, maximum)
        case CoverFlag.HISTOGRAM => histogramHelper(points, minimum, maximum)
        case CoverFlag.SUMMIT => summitHelper(points, minimum, maximum)
        case CoverFlag.FLAT => coverHelper(points, minimum, maximum)
      }
    })

    //regions that are completely inside a bin
    val valid: RDD[((String, Int, Char), (Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean))] =
      splittedDs.filter((v) => (v._2._1 % binSize != 0 && v._2._2 % binSize != 0))

    //regions that are on bin boundaries
    val notValid: RDD[((String, Int, Char), (Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean))] =
      splittedDs.filter((v) => (v._2._1 % binSize == 0 || v._2._2 %  binSize == 0))

    // merge regions between bins
    // JOIN REGIONS THAT
    // ARE IN THE SAME CHROMOSOME
    // HAVE COINCIDENT STOP AND START
    // HAVE SAME INTERSECTION COUNTER
    val mergedDs: RDD[(GRegionKey, Map[Long, GAttributes], Int, Long, Long, Boolean)] = mergeBin(notValid, coverFlag).map{ x=>
      (new GRegionKey(x._1._1, x._2._1, x._2._2, x._1._3), x._2._3, x._2._4, x._2._5, x._2._6, x._2._7)
    }

    //produce final result
    //compute Jaccar Indexes and Aggregations
    val res = valid.map{ x=>
      (new GRegionKey(x._1._1, x._2._1, x._2._2, x._1._3), x._2._3, x._2._4, x._2._5, x._2._6, x._2._7)
    }.union(mergedDs).map{x=>
      val start : Long = if(coverFlag == CoverFlag.FLAT) x._4 else x._1.start
      val end : Long = if (coverFlag == CoverFlag.FLAT) x._5 else x._1.stop

      // Accumulation Index
      val accIndex = Array(Array[GValue](GDouble(x._3)))
      // JaccardResult
      //      val jaccardResult = if(x._5-x._4 != 0){ GDouble(Math.abs((x._1.stop -x._1.start).toDouble/(x._5-x._4 ).toDouble)) } else { GDouble(0) }
      // JaccardIntersect
      //      val jaccardIntersect = if(!x._6) jaccardResult else GDouble(0)
      val preparedValues = x._2.values.foldLeft(new Array[Array[GValue]](0)) { (acc, z) =>
        if (acc.isEmpty)
          z._2.map(v => v.flatten)
        else
          acc.zip(z._2.map(v => v.flatten)).map(v => v._1 ++ v._2)
      }

      val aggregations = aggregators.map { a =>
        val agg = a.funOut(a.fun(preparedValues(a.index).toList), (1, preparedValues(a.index).length))
        Array(Array(agg))
      }.toArray

      //      val newVal = aggregators.map{a=>
      //        x._2.flatMap{s=>
      ////          val notNull = s._2._2(a.index)._1.flatten.filter(f=> !f.isInstanceOf[GNull])
      //          val agg = a.funOut(a.fun(s._2._2(a.index)._1.flatten.toList), (1, x._2.size))
      //          Some(agg)
      //        }.toArray
      //      }.toArray

      val samples = Array((0l, 1))
      val values: Array[Array[Array[GValue]]] = Array(/*(Array(Array(GDouble(sum))), 1),*/ /*(Array(Array(GDouble(avg))), 1),*/ Array(Array(GDouble(x._3))) /*opInChain+*//*, (Array(Array(jaccardResult)), opInChain+1),(Array(Array(jaccardIntersect)), opInChain+1), (Array(Array(GDouble(x._3))), opInChain+1) */)
      val attributes = new GAttributes(samples, (accIndex +: aggregations))
      (new GRegionKey(x._1.chrom, start, end, x._1.strand), attributes)
    }

    res
  }

  // EXECUTORS

  /**
    * Tail recursive helper for split
    *
    * @param input
    */
  def split(input: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)]): List[( Long, Long, Map[Long, GAttributes], Int, Long, Long)] = {

    // ( start, stop, Map(regionId -> attributes), count, start_union, stop_union )
    var regionCache: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)] = List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)]()
    val regions = input.iterator

    val toOut = regions.flatMap{
      record =>
        var temp: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)] = List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)]()
        var current = record
        if (regionCache.isEmpty)
        {
          regionCache ::= current
          if (!regions.hasNext) regionCache else None
        }
        else
        {
          val cache = regionCache.sortBy(y=>(y._1,y._2)).iterator
          var res: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)] = cache.flatMap { cRegion =>
            val out =
            // if cRegion intersect with current
              if (cRegion._1 < current._2 && current._1 < cRegion._2) {
                if (cRegion._1 == current._1 && cRegion._2 == current._2) {
                  temp ::= (current._1, current._2, cRegion._3 ++ current._3, cRegion._4 + current._4, Math.min(cRegion._5, current._5), Math.max(cRegion._6, current._6))
                  None
                }
                else {
                  temp ::= (Math.max(cRegion._1, current._1), Math.min(cRegion._2, current._2), cRegion._3 ++ current._3, cRegion._4 + current._4, Math.min(cRegion._5, current._5), Math.max(cRegion._6, current._6))


                  // right
                  if (cRegion._2 != current._2) {
                    if (cRegion._2 > current._2) {
                      temp ::= (Math.min(cRegion._2, current._2), Math.max(cRegion._2, current._2), cRegion._3, cRegion._4, cRegion._5, cRegion._6)
                    }
                    else {
                      if (!cache.hasNext) {
                        temp ::= (Math.min(cRegion._2, current._2), Math.max(cRegion._2, current._2), current._3, current._4, current._5, current._6)
                      }
                    }
                  }

                  // left
                  if (cRegion._1 < current._1)
                    Some((Math.min(cRegion._1, current._1), Math.max(cRegion._1, current._1), cRegion._3, cRegion._4, cRegion._5, cRegion._6))
                  else {
                    current = (if (cRegion._1 == current._1) cRegion._2 else cRegion._1, current._2, current._3, current._4, current._5, current._6)
                    if (cRegion._1 > current._1) {
                      val oldTemp = if (temp.nonEmpty) temp.map(t=> (t._1, t._2)) else List[(Long, Long)]()
                      if (oldTemp.contains(current._1, cRegion._1)) {
                        val index = oldTemp.indexOf((current._1, cRegion._1))
                        val oldTempReg = temp(oldTemp.indexOf((cRegion._1, cRegion._2)))
                        val diff2 = current._3.keySet diff oldTempReg._3.keySet
                        val toTempMap = oldTempReg._3 ++ current._3.filter(f => diff2.contains(f._1))
                        val toTempAccInd = current._3.filter(f => diff2.contains(f._1)).foldLeft(0) { (acc, z) => acc + z._2._1.foldLeft(0) { (acc1, z1) => acc1 + z1._2 } }
                        val newTemp = temp.take(index) ++ temp.drop(index + 1) :+ (oldTempReg._1, oldTempReg._2, toTempMap, oldTempReg._4 + toTempAccInd, Math.min(oldTempReg._5, current._5), Math.max(oldTempReg._6, current._6))
                        temp = List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)]()
                        temp = newTemp
                      } else
                        temp ::= (current._1, cRegion._1, current._3, current._4, current._5, current._6)
                    }
                    None
                  }
                }
              }
              else
              {
                if (cRegion._1 >= current._2){
                  val oldTemp = if (temp.nonEmpty) temp.map(t=> (t._1, t._2)) else List[(Long, Long)]()
                  if (oldTemp.contains(cRegion._1, cRegion._2)) {
                    val index = oldTemp.indexOf((cRegion._1, cRegion._2))
                    val oldTempReg = temp(oldTemp.indexOf((cRegion._1, cRegion._2)))
                    val diff1 = cRegion._3.keySet diff oldTempReg._3.keySet;
                    val toTempMap = oldTempReg._3 ++ cRegion._3.filter(f => diff1.contains(f._1))
                    val toTempAccInd = cRegion._3.filter(f => diff1.contains(f._1)).foldLeft(0) { (acc, z) => acc + z._2._1.foldLeft(0) { (acc1, z1) => acc1 + z1._2 } }
                    val newTemp = temp.take(index) ++ temp.drop(index + 1) :+ (oldTempReg._1, oldTempReg._2, toTempMap, oldTempReg._4 + toTempAccInd, Math.min(oldTempReg._5, cRegion._5), Math.max(oldTempReg._6, cRegion._6))
                    temp = List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)]()
                    temp = newTemp
                  } else
                  temp ::= cRegion
                  None
                } else {
                  if (!cache.hasNext) {
                    val oldTemp = if (temp.nonEmpty) temp.map(t=> (t._1, t._2)) else List[(Long, Long)]()
                    if (oldTemp.contains(current._1, current._2)) {
                      val index = oldTemp.indexOf((current._1, current._2))
                      val oldTempReg = temp(oldTemp.indexOf((current._1, current._2)))
                      val diff2 = current._3.keySet diff oldTempReg._3.keySet
                      val toTempMap = oldTempReg._3 ++ current._3.filter(f => diff2.contains(f._1))
                      val toTempAccInd = current._3.filter(f => diff2.contains(f._1)).foldLeft(0) { (acc, z) => acc + z._2._1.foldLeft(0) { (acc1, z1) => acc1 + z1._2 } }
                      val newTemp = temp.take(index) ++ temp.drop(index + 1) :+ (oldTempReg._1, oldTempReg._2, toTempMap, oldTempReg._4 + toTempAccInd, Math.min(oldTempReg._5, current._5), Math.max(oldTempReg._6, current._6))
                      temp = List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)]()
                      temp = newTemp
                    } else
                      temp ::= current
                  }
                  Some(cRegion)
                }
              }

            if (!cache.hasNext) {
              regionCache = List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)]()
              regionCache = temp
              temp = List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)]()
              out
            }
            else{
              regionCache = List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)]()
              regionCache = temp
              out
            }


          }.toList

          if (regions.hasNext) res else res ::: regionCache

        }
    }.toList

    toOut
  }

  def merge(input: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean)])=
  {
    val i = input.iterator
    var out = List[( Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean)]()
    if (i.hasNext) {
      var old: (Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean) = i.next()
      while (i.hasNext) {
        val current = i.next()
        if ( old._2.equals(current._1) /*&& old._4.equals(current._4)*/ ) {
          old = ( old._1, current._2, old._3 ++ current._3, Math.max(old._4, current._4), Math.min(old._5, current._5), Math.max(old._6, current._6), true)
        } else {
          out = out :+ old
          old = current
        }
      }
      out = out :+ old
    }
    out
  }

  def mergeBin(input: RDD[((String, Int, Char), (Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean))], coverFlag: CoverFlag)=
  {
    input
      .groupBy(x=>(x._1._1,x._1._3))
      .flatMap{x=>
        val i = x._2.toList.sortBy(x=>(x._2._1,x._2._2)).iterator
        var out = List[((String, Int, Char), (Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean))]()
        if (i.hasNext) {
          var old: ((String, Int, Char), (Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean)) = i.next()
          while (i.hasNext) {
            val current = i.next()
            if (old._2._2.equals(current._2._1) && (!coverFlag.equals(CoverFlag.HISTOGRAM) || old._2._4.equals(current._2._4)) ) {
              old = (old._1, (old._2._1, current._2._2, old._2._3 ++ current._2._3, Math.max(old._2._4, current._2._4), Math.min(old._2._5, current._2._5), Math.max(old._2._6, current._2._6), true))
            } else {
              out = out :+ old
              old = current
            }
          }
          out = out :+ old
        }
        out
      }
  }

  /**
    * Tail recursive helper for histogram
    *
    * @param points
    * @param minimum
    * @param maximum
    */
  final def histogramHelper(points: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)], minimum:Int, maximum: Int) = {
    // 3 steps : SPLIT, FILTER, MERGE
    val splitBin: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)] = split(points)
    val filteredBin = splitBin.filter { x => (x._4 >= minimum && x._4 <= maximum)}.map(f=> (f._1, f._2, f._3, f._4, f._5, f._6, false))

    val i = filteredBin.sortBy(_._2).iterator
    var out = List[( Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean)]()
    if (i.hasNext) {
      var old: (Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean) = i.next()
      while (i.hasNext) {
        val current = i.next()
        if ( old._2.equals(current._1) && old._4.equals(current._4) ) {
          old = ( old._1, current._2, old._3 ++ current._3, Math.max(old._4, current._4), Math.min(old._5, current._5), Math.max(old._6, current._6), true)
        } else {
          out = out :+ old
          old = current
        }
      }
      out = out :+ old
    }
    val mergedBin: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean)] = out

    mergedBin
  }

  /**
    * Tail recursive helper for cover
    *
    * @param points
    * @param minimum
    * @param maximum
    */
  final def coverHelper(points: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)], minimum:Int, maximum: Int) = {
    // 3 steps : SPLIT, FILTER, MERGE
    val splitBin: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)] = split(points)
    val filteredBin = splitBin.filter { x => (x._4 >= minimum && x._4 <= maximum)}.map(f=> (f._1, f._2, f._3, f._4, f._5, f._6, false))
    val mergedBin: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean)] = merge(filteredBin.sortBy(_._2))

    mergedBin
  }

  /**
    * Tail recursive helper for summit
    *
    * @param points
    * @param minimum
    * @param maximum
    */
  final def summitHelper(points: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)], minimum:Int, maximum: Int) = {
    // 3 steps : SPLIT, FILTER, MERGE
    val splitBin: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)] = split(points)
    val filteredBin = splitBin.filter { x => (x._4 >= minimum && x._4 <= maximum)}.map(f=> (f._1, f._2, f._3, f._4, f._5, f._6, false))
    //    val mergedBin: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean)] = merge(filteredBin.sortBy(_._2))

    val i = filteredBin.sortBy(_._2).iterator
    var out = List[( Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean)]()
    if (i.hasNext) {
      var old: (Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean) = i.next()
      while (i.hasNext) {
        val current = i.next()
        if ( old._2.equals(current._1) && old._4.equals(current._4) ) { // if accIndex is equal, then merge 2 regions
          old = ( old._1, current._2, old._3 ++ current._3, Math.max(old._4, current._4), Math.min(old._5, current._5), Math.max(old._6, current._6), true)
        } else if (old._2.equals(current._1) && old._4 > current._4) { // if old.accIndex > current.accIndex, then return old to the result and put current to old
          out = out :+ old
          old = current
        } else if (old._2.equals(current._1) && old._4 < current._4) { // if old.accIndex < current.accIndex, then don't return anything and put current to old
          old = current
        }
        else {
          out = out :+ old
          old = current
        }
      }
      out = out :+ old
    }
    val mergedBin: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean)] = out

    mergedBin
  }

  /**
    * Tail recursive helper for flat
    *
    * @param points
    * @param minimum
    * @param maximum
    */
  final def flatHelper(points: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)], minimum:Int, maximum: Int) = {
    // 3 steps : SPLIT, FILTER, MERGE
    val splitBin: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long)] = split(points)
    val filteredBin = splitBin.filter { x => (x._4 >= minimum && x._4 <= maximum)}.map(f=> (f._1, f._2, f._3, f._4, f._5, f._6, false))
    val mergedBin: List[(Long, Long, Map[Long, GAttributes], Int, Long, Long, Boolean)] = merge(filteredBin.sortBy(_._2))

    mergedBin
  }


  //PREPARATORS

  def assignStrand(dataset : RDD[GARRAY]): RDD[GARRAY] = {
    val strands = dataset.map(x=>x._1._4).distinct().collect()
    val doubleStrand = if(strands.size > 2) true else false
    val positiveStrand = if(strands.size == 2 && strands.contains('+') ) true else false
    dataset.flatMap{x =>
      if (x._1._4.equals('*')&& doubleStrand) {
        List((new GRegionKey(x._1._1, x._1._2, x._1._3, '-'), x._2)
          ,(new GRegionKey(x._1._1, x._1._2, x._1._3, '+'), x._2))
      }else if (x._1._4.equals('*') && positiveStrand) {
        //output is Positive strand because the data contains only Positive strand
        List((new GRegionKey(x._1._1, x._1._2, x._1._3, '+'), x._2))
      } else if (x._1._4.equals('*') && !positiveStrand && doubleStrand) {
        //output is minus strand because the data contains only minus strand
        List((new GRegionKey(x._1._1, x._1._2, x._1._3, '-'), x._2))
      } else {
        List((new GRegionKey(x._1._1, x._1._2, x._1._3, x._1._4), x._2))
      }

    }
  }

  def assignStrand2(dataset : RDD[GArray]): RDD[GArray] = {
    val strands = dataset.map(x=>x.key._4).distinct().collect()
    val doubleStrand = if(strands.size > 2) true else false
    val positiveStrand = if(strands.size == 2 && strands.contains('+') ) true else false
    dataset.flatMap{x =>
      if (x.key._4.equals('*')&& doubleStrand) {
        List(GArray(new GRegionKey(x.key._1, x.key._2, x.key._3, '-'), x.values)
          ,GArray(new GRegionKey(x._1._1, x._1._2, x._1._3, '+'), x._2))
      }else if (x._1._4.equals('*') && positiveStrand) {
        //output is Positive strand because the data contains only Positive strand
        List(GArray(new GRegionKey(x._1._1, x._1._2, x._1._3, '+'), x._2))
      } else if (x._1._4.equals('*') && !positiveStrand && doubleStrand) {
        //output is minus strand because the data contains only minus strand
        List(GArray(new GRegionKey(x._1._1, x._1._2, x._1._3, '-'), x._2))
      } else {
        List(GArray(new GRegionKey(x._1._1, x._1._2, x._1._3, x._1._4), x._2))
      }

    }
  }

  def extractor(dataset : RDD[GARRAY], binSize : Long) = {
    dataset.flatMap{x =>
      val startBin =(x._1.start/binSize).toInt
      val stopBin = if ((x._1.stop/binSize).toInt * binSize == x._1.stop) ((x._1.stop/binSize).toInt - 1) else (x._1.stop/binSize).toInt

      val id = Hashing.md5().newHasher().putString(x._1.chrom + x._1.start + x._1.stop + x._1.strand,java.nio.charset.Charset.defaultCharset()).hash().asLong()

      val numReplications = x._2._1.foldLeft(0)((acc, z) => acc + z._2)
      if(startBin==stopBin) {
        List(((x._1.chrom, startBin, x._1.strand), (id, x._1.start, x._1.stop, x._2, numReplications) ))
      }
      else{
        val map_start = ((x._1.chrom, startBin, x._1.strand), (id, x._1.start, startBin * binSize + binSize, x._2, numReplications))
        val map_stop   = ((x._1.chrom, stopBin, x._1.strand), (id, stopBin * binSize, x._1.stop, x._2, numReplications))
        val map_int = for (i <- startBin+1 to stopBin-1) yield ((x._1.chrom, i, x._1.strand), (id, i * binSize, i * binSize + binSize, x._2, numReplications))

        List(map_start, map_stop) ++ map_int
      }
    }
  }

  def extractor2(dataset : RDD[GArray], binSize : Long) = {
    dataset.flatMap{x =>
      val startBin =(x._1.start/binSize).toInt
      val stopBin = if ((x._1.stop/binSize).toInt * binSize == x._1.stop) ((x._1.stop/binSize).toInt - 1) else (x._1.stop/binSize).toInt

      val id = Hashing.md5().newHasher().putString(x._1.chrom + x._1.start + x._1.stop + x._1.strand,java.nio.charset.Charset.defaultCharset()).hash().asLong()

      val numReplications = x._2._1.foldLeft(0)((acc, z) => acc + z._2)
      if(startBin==stopBin) {
        List(((x._1.chrom, startBin, x._1.strand), (id, x._1.start, x._1.stop, x._2, numReplications) ))
      }
      else{
        val map_start = ((x._1.chrom, startBin, x._1.strand), (id, x._1.start, startBin * binSize + binSize, x._2, numReplications))
        val map_stop   = ((x._1.chrom, stopBin, x._1.strand), (id, stopBin * binSize, x._1.stop, x._2, numReplications))
        val map_int = for (i <- startBin+1 to stopBin-1) yield ((x._1.chrom, i, x._1.strand), (id, i * binSize, i * binSize + binSize, x._2, numReplications))

        List(map_start, map_stop) ++ map_int
      }
    }
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

  def assignGroups2(dataset: RDD[GArray]): RDD[GArray] = {
    val containsStrandUnknown = dataset.map(x => x._1._4 == '*').fold(false)(_ || _)
    dataset.map { x =>
      val strand =
        if (containsStrandUnknown)
          '*'
        else
          x._1._4
      GArray(new GRegionKey(x._1.chrom, x._1.start, x._1.stop, strand), x._2)
    }
  }


}

