package it.polimi.genomics.array.implementation.RegionsOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GArray, GAttributes, GRegionKey}
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.{GDouble, GValue}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  *
  * This version takes as input 2 arrays (ref and exp).
  *
  * Limitations:
  * - joinby
  *
  * TODO:
  * - add aggregations
  * - remove splitting regions and values
  *
  */
object GenometricMap {
  private final val logger = Logger.getLogger(this.getClass);
  private final type groupType = Array[((Long, String), Array[Long])]

  def apply(executor: GMQLArrayExecutor, left: RegionOperator, right: RegionOperator, BINNING_PARAMETER: Long, sc: SparkContext): RDD[(GRegionKey, GAttributes)] = {

    logger.info("----------------GenometricMap executing...")

    val ref: RDD[(GRegionKey, GAttributes)] = executor.implement_rd(left, sc)
    val exp: RDD[(GRegionKey, GAttributes)] = executor.implement_rd(right, sc)

    execute(ref, exp, BINNING_PARAMETER, sc)

  }

  def apply(left: RDD[GARRAY], right: RDD[GARRAY], BINNING_PARAMETER: Long, sc: SparkContext): RDD[GARRAY] = {

    logger.info("----------------GenometricMap executing...")

    execute(left, right, BINNING_PARAMETER, sc)
  }

  def execute(ref: RDD[GARRAY], exp: RDD[GARRAY], BINNING_PARAMETER: Long, sc: SparkContext): RDD[(GRegionKey, GAttributes)] = {

    val allExpIds: Broadcast[Array[Long]] = sc.broadcast(exp.flatMap(_._2._1.map(_._1)).distinct().collect())

    val expBinned: RDD[((String, Int), (Long, Long, Char, GAttributes))] = exp.binExpDS(BINNING_PARAMETER)
    val refKeys = ref.map(_._1)

    val refBinnedRep: RDD[((String, Int), (Long, Long, Char))] = refKeys.binDS(BINNING_PARAMETER)

//    implicit def orderGrecord: Ordering[((String, Int), (Long, Long, Char))] = Ordering.by{s => val e = s._1;(e._1,e._2,e._3,e._4)}

    val RefExpJoined: RDD[(GRegionKey, (GAttributes, Array[Int]))] = refBinnedRep.repartition(refBinnedRep.getNumPartitions * expBinned.getNumPartitions).cogroup(expBinned)
      .flatMap { grouped: ((String, Int), (Iterable[(Long, Long, Char)], Iterable[(Long, Long, Char, GAttributes)])) =>
        val key: (String, Int) = grouped._1;
        val ref: Iterable[(Long, Long, Char)] = grouped._2._1
        val exp: Iterable[(Long, Long, Char, GAttributes)] = grouped._2._2
        ref.flatMap { refRecord =>
          val refInStartBin = (refRecord._1 / BINNING_PARAMETER).toInt.equals(key._2)

          if (exp.nonEmpty)
            exp.map { expRecord =>
              if ( /* space overlapping */
                (refRecord._1 < expRecord._2 && expRecord._1 < refRecord._2)
                  && /* same strand */
                  (refRecord._3.equals('*') || expRecord._3.equals('*') || refRecord._3.equals(expRecord._3)) &&
                  /* first comparison (start bin of either the ref or exp)*/
                  (refInStartBin || (expRecord._1 / BINNING_PARAMETER).toInt.equals(key._2))
              ) {
                //              refRecord.count += 1
                val values = Array.fill(expRecord._4._1.length)(1)
                (new GRegionKey(key._1, refRecord._1, refRecord._2, refRecord._3), (expRecord._4, values))
              } else {
                (new GRegionKey(key._1, refRecord._1, refRecord._2, refRecord._3), (new GAttributes(), Array(0)))
              }
            }
          else {
            Array((new GRegionKey(key._1, refRecord._1, refRecord._2, refRecord._3), (new GAttributes(), Array(0))))
          }
        }

      }

    val reduced = RefExpJoined.reduceByKey { (l, r) =>
      val att = new GAttributes(l._1._1 ++ r._1._1, l._1._2 ++ r._1._2)
      val indexesofL = l._1._1.map(_._1)
      val indexesofR = r._1._1.map(_._1)
      val diffL = indexesofL diff indexesofR
      val diffR = indexesofR diff indexesofL
      val intersect = indexesofL intersect indexesofR

      val (newAtt, newCount) =
        if (l._1._1.nonEmpty && r._1._1.nonEmpty) {
          val c = (intersect.map { i =>
            (i, l._2(indexesofL.indexOf(i)) + r._2(indexesofR.indexOf(i)), l._1._1(indexesofL.indexOf(i))._2 + r._1._1(indexesofR.indexOf(i))._2, indexesofL.indexOf(i))
          } ++ diffL.map { i =>
            (i, l._2(indexesofL.indexOf(i)), l._1._1(indexesofL.indexOf(i))._2, indexesofL.indexOf(i))
          }).sortBy(_._3) ++ diffR.map { i =>
            (i, r._2(indexesofR.indexOf(i)), r._1._1(indexesofR.indexOf(i))._2, indexesofR.indexOf(i))
          }.sortBy(_._3)

          val cCount = c.map(i => i._2)
          val cIds = c.map(i => (i._1, i._3))
          (new GAttributes(cIds, l._1._2 ++ r._1._2), cCount)
        }
        else if (l._1._1.isEmpty && r._1._1.nonEmpty)
          (r._1, r._2)
        else if (l._1._1.nonEmpty && r._1._1.isEmpty)
          (l._1, l._2)
        else (l._1, l._2)

      (newAtt, newCount)
    }

    val joined: RDD[(GRegionKey, GAttributes)] = reduced.join(ref).map { x =>
      val newNumIds = x._2._1._1._1.length

      val diff = allExpIds.value diff x._2._1._1._1.map(_._1)

      val newIds = x._2._2._1.flatMap { id =>
        if (newNumIds != 0) {
          val ss: Seq[(Long, Int)] = for (i <- 0 until newNumIds) yield {
            (Hashing.md5().newHasher().putLong(id._1).putLong(x._2._1._1._1(i)._1).hash().asLong(), id._2)
          }
          val rr: Seq[(Long, Int)] = for (i <- 0 until diff.length) yield {
            (Hashing.md5().newHasher().putLong(id._1).putLong(diff(i)).hash().asLong(), 1)
          }
          ss ++ rr
        }
        else
          for (i <- allExpIds.value) yield {
            (Hashing.md5().newHasher().putLong(id._1).putLong(i).hash().asLong(), id._2)
          }
      }

      val newCount: Array[Array[GValue]] = x._2._1._2.map { c => Array[GValue](GDouble(c.toDouble)) } ++
        (for (i <- 0 until (allExpIds.value.length - x._2._1._2.length)) yield {
          Array[GValue](GDouble(0))
        })

      val attLeft = x._2._2._2.map{a=> a.flatMap(Array.fill(newIds.length/a.length)(_)).zip(newIds).map{v=> v._1.flatMap(Array.fill(v._2._2 / v._1.length)(_))}}
      val attRight = Array.fill(newIds.length/newCount.length)((newCount)).flatten.zip(newIds).map{v=> v._1.flatMap(Array.fill(v._2._2 / v._1.length)(_))}
      val newValues: Array[Array[Array[GValue]]] = if (attLeft.nonEmpty) attLeft :+ attRight else Array(attRight)

      (x._1, new GAttributes(newIds, newValues))
    }

    joined

  }

  def execute2(ref: RDD[GArray], exp: RDD[GArray], BINNING_PARAMETER: Long, sc: SparkContext): RDD[GArray] = {

    val allExpIds: Broadcast[Array[Long]] = sc.broadcast(exp.flatMap(_._2._1.map(_._1)).distinct().collect())

    val expBinned: RDD[((String, Int), (Long, Long, Char, GAttributes))] = exp.binExpDS(BINNING_PARAMETER)

    val refBinnedRep: RDD[((String, Int), (Long, Long, Char, GAttributes))] = ref.binExpDS(BINNING_PARAMETER)

    val RefExpJoined: RDD[(GRegionKey, (GAttributes, GAttributes, Array[Int]))] = refBinnedRep.cogroup(expBinned)
      .flatMap { grouped: ((String, Int), (Iterable[(Long, Long, Char, GAttributes)], Iterable[(Long, Long, Char, GAttributes)])) =>
        val key: (String, Int) = grouped._1;
        val ref: Iterable[(Long, Long, Char, GAttributes)] = grouped._2._1
        val exp: Iterable[(Long, Long, Char, GAttributes)] = grouped._2._2
        ref.flatMap { refRecord =>
          val refInStartBin = (refRecord._1 / BINNING_PARAMETER).toInt.equals(key._2)

          if (exp.nonEmpty)
            exp.map { expRecord =>
              if ( /* space overlapping */
                (refRecord._1 < expRecord._2 && expRecord._1 < refRecord._2)
                  && /* same strand */
                  (refRecord._3.equals('*') || expRecord._3.equals('*') || refRecord._3.equals(expRecord._3)) &&
                  /* first comparison (start bin of either the ref or exp)*/
                  (refInStartBin || (expRecord._1 / BINNING_PARAMETER).toInt.equals(key._2))
              ) {
                //              refRecord.count += 1
                val values = Array.fill(expRecord._4._1.length)(1)
                (new GRegionKey(key._1, refRecord._1, refRecord._2, refRecord._3), (refRecord._4, expRecord._4, values))
              } else {
                (new GRegionKey(key._1, refRecord._1, refRecord._2, refRecord._3), (refRecord._4, new GAttributes(), Array(0)))
              }
            }
          else {
            Array((new GRegionKey(key._1, refRecord._1, refRecord._2, refRecord._3), (refRecord._4, new GAttributes(), Array(0))))
          }
        }

      }

    val reduced = RefExpJoined.reduceByKey { (l, r) =>
      val att = new GAttributes(l._2._1 ++ r._2._1, l._2._2 ++ r._2._2)
      val indexesofL = l._2._1.map(_._1)
      val indexesofR = r._2._1.map(_._1)
      val diffL = indexesofL diff indexesofR
      val diffR = indexesofR diff indexesofL
      val intersect = indexesofL intersect indexesofR

      val (newAtt, newCount) =
        if (l._2._1.nonEmpty && r._2._1.nonEmpty) {
          val c = (intersect.map { i =>
            (i, l._3(indexesofL.indexOf(i)) + r._3(indexesofR.indexOf(i)), l._2._1(indexesofL.indexOf(i))._2 + r._2._1(indexesofR.indexOf(i))._2, indexesofL.indexOf(i))
          } ++ diffL.map { i =>
            (i, l._3(indexesofL.indexOf(i)), l._2._1(indexesofL.indexOf(i))._2, indexesofL.indexOf(i))
          }).sortBy(_._3) ++ diffR.map { i =>
            (i, r._3(indexesofR.indexOf(i)), r._2._1(indexesofR.indexOf(i))._2, indexesofR.indexOf(i))
          }.sortBy(_._3)

          val cCount = c.map(i => i._2)
          val cIds = c.map(i => (i._1, i._3))
          (new GAttributes(cIds, l._2._2 ++ r._2._2), cCount)
        }
        else if (l._2._1.isEmpty && r._2._1.nonEmpty)
          (r._2, r._3)
        else if (l._2._1.nonEmpty && r._2._1.isEmpty)
          (l._2, l._3)
        else (l._2, l._3)

      (l._1, newAtt, newCount)
    }

    val res = reduced.map { x =>

      val refVal = x._2._1
      val expVal = x._2._2
      val newNumIds = expVal._1.length

      val diff = allExpIds.value diff expVal._1.map(_._1)

      val newIds = refVal._1.flatMap { id =>
        if (newNumIds != 0) {
          val ss: Seq[(Long, Int)] = for (i <- 0 until newNumIds) yield {
            (Hashing.md5().newHasher().putLong(id._1).putLong(expVal._1(i)._1).hash().asLong(), id._2)
          }
          val rr: Seq[(Long, Int)] = for (i <- 0 until diff.length) yield {
            (Hashing.md5().newHasher().putLong(id._1).putLong(diff(i)).hash().asLong(), 1)
          }
          ss ++ rr
        }
        else
          for (i <- allExpIds.value) yield {
            (Hashing.md5().newHasher().putLong(id._1).putLong(i).hash().asLong(), id._2)
          }
      }

      val newCount: Array[Array[GValue]] = x._2._3.map { c => Array[GValue](GDouble(c.toDouble)) } ++
        (for (i <- 0 until (allExpIds.value.length - x._2._3.length)) yield {
          Array[GValue](GDouble(0))
        })

      val attLeft = refVal._2.map{a=> a.flatMap(Array.fill(newIds.length/a.length)(_)).zip(newIds).map{v=> v._1.flatMap(Array.fill(v._2._2 / v._1.length)(_))}}
      val attRight = Array.fill(newIds.length/newCount.length)((newCount)).flatten.zip(newIds).map{v=> v._1.flatMap(Array.fill(v._2._2 / v._1.length)(_))}
      val newValues: Array[Array[Array[GValue]]] = if (attLeft.nonEmpty) attLeft :+ attRight else Array(attRight)

      GArray(x._1, new GAttributes(newIds, newValues))
    }

    res

  }

  implicit class BinningArr(rdd: RDD[(GRegionKey)]) {
    def binDS(bin: Long): RDD[((String, Int), (Long, Long, Char))] =
      rdd.flatMap { x =>
        if (bin > 0) {
          val startbin = (x._2 / bin).toInt
          val stopbin = (x._3 / bin).toInt
          for (i <- startbin to stopbin)
            yield ((x._1, i), (x._2, x._3, x._4))
        } else
          Some((x._1, 0), (x._2, x._3, x._4))
      }
  }

  implicit class BinningExp(rdd: RDD[(GRegionKey, GAttributes)]) {
    def binExpDS(bin: Long): RDD[((String, Int), (Long, Long, Char, GAttributes))] =
      rdd.flatMap { x =>
        if (bin > 0) {
          val startbin = (x._1._2 / bin).toInt
          val stopbin = (x._1._3 / bin).toInt
          for (i <- startbin to stopbin)
            yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, x._2))
        } else
          Some((x._1._1, 0), (x._1._2, x._1._3, x._1._4, x._2))
      }
  }

  implicit class BinningExp2(rdd: RDD[GArray]) {
    def binExpDS(bin: Long): RDD[((String, Int), (Long, Long, Char, GAttributes))] =
      rdd.flatMap { x =>
        if (bin > 0) {
          val startbin = (x._1._2 / bin).toInt
          val stopbin = (x._1._3 / bin).toInt
          for (i <- startbin to stopbin)
            yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, x._2))
        } else
          Some((x._1._1, 0), (x._1._2, x._1._3, x._1._4, x._2))
      }
  }


}

