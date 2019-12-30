package it.polimi.genomics.array.implementation.RegionsOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GArray, GAttributes, GRegionKey}
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.JoinParametersRD.{DistLess, RegionBuilder}
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.GValue
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  *
  * Limitations:
  * - distance based join
  * - joinby clause
  *
  * TODO:
  * - MINDISTANCE, distance less/greater than
  */
object GenometricJoin {


  private final val logger = Logger.getLogger(this.getClass);
  private final type groupType = Array[((Long, String), Array[Long])]

  def apply(executor: GMQLArrayExecutor, left: RegionOperator, right:  RegionOperator, regionBuilder : RegionBuilder, less: Option[DistLess], BINNING_PARAMETER:Long, sc : SparkContext) :  RDD[(GRegionKey, GAttributes)] = {

    logger.info(s"----------------GenometricJoin: $regionBuilder executing...")

    val ref = executor.implement_rd(left, sc)
    val exp = executor.implement_rd(right, sc)

    execute(ref, exp, regionBuilder, less, BINNING_PARAMETER, sc)
  }

  def apply(left: RDD[GARRAY], right:  RDD[GARRAY], regionBuilder : RegionBuilder, less: Option[DistLess], BINNING_PARAMETER:Long, sc : SparkContext) :  RDD[GARRAY] = {

    logger.info(s"----------------GenometricJoin: $regionBuilder executing...")

    execute(left,right,regionBuilder,less,BINNING_PARAMETER,sc)
  }

  def execute(ref: RDD[GARRAY], exp:  RDD[GARRAY], regionBuilder : RegionBuilder, less: Option[DistLess], BINNING_PARAMETER:Long, sc : SparkContext) :  RDD[(GRegionKey, GAttributes)] = {

    val distLess = if (less.isDefined) less.get.limit else 0

    val expBinned: RDD[((String, Int), (Long, Long, Char, GAttributes))] = exp.binExpDS(BINNING_PARAMETER,distLess)

    val refBinnedRep: RDD[((String, Int), (Long, Long, Char, GAttributes))] = ref.binExpDS(BINNING_PARAMETER,distLess)

    val RefExpJoined: RDD[(GRegionKey, GAttributes)] = refBinnedRep.cogroup(expBinned)
      .flatMap { grouped: ((String, Int), (Iterable[(Long, Long, Char, GAttributes)], Iterable[(Long, Long, Char, GAttributes)])) =>
        val key: (String, Int) = grouped._1;
        val ref: Iterable[(Long, Long, Char, GAttributes)] = grouped._2._1
        val exp: Iterable[(Long, Long, Char, GAttributes)] = grouped._2._2

        ref.flatMap { refRecord =>
          val refInStartBin = (refRecord._1 / BINNING_PARAMETER).toInt.equals(key._2)

          exp.flatMap { expRecord =>
            if ( /* space overlapping */
              (refRecord._1 < expRecord._2 && expRecord._1 < refRecord._2)
                && /* same strand */
                (refRecord._3.equals('*') || expRecord._3.equals('*') || refRecord._3.equals(expRecord._3)) &&
                /* first comparison (start bin of either the ref or exp)*/
                (refInStartBin || (expRecord._1 / BINNING_PARAMETER).toInt.equals(key._2))

            ) {

              val newIds = refRecord._4._1.flatMap { rId =>
                expRecord._4._1.map ( eId => Hashing.md5().newHasher().putLong(rId._1).putLong(eId._1).hash().asLong())
              }
              val size = refRecord._4._1.flatMap { id => expRecord._4._2.last.map(y => y.length * id._2)}

              val attLeft = refRecord._4._2.map{a=> a.flatMap(Array.fill(newIds.length/a.length)(_)).zip(size).map{v=> v._1.flatMap(Array.fill(v._2 / v._1.length)(_))}}
              val attRight = expRecord._4._2.map{a=>
                val ss: Array[Array[GValue]] = Array.fill(newIds.length/a.length)((a)).flatten.zip(size).map{ v=> v._1.flatMap(Array.fill(v._2 / v._1.length)(_))}
                Array.fill(newIds.length/a.length)((a)).flatten.zip(size).map{v=> v._1.flatMap(Array.fill(v._2 / v._1.length)(_))}
              }

              Some(new GRegionKey(key._1, refRecord._1, refRecord._2, refRecord._3), new GAttributes(newIds.zip(size), attLeft++attRight))

              regionBuilder match {
                case RegionBuilder.LEFT => {
                  Some(new GRegionKey(key._1, refRecord._1, refRecord._2, refRecord._3), new GAttributes(newIds.zip(size), attLeft++attRight))
                }
                case RegionBuilder.RIGHT => {
                  Some( new GRegionKey(key._1, expRecord._1, expRecord._2, expRecord._3), new GAttributes(newIds.zip(size), attLeft++attRight))
                }
                case RegionBuilder.CONTIG => {
                  val start: Long = Math.min(refRecord._1, expRecord._1)
                  val stop: Long = Math.max(refRecord._2, expRecord._2)
                  val strand: Char = if (refRecord._3.equals(expRecord._3)) refRecord._3 else '*'
                  Some(new GRegionKey(key._1, start, stop, strand), new GAttributes(newIds.zip(size), attLeft++attRight))
                }
                case RegionBuilder.INTERSECTION => {
                  val start: Long = Math.max(refRecord._1, expRecord._1)
                  val stop: Long = Math.min(refRecord._2, expRecord._2)
                  val strand: Char = if (refRecord._3.equals(expRecord._3)) refRecord._3 else '*'

                  Some( new GRegionKey(key._1, start, stop, strand), new GAttributes(newIds.zip(size), attLeft++attRight))
                }
              }

            } else {
              None
            }
          }
        }

      }

    val res = RefExpJoined.reduceByKey{(l,r) =>
      val indexesofL = l._1.map(_._1)
      val indexesofR = r._1.map(_._1)
      val diffL = indexesofL diff indexesofR
      val diffR = indexesofR diff indexesofL
      val intersect = indexesofL intersect indexesofR

      val newVal = {
        l._2.zip(r._2).zipWithIndex.map { v=>
          (((intersect.map { i =>
            (v._1._1(indexesofL.indexOf(i)) ++ v._1._2(indexesofR.indexOf(i)), indexesofL.indexOf(i))
          } ++ diffL.map { i =>
            (v._1._1(indexesofL.indexOf(i)), indexesofL.indexOf(i))
          }).sortBy(_._2) ++ diffR.map { i =>
            (v._1._2(indexesofR.indexOf(i)), indexesofR.indexOf(i))
          }.sortBy(_._2)).map(_._1))
        }
      }.toArray

      val newIds = {
        ((intersect.map { i =>
          (i, l._1(indexesofL.indexOf(i))._2 + r._1(indexesofR.indexOf(i))._2, indexesofL.indexOf(i))
        } ++ diffL.map { i =>
          (i, l._1(indexesofL.indexOf(i))._2, indexesofL.indexOf(i))
        }).sortBy(_._3) ++ diffR.map { i =>
          (i, r._1(indexesofR.indexOf(i))._2, indexesofR.indexOf(i))
        }.sortBy(_._3)).map(i => (i._1, i._2))
      }

      //      newAtt
      new GAttributes(newIds, newVal)
    }

    res
  }

  def execute2(ref: RDD[GArray], exp:  RDD[GArray], regionBuilder : RegionBuilder, less: Option[DistLess], BINNING_PARAMETER:Long, sc : SparkContext) :  RDD[GArray] = {

    val distLess = if (less.isDefined) less.get.limit else 0

    val expBinned: RDD[((String, Int), (Long, Long, Char, GAttributes))] = exp.binExpDS(BINNING_PARAMETER,distLess)

    val refBinnedRep: RDD[((String, Int), (Long, Long, Char, GAttributes))] = ref.binExpDS(BINNING_PARAMETER,distLess)

    val RefExpJoined: RDD[(GRegionKey, GAttributes)] = refBinnedRep.cogroup(expBinned)
      .flatMap { grouped: ((String, Int), (Iterable[(Long, Long, Char, GAttributes)], Iterable[(Long, Long, Char, GAttributes)])) =>
        val key: (String, Int) = grouped._1;
        val ref: Iterable[(Long, Long, Char, GAttributes)] = grouped._2._1
        val exp: Iterable[(Long, Long, Char, GAttributes)] = grouped._2._2

        ref.flatMap { refRecord =>
          val refInStartBin = (refRecord._1 / BINNING_PARAMETER).toInt.equals(key._2)

          exp.flatMap { expRecord =>
            if ( /* space overlapping */
              (refRecord._1 < expRecord._2 && expRecord._1 < refRecord._2)
                && /* same strand */
                (refRecord._3.equals('*') || expRecord._3.equals('*') || refRecord._3.equals(expRecord._3)) &&
                /* first comparison (start bin of either the ref or exp)*/
                (refInStartBin || (expRecord._1 / BINNING_PARAMETER).toInt.equals(key._2))

            ) {

              val newIds = refRecord._4._1.flatMap { rId =>
                expRecord._4._1.map ( eId => Hashing.md5().newHasher().putLong(rId._1).putLong(eId._1).hash().asLong())
              }
              val size = refRecord._4._1.flatMap { id => expRecord._4._2.last.map(y => y.length * id._2)}

              val attLeft = refRecord._4._2.map{a=> a.flatMap(Array.fill(newIds.length/a.length)(_)).zip(size).map{v=> v._1.flatMap(Array.fill(v._2 / v._1.length)(_))}}
              val attRight = expRecord._4._2.map{a=>
                val ss: Array[Array[GValue]] = Array.fill(newIds.length/a.length)((a)).flatten.zip(size).map{ v=> v._1.flatMap(Array.fill(v._2 / v._1.length)(_))}
                Array.fill(newIds.length/a.length)((a)).flatten.zip(size).map{v=> v._1.flatMap(Array.fill(v._2 / v._1.length)(_))}
              }

              Some(new GRegionKey(key._1, refRecord._1, refRecord._2, refRecord._3), new GAttributes(newIds.zip(size), attLeft++attRight))

              regionBuilder match {
                case RegionBuilder.LEFT => {
                  Some(new GRegionKey(key._1, refRecord._1, refRecord._2, refRecord._3), new GAttributes(newIds.zip(size), attLeft++attRight))
                }
                case RegionBuilder.RIGHT => {
                  Some( new GRegionKey(key._1, expRecord._1, expRecord._2, expRecord._3), new GAttributes(newIds.zip(size), attLeft++attRight))
                }
                case RegionBuilder.CONTIG => {
                  val start: Long = Math.min(refRecord._1, expRecord._1)
                  val stop: Long = Math.max(refRecord._2, expRecord._2)
                  val strand: Char = if (refRecord._3.equals(expRecord._3)) refRecord._3 else '*'
                  Some(new GRegionKey(key._1, start, stop, strand), new GAttributes(newIds.zip(size), attLeft++attRight))
                }
                case RegionBuilder.INTERSECTION => {
                  val start: Long = Math.max(refRecord._1, expRecord._1)
                  val stop: Long = Math.min(refRecord._2, expRecord._2)
                  val strand: Char = if (refRecord._3.equals(expRecord._3)) refRecord._3 else '*'

                  Some( new GRegionKey(key._1, start, stop, strand), new GAttributes(newIds.zip(size), attLeft++attRight))
                }
              }

            } else {
              None
            }
          }
        }

      }

    val res = RefExpJoined.reduceByKey{(l,r) =>
      val indexesofL = l._1.map(_._1)
      val indexesofR = r._1.map(_._1)
      val diffL = indexesofL diff indexesofR
      val diffR = indexesofR diff indexesofL
      val intersect = indexesofL intersect indexesofR

      val newVal = {
        l._2.zip(r._2).zipWithIndex.map { v=>
          (((intersect.map { i =>
            (v._1._1(indexesofL.indexOf(i)) ++ v._1._2(indexesofR.indexOf(i)), indexesofL.indexOf(i))
          } ++ diffL.map { i =>
            (v._1._1(indexesofL.indexOf(i)), indexesofL.indexOf(i))
          }).sortBy(_._2) ++ diffR.map { i =>
            (v._1._2(indexesofR.indexOf(i)), indexesofR.indexOf(i))
          }.sortBy(_._2)).map(_._1))
        }
      }.toArray

      val newIds = {
        ((intersect.map { i =>
          (i, l._1(indexesofL.indexOf(i))._2 + r._1(indexesofR.indexOf(i))._2, indexesofL.indexOf(i))
        } ++ diffL.map { i =>
          (i, l._1(indexesofL.indexOf(i))._2, indexesofL.indexOf(i))
        }).sortBy(_._3) ++ diffR.map { i =>
          (i, r._1(indexesofR.indexOf(i))._2, indexesofR.indexOf(i))
        }.sortBy(_._3)).map(i => (i._1, i._2))
      }

      //      newAtt
      new GAttributes(newIds, newVal)
    }.map(r=> GArray(r._1, r._2))

    res
  }

  implicit class BinningExp(rdd: RDD[(GRegionKey, GAttributes)]) {
    def binExpDS(bin: Long, less: Long): RDD[((String, Int), (Long, Long, Char, GAttributes))] =
      rdd.flatMap { x =>
        if (bin > 0) {
          val startbin = ((x._1._2 - less).max(0) / bin).toInt
          val stopbin = ((x._1._3 + less).max(0) / bin).toInt
          for (i <- startbin to stopbin)
            yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, x._2))
        } else
          Some((x._1._1, 0), (x._1._2, x._1._3, x._1._4, x._2))
      }
  }

  implicit class BinningExp2(rdd: RDD[GArray]) {
    def binExpDS(bin: Long, less: Long): RDD[((String, Int), (Long, Long, Char, GAttributes))] =
      rdd.flatMap { x =>
        if (bin > 0) {
          val startbin = ((x._1._2 - less).max(0) / bin).toInt
          val stopbin = ((x._1._3 + less).max(0) / bin).toInt
          for (i <- startbin to stopbin)
            yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, x._2))
        } else
          Some((x._1._1, 0), (x._1._2, x._1._3, x._1._4, x._2))
      }
  }


}
