package it.polimi.genomics.array.implementation.RegionsOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GArray, GAttributes, GRegionKey}
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.RegionOperator
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  * Difference operator returns all the regions in the first dataset that do not overlap any region in the second dataset.
  * This version takes as input 2 arrays (ref and exp).
  *
  * Limitations:
  * - joinby clause
  *
  * TODO: add 'exact' option
  */
object GenometricDifference {
  private final val BINNING_PARAMETER = 50000
  private final val logger = Logger.getLogger(this.getClass);

  def apply(executor: GMQLArrayExecutor, left:RegionOperator, right: RegionOperator, BINNING_PARAMETER:Long, exact:Boolean, sc : SparkContext) : RDD[(GRegionKey, GAttributes)] = {

    logger.info("----------------GenometricDifference executing...")

    val ref = executor.implement_rd(left, sc)
    val exp = executor.implement_rd(right, sc)

    execute(ref, exp, BINNING_PARAMETER, exact, sc)
  }

  def apply(left:RDD[GArray], right: RDD[GArray], BINNING_PARAMETER:Long, exact:Boolean, sc : SparkContext) : RDD[GArray] = {

    logger.info("----------------GenometricDifference executing...")

    execute2(left,right,BINNING_PARAMETER,exact,sc)
  }

  def execute(ref:RDD[GARRAY], exp: RDD[GARRAY], BINNING_PARAMETER:Long, exact:Boolean, sc : SparkContext) : RDD[(GRegionKey, GAttributes)] = {

    val expIds: Broadcast[Array[Long]] = sc.broadcast(exp.flatMap(_._2._1.map(_._1)).distinct().collect())
    val refKeys = ref.map(_._1)

    val expBinned: RDD[((String, Int), (Long, Long, Char, GAttributes))] = binExpDS(exp, BINNING_PARAMETER)
    val refBinnedRep: RDD[((String, Int), (Long, Long, Char))] = binRefDS(refKeys, BINNING_PARAMETER).distinct()

    val coGroupResult: RDD[(GRegionKey, Int)] =
      refBinnedRep // bin, chromosome
        .cogroup(expBinned).flatMap { x =>
        val ref = x._2._1;
        val exp = x._2._2;
        val key = x._1
        ref.map { region =>
          var count = 0
          val aggregation = Hashing.md5().newHasher().putString(key._1 + region._2 + region._3, java.nio.charset.Charset.defaultCharset()).hash().asLong()
          for (experiment <- exp)
            if ( /* cross */
            /* space overlapping */
              (region._1 < experiment._2 && experiment._1 < region._2)
                && /* same strand */
                (region._3.equals('*') || experiment._3.equals('*') || region._3.equals(experiment._3))
                && /* first comparison */
                ((region._1 / BINNING_PARAMETER).toInt.equals(key._2) || (experiment._1 / BINNING_PARAMETER).toInt.equals(key._2))
            ) {
              if(!exact || (region._1 == experiment._1 && region._2 == experiment._2))
                count = count + 1;
            }

          (new GRegionKey(key._1, region._1, region._2, region._3), count)
        }
      }

    val filteredReduceResult =
    // group regions by aggId (same region)
      coGroupResult
        // reduce phase -> sum the count value of left and right
        .reduceByKey((r1, r2) =>
        (r1 + r2)
      )
        // filter only region with count = 0
        .flatMap(r =>
        if (r._2.equals(0))
          Some((r._1, r._2))
        else
          None
      )

    val joined = filteredReduceResult.join(ref).map{ x=>
      val newIds = x._2._2._1.map(id => (Hashing.md5().newHasher().putLong(id._1).hash().asLong(), id._2))
      (x._1, new GAttributes(newIds, x._2._2._2))
    }

    joined
  }

  def execute2(ref:RDD[GArray], exp: RDD[GArray], BINNING_PARAMETER:Long, exact:Boolean, sc : SparkContext) : RDD[GArray] = {

    val expIds: Broadcast[Array[Long]] = sc.broadcast(exp.flatMap(_._2._1.map(_._1)).distinct().collect())
//    val refKeys = ref.map(_._1)

    val expBinned: RDD[((String, Int), (Long, Long, Char, GAttributes))] = binExpDS2(exp, BINNING_PARAMETER)
    val refBinnedRep: RDD[((String, Int), (Long, Long, Char, GAttributes))] = binExpDS2(ref, BINNING_PARAMETER).distinct()

    val coGroupResult: RDD[(GArray, Int)] =
      refBinnedRep // bin, chromosome
        .cogroup(expBinned).flatMap { x =>
        val ref = x._2._1;
        val exp = x._2._2;
        val key = x._1
        ref.map { region =>
          var count = 0
          val aggregation = Hashing.md5().newHasher().putString(key._1 + region._2 + region._3, java.nio.charset.Charset.defaultCharset()).hash().asLong()
          for (experiment <- exp)
            if ( /* cross */
            /* space overlapping */
              (region._1 < experiment._2 && experiment._1 < region._2)
                && /* same strand */
                (region._3.equals('*') || experiment._3.equals('*') || region._3.equals(experiment._3))
                && /* first comparison */
                ((region._1 / BINNING_PARAMETER).toInt.equals(key._2) || (experiment._1 / BINNING_PARAMETER).toInt.equals(key._2))
            ) {
              if(!exact || (region._1 == experiment._1 && region._2 == experiment._2))
                count = count + 1;
            }

          (GArray(new GRegionKey(key._1, region._1, region._2, region._3),region._4), count)
        }
      }

    import com.softwaremill.quicklens._

    val filteredReduceResult =
    // group regions by aggId (same region)
      coGroupResult
        // reduce phase -> sum the count value of left and right
        .reduceByKey((r1, r2) =>
        (r1 + r2)
      )
        // filter only region with count = 0
        .flatMap(r =>
        if (r._2.equals(0)){
          val newIds = r._1._2._1.map(id => (Hashing.md5().newHasher().putLong(id._1).hash().asLong(), id._2))
          Some(r._1.modify(_.values.samples).setTo(newIds))
        }

        else
          None
      )

    filteredReduceResult
  }


  def binRefDS(rdd: RDD[(GRegionKey)], bin: Long): RDD[((String, Int), (Long, Long, Char))] =
    rdd.flatMap { x =>
      if (bin > 0) {
        val startbin = (x._2 / bin).toInt
        val stopbin = (x._3 / bin).toInt
        for (i <- startbin to stopbin)
          yield ((x._1, i), (x._2, x._3, x._4))
      } else
        Some((x._1, 0), (x._2, x._3, x._4))
    }

  def binExpDS(rdd: RDD[(GRegionKey, GAttributes)], bin: Long): RDD[((String, Int), (Long, Long, Char, GAttributes))] =
    rdd.flatMap { x =>
      if (bin > 0) {
        val startbin = (x._1._2 / bin).toInt
        val stopbin = (x._1._3 / bin).toInt
        for (i <- startbin to stopbin)
          yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, x._2))
      } else
        Some((x._1._1, 0), (x._1._2, x._1._3, x._1._4, x._2))
    }

  def binExpDS2(rdd: RDD[GArray], bin: Long): RDD[((String, Int), (Long, Long, Char, GAttributes))] =
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

