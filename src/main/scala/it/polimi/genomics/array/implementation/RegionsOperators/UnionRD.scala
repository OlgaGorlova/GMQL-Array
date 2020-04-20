package it.polimi.genomics.array.implementation.RegionsOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GArray, GAttributes}
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.{GNull, GValue}
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object UnionRD {

  private final val logger = LoggerFactory.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor: GMQLArrayExecutor, schemaReformatting: List[Int], leftDataset: RegionOperator, rightDataset: RegionOperator, sc: SparkContext):RDD[GARRAY] = {
    logger.info("----------------UnionRD executing..")

    val left = executor.implement_rd(leftDataset, sc)
    val right = executor.implement_rd(rightDataset, sc)

    execute(schemaReformatting, left, right, sc)
  }

  def apply(schemaReformatting: List[Int], leftDataset: RDD[GARRAY], rightDataset: RDD[GARRAY], sc: SparkContext):RDD[GARRAY] = {
    logger.info("----------------UnionRD executing..")

    execute(schemaReformatting, leftDataset, rightDataset, sc)
  }

  def apply(leftDataset: RDD[GARRAY], rightDataset: RDD[GARRAY], sc: SparkContext):RDD[GARRAY] = {
    logger.info("----------------UnionRD executing..")

    execute(leftDataset, rightDataset, sc)
  }


  def execute(schemaReformatting: List[Int], left: RDD[GARRAY], right: RDD[GARRAY], sc: SparkContext):RDD[GARRAY] = {

    val union = left.cogroup(right).mapValues{r=>
      if (r._1.nonEmpty && r._2.nonEmpty) {
        val left = r._1.head
        val right = r._2.head

        val attSchema = right._2.last.map(_.length)
        val valuesRight = schemaReformatting.foldLeft(new Array[Array[Array[GValue]]](0))((z, a) => {
          if (a.equals(-1)) {
            val nullAtt: Array[Array[GValue]] = attSchema.map(Array.fill[GValue](_)(GNull()))
            z :+ nullAtt
          } else {
            z :+ right._2(a)
          }
        })

        val leftIds = left._1.map(id => (Hashing.md5.newHasher.putLong(0L).putLong(id._1).hash.asLong, id._2))
        val rightIds = right._1.map(id => (Hashing.md5.newHasher.putLong(1L).putLong(id._1).hash.asLong, id._2))
        val ids: Array[(Long, Int)] = leftIds ++ rightIds
        val values: Array[Array[Array[GValue]]] = left._2.zip(valuesRight).map { v => v._1.union(v._2) }

        new GAttributes(ids, values)
      } else if (r._1.isEmpty) {
        val rightIds = r._2.head._1.map(id => (Hashing.md5.newHasher.putLong(1L).putLong(id._1).hash.asLong, id._2))
        new GAttributes(rightIds, r._2.head._2)
      }
      else {
        val leftIds = r._1.head._1.map(id => (Hashing.md5.newHasher.putLong(0L).putLong(id._1).hash.asLong, id._2))
        new GAttributes(leftIds, r._1.head._2)
      }

    }

    union
  }


  def execute2(schemaReformatting: List[Int], left: RDD[GArray], right: RDD[GArray], sc: SparkContext):RDD[GArray] = {

    val leftDS = left.map(l=> (l._1,l._2))
    val rightDS = right.map(r=> (r._1, r._2))

    val union = leftDS.cogroup(rightDS).map{r=>
      if (r._2._1.nonEmpty && r._2._2.nonEmpty) {
        val lft = r._2._1.head
        val rgt = r._2._2.head

        val attSchema = rgt._2.last.map(_.length)
        val valuesRight = schemaReformatting.foldLeft(new Array[Array[Array[GValue]]](0))((z, a) => {
          if (a.equals(-1)) {
            val nullAtt: Array[Array[GValue]] = attSchema.map(Array.fill[GValue](_)(GNull()))
            z :+ nullAtt
          } else {
            z :+ rgt._2(a)
          }
        })

        val leftIds = lft._1.map(id => (Hashing.md5.newHasher.putLong(0L).putLong(id._1).hash.asLong, id._2))
        val rightIds = rgt._1.map(id => (Hashing.md5.newHasher.putLong(1L).putLong(id._1).hash.asLong, id._2))
        val ids: Array[(Long, Int)] = leftIds ++ rightIds
        val values: Array[Array[Array[GValue]]] = lft._2.zip(valuesRight).map { v => v._1.union(v._2) }

        GArray(r._1, new GAttributes(ids, values))
      } else if (r._2._1.isEmpty) {
        val rightIds = r._2._2.head._1.map(id => (Hashing.md5.newHasher.putLong(1L).putLong(id._1).hash.asLong, id._2))
        GArray(r._1, new GAttributes(rightIds, r._2._2.head._2))
      }
      else {
        val leftIds = r._2._1.head._1.map(id => (Hashing.md5.newHasher.putLong(0L).putLong(id._1).hash.asLong, id._2))
        GArray(r._1, new GAttributes(leftIds, r._2._1.head._2))
      }

    }

    union
  }

  def execute(left: RDD[GARRAY], right: RDD[GARRAY], sc: SparkContext):RDD[GARRAY] = {

    val union = left.cogroup(right).mapValues{r=>
      if (r._1.nonEmpty && r._2.nonEmpty) {
        val left = r._1.head
        val right = r._2.head

        val leftIds = left._1.map(id => (Hashing.md5.newHasher.putLong(0L).putLong(id._1).hash.asLong, id._2))
        val rightIds = right._1.map(id => (Hashing.md5.newHasher.putLong(1L).putLong(id._1).hash.asLong, id._2))
        val ids: Array[(Long, Int)] = leftIds ++ rightIds
        val values: Array[Array[Array[GValue]]] = left._2.zip(right._2).map { v => v._1.union(v._2) }

        new GAttributes(ids, values)
      } else if (r._1.isEmpty) {
        val rightIds = r._2.head._1.map(id => (Hashing.md5.newHasher.putLong(1L).putLong(id._1).hash.asLong, id._2))
        new GAttributes(rightIds, r._2.head._2)
      }
      else {
        val leftIds = r._1.head._1.map(id => (Hashing.md5.newHasher.putLong(0L).putLong(id._1).hash.asLong, id._2))
        new GAttributes(leftIds, r._1.head._2)
      }

    }

    union
  }

}

