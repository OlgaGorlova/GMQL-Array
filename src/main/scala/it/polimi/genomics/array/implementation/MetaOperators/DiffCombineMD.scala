package it.polimi.genomics.array.implementation.MetaOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.{MetaOperator, OptionalMetaJoinOperator, SomeMetaJoinOperator}
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object DiffCombineMD {

  private final val logger = Logger.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor : GMQLArrayExecutor, grouping : OptionalMetaJoinOperator, leftDataset : MetaOperator, rightDataset : MetaOperator, leftTag:String = "left", rightTag:String = "right", sc : SparkContext) : RDD[MetaType] = {

    logger.info("----------------CombineMD executing..")

    val left = executor.implement_md(leftDataset, sc)
    val right = executor.implement_md(rightDataset, sc)

    left
//    val ltag = if (!leftTag.isEmpty()){leftTag +"." } else ""
//    val rtag = if (!rightTag.isEmpty()){rightTag +"." } else ""
//
//    if (grouping.isInstanceOf[SomeMetaJoinOperator]) {
//      val pairs = executor.implement_mjd(grouping, sc).collectAsMap()
//
//
//      val leftOut = left.flatMap{ l => val pair = pairs.get(l._1)
//        if(pair.isDefined){
//          Some(Hashing.md5().newHasher().putLong(l._1).hash().asLong, (ltag + l._2._1, l._2._2))/*)*/}
//        else None
//      }
//      leftOut
//
//    } else {
//      val leftIds = left.keys.distinct().collect()
//
//      val leftOut = left.map{l=>
//        (Hashing.md5().newHasher().putLong(l._1).hash.asLong, (ltag + l._2._1,l._2._2))
//      }
//
//      leftOut
//    }
  }


}

