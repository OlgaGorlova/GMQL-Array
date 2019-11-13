package it.polimi.genomics.array.implementation.MetaOperators

import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object UnionAggMD {

  private final val logger = Logger.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor : GMQLArrayExecutor, leftDataset : MetaOperator, rightDataset : MetaOperator,leftTag: String = "left", rightTag :String = "right", sc : SparkContext) = {

    logger.info("----------------UnionMD executing..")

    //create the datasets
    val left: RDD[(Long, (String, String))] =
      executor.implement_md(leftDataset, sc)

    val right: RDD[(Long, (String, String))] =
      executor.implement_md(rightDataset, sc)

    //change ID of each region according to previous computation
    val leftMod  = left

    val rightMod  = right
    //merge datasets
    (leftMod.union(rightMod)).distinct()
  }
}
