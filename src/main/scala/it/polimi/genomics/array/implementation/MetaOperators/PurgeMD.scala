package it.polimi.genomics.array.implementation.MetaOperators

import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object PurgeMD {
  private final val logger = Logger.getLogger(this.getClass);


  @throws[SelectFormatException]
  def apply(executor : GMQLArrayExecutor, regionDataset : RegionOperator, inputDataset : MetaOperator, sc : SparkContext) : RDD[MetaType] = {
    logger.info("----------------PurgeMD executing..")

    val input = executor.implement_md(inputDataset, sc)
    val metaIdList = executor.implement_rd(regionDataset, sc).keys.map(x=>x._1).distinct.collect
    input/*.filter((a : MetaType) => metaIdList.contains(a._1))*/

  }
}
