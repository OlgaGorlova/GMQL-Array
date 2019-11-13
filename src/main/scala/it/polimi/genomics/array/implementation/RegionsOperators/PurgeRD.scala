package it.polimi.genomics.array.implementation.RegionsOperators

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.GArray
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object PurgeRD {

  private final val logger = LoggerFactory.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor: GMQLArrayExecutor, metaDataset: MetaOperator, inputDataset: RegionOperator, sc: SparkContext): RDD[GARRAY] = {
    logger.info("----------------PurgeRD executing..")
    val metaIdsList = executor.implement_md(metaDataset, sc).keys.distinct.collect
    executor.implement_rd(inputDataset, sc)/*.filter((a: GArray) => metaIdsList.contains(a._1._1))*/
  }

}
