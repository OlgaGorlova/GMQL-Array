package it.polimi.genomics.array.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by Olga Gorlova on 29/10/2019.
  */
object CountRD {

  private final val logger = LoggerFactory.getLogger(CountRD.getClass);
  private final val ENCODING = "UTF-8"

  @throws[SelectFormatException]
  def apply(executor: GMQLArrayExecutor, path: String, value: RegionOperator, sc: SparkContext): RDD[GARRAY] = {
    val input = executor.implement_rd(value, sc)
    logger.info("COUNT: ", input.count())
    input
  }

}
