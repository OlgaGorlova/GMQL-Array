package it.polimi.genomics.array.implementation.MetaOperators

import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object StoreMD {

  private final val logger = Logger.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor : GMQLArrayExecutor, path: String, value: MetaOperator, sc : SparkContext): RDD[MetaType] = {
    logger.info("----------------STOREMD executing..")
    executor.implement_md(value, sc)
  }

}
