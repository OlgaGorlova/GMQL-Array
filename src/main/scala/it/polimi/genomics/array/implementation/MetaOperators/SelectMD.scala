package it.polimi.genomics.array.implementation.MetaOperators

import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta.MetaSelection
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object SelectMD {

  private final val logger = Logger.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor : GMQLArrayExecutor,
            metaCondition: MetadataCondition,
            inputDataset: MetaOperator,
            sc : SparkContext) : RDD[MetaType] = {

    logger.info("----------------SELECTMD executing..")

    val input = executor.implement_md(inputDataset, sc).cache()

    input
      .groupByKey()
      .filter(x => metaSelection.build_set_filter(metaCondition)(x._2))
      .flatMap(x=> for(p <- x._2) yield (x._1, p))
      .cache()

  }
  object metaSelection extends MetaSelection


}
