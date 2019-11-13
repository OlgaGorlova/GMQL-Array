package it.polimi.genomics.array.implementation.RegionsOperators

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GArray, GAttributes, GRegionKey}
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.array.implementation.RegionsOperators.SelectRegions.PredicateRD
import it.polimi.genomics.core
import it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.REG_OP
import it.polimi.genomics.core.DataStructures.RegionCondition._
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.{GDouble, GNull, GString, GValue}
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  *
  * Limitations:
  * - semijoin
  *
  * TODO: create better selection that has both, meta and regions, and that can recognize when data has to be loaded and when select should be applied.
  */
object SelectRD {

  private final val logger = Logger.getLogger(this.getClass);

  def apply(executor: GMQLArrayExecutor, regionCondition: Option[RegionCondition], filteredMeta: Option[MetaOperator], inputDataset: RegionOperator, sc: SparkContext): RDD[(GRegionKey, GAttributes)] = {

    logger.info("----------------SelectRD executing...")

    PredicateRD.executor = executor

    val optimized_reg_cond = if (regionCondition.isDefined) Some(PredicateRD.optimizeConditionTree(regionCondition.get, false, filteredMeta, sc)) else None


    val input = executor.implement_rd(inputDataset, sc)

    if (filteredMeta.isDefined && regionCondition.isDefined){
      val metaIdList = executor.implement_md(filteredMeta.get, sc).keys.distinct.collect
      input.flatMap { region =>
        val filteredRegion = PredicateRD.applyMetaFilter(metaIdList, region)
        if (filteredRegion.isDefined) {
          PredicateRD.applyRegionCondition(optimized_reg_cond.get, filteredRegion.get)
        }
        else None
      }
    }
    else if (filteredMeta.isDefined){
      val metaIdList = executor.implement_md(filteredMeta.get, sc).keys.distinct.collect
      input.flatMap( region => PredicateRD.applyMetaFilter(metaIdList, region))
    }
    else if (regionCondition.isDefined){
      input.flatMap ( region => PredicateRD.applyRegionCondition(optimized_reg_cond.get, region))
    }
    else input
  }

  def apply(regionCondition: Option[RegionCondition], input: RDD[GArray], sc: SparkContext): RDD[GArray] = {

    if (regionCondition.isDefined){
      input.flatMap { region =>
        val newRegion = PredicateRD.applyRegionCondition(regionCondition.get, (region._1, region._2));
        if (newRegion.isDefined) Some(GArray(newRegion.get._1, newRegion.get._2)) else None
      }
    }
    else input
  }

}
