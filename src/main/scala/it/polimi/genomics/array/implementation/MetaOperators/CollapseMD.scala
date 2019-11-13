package it.polimi.genomics.array.implementation.MetaOperators

import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.{MetaGroupOperator, MetaOperator}
import it.polimi.genomics.core.DataTypes.MetaType
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object CollapseMD {

  private final val logger = Logger.getLogger(this.getClass);

  def apply(executor : GMQLArrayExecutor, grouping : Option[MetaGroupOperator], inputDataset : MetaOperator, sc : SparkContext) : RDD[MetaType] = {

    logger.info("----------------CollapseMD executing..")

    val input = executor.implement_md(inputDataset, sc)
//    val out = if(grouping.isDefined){
//      val groups = executor.implement_mgd(grouping.get, sc)
//      input.join(groups).map{x=> val meta =x._2._1; val group =x._2._2; (group, meta)}
//    } else input.map((meta) => (0L, meta._2))
//
//    out.distinct()

    input

  }

}

