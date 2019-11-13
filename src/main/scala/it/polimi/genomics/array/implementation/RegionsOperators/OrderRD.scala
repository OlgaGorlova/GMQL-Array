package it.polimi.genomics.array.implementation.RegionsOperators

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.Direction
import it.polimi.genomics.core.DataStructures.GroupMDParameters._
import it.polimi.genomics.core.DataStructures.RegionOperator
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object OrderRD {

  private final val logger = Logger.getLogger(this.getClass);

  def apply(executor: GMQLArrayExecutor, ordering : List[(Int, Direction)], topParameter : TopParameter, inputDataset : RegionOperator, sc : SparkContext) : RDD[GARRAY] = {
    logger.info("----------------OrderRD: operation is not supported")

    val grouping : Boolean =
      topParameter match {
        case NoTop() => false
        case Top(_) => false
        case TopP(_) => true
        case TopG(_) => true
      }

    val top : Int =
      topParameter match {
        case NoTop() => 0
        case Top(v) => v
        case TopP(v) => v
        case TopG(v) => v
      }

    executor.implement_rd(inputDataset,sc)
  }


}
