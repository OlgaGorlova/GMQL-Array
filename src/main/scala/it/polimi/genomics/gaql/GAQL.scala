package it.polimi.genomics.gaql

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GAttributes, GRegionKey}
import it.polimi.genomics.array.implementation.RegionsOperators._
import it.polimi.genomics.core.DataStructures.CoverParameters.CoverFlag.CoverFlag
import it.polimi.genomics.core.DataStructures.CoverParameters.CoverParam
import it.polimi.genomics.core.DataStructures.JoinParametersRD.DistLess
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.{GroupRDParameters, RegionAggregate}
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionExtension, RegionsToRegion}
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 19/04/2020.
  */
object GAQL {

  var sc: SparkContext = _

  implicit class Operators(left_array: RDD[GARRAY]) {

    def Dice(select: Option[RegionCondition], project: Option[List[Int]]): RDD[GARRAY] ={
      if (select.isDefined && !project.isDefined)
        SelectRD(select, left_array, sc)
      else if (!select.isDefined && project.isDefined)
        ProjectRD(project, None, left_array, sc)
      else if (select.isDefined && project.isDefined)
        ProjectRD(project, None, SelectRD(select, left_array, sc), sc)
      else left_array
    }

    def DrillDown(aggregate: Either[List[RegionExtension],List[RegionAggregate.RegionsToRegion]]): RDD[GARRAY] ={
      aggregate match {
        case Left(a) => ProjectRD2(None, Some(a), left_array, sc)
        case Right(b) => GroupRD(None, Some(b), left_array, sc)
      }
    }

    def RollUp(): RDD[GARRAY] ={
      MergeRD(left_array, sc)
    }

    def CellUpdate(aggregate: Option[List[RegionExtension]]): RDD[GARRAY] ={
      ProjectRD2(None, aggregate, left_array, sc)
    }


    def GroupValues(groupingParameters : Option[List[GroupRDParameters.GroupingParameter]]): RDD[GARRAY] ={
      GroupRD(groupingParameters, None, left_array, sc)
    }

    def ArrayMerge(right_array: RDD[GARRAY]): RDD[GARRAY] ={
      UnionRD(left_array, right_array, sc)
    }

    def Subtract(BINNING_PARAMETER:Long, exact:Boolean, right_array: RDD[GARRAY]): RDD[GARRAY] ={
      GenometricDifference(left_array, right_array, BINNING_PARAMETER, exact, sc)
    }

    def Map(aggregates : Option[List[RegionAggregate.RegionsToRegion]], BINNING_PARAMETER: Long, right_array: RDD[GARRAY]): RDD[GARRAY] ={
      GenometricMap(left_array, right_array, BINNING_PARAMETER, sc)
    }

    def Join(region_builder : RegionBuilder, less: Option[DistLess], BINNING_PARAMETER: Long, right_array: RDD[GARRAY]): RDD[GARRAY] ={
      GenometricJoin(left_array, right_array, region_builder, less, BINNING_PARAMETER, sc)
    }

    def Cover(coverFlag : CoverFlag, min : CoverParam, max : CoverParam, aggregators: List[RegionsToRegion], BINNING_PARAMETER: Long): RDD[GARRAY] ={
      GenometricCover_v2(coverFlag, min, max, aggregators, left_array, BINNING_PARAMETER, sc)
    }
  }

}
