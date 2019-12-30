package it.polimi.genomics.array.implementation.RegionsOperators

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GArray, GAttributes, GRegionKey}
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.array.implementation.RegionsOperators.SelectRegions.PredicateRD
import it.polimi.genomics.array.implementation.RegionsOperators.SelectRegions.PredicateRD.{applyRegionSelect, _}
import it.polimi.genomics.avro.myavro.{gregion, idsList, repRec, sampleRec}
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

import scala.collection.JavaConversions._

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

  def apply(regionCondition: Option[RegionCondition], input: RDD[GARRAY], sc: SparkContext): RDD[GARRAY] = {

    if (regionCondition.isDefined){
      input.flatMap { region => PredicateRD.applyRegionCondition(regionCondition.get, region)
      }
    }
    else input
  }

  def applyAvro(regionCondition: Option[RegionCondition], input: RDD[gregion], sc: SparkContext): RDD[gregion] = {

    if (regionCondition.isDefined){
      input.flatMap { region =>
        applyRegionCondition(regionCondition.get, region)
      }
    }
    else input
  }

  def applyRegionSelect(regionCondition: RegionCondition, input: gregion): Boolean = {
    regionCondition match {
      case chrCond: ChrCondition => input.getChr.equals(chrCond.chr_name)
      case strandCond: StrandCondition => input.getStrand.equals(strandCond.strand(0))
      case leftEndCond: LeftEndCondition => applyRegionPredicate(leftEndCond.op, leftEndCond.value, core.GDouble(input.getStart))
      case rightEndCond: RightEndCondition => applyRegionPredicate(rightEndCond.op, rightEndCond.value, core.GDouble(input.getStop))
      case startCond: StartCondition =>
        input.getStrand.charAt(0) match {
          case '*' => applyRegionPredicate(startCond.op, startCond.value, core.GDouble(input.getStart))
          case '+' => applyRegionPredicate(startCond.op, startCond.value, core.GDouble(input.getStop))
          case '-' => applyRegionPredicate(startCond.op, startCond.value, core.GDouble(input.getStop))
        }
      case stopCond: StopCondition =>
        input.getStrand.charAt(0) match {
          case '*' => applyRegionPredicate(stopCond.op, stopCond.value, core.GDouble(input.getStop))
          case '+' => applyRegionPredicate(stopCond.op, stopCond.value, core.GDouble(input.getStop))
          case '-' => applyRegionPredicate(stopCond.op, stopCond.value, core.GDouble(input.getStart))
        }
      case region_cond: OR => applyRegionConditionOR(region_cond.first_predicate, region_cond.second_predicate, input)
      case region_cond: AND => applyRegionConditionAND(region_cond.first_predicate, region_cond.second_predicate, input)
      case region_cond: NOT => applyRegionConditionNOT(region_cond.predicate, input)
    }
  }

  //Predicate evaluation methods

  def applyRegionCondition(rCondition: RegionCondition, region: gregion): Option[gregion] ={
    rCondition match {
      case attributePredicate: Predicate =>{
        val arr: Array[Array[GValue]] = region.getValuesArray.get(attributePredicate.position).getSampleArray.toArray().map{v=>
          val gg: Array[GValue] = v.asInstanceOf[repRec].getRepArray.toArray.map(v1=> GDouble(v1.asInstanceOf[Double]))
        gg
        }
        val flags: Array[Array[Boolean]] = applyAttributePredicate(attributePredicate.operator, attributePredicate.value, arr)
        val allValues = region.getValuesArray.map{ y=> flags.zip(y.getSampleArray).flatMap {s=> if (s._1.exists(e => e.equals(true))) Some(s._1.zip(s._2.getRepArray).filter(v => v._1.equals(true)).map(_._2)) else None}}

        if (allValues.last.isEmpty) None
        else {
          val newIds: java.util.List[idsList] = flags.zip(region.getIdsList).flatMap{ id=>
            val tt = id._1.groupBy(l => l).map(t => (t._1, t._2.length))
            if (tt.contains(true)) Some(idsList.newBuilder(id._2).setRep(tt.get(true).get).build() ) else None
          }.toList
          val newVal = allValues.map{v=>
            sampleRec.newBuilder().setSampleArray(v.map(r=> repRec.newBuilder().setRepArray(r.toList).build()).toList).build()
          }.toList

          Some(gregion.newBuilder(region).setIdsList(newIds).setValuesArray(newVal).build())
        }
      }
      case _ => if (applyRegionSelect(rCondition, region)) Some(region) else None
    }
  }

  def applyRegionConditionOR(meta1: RegionCondition, meta2: RegionCondition, input: gregion): Boolean = {
    applyRegionSelect(meta1, input) || applyRegionSelect(meta2, input)
  }

  def applyRegionConditionAND(meta1: RegionCondition, meta2: RegionCondition, input: gregion): Boolean = {
    applyRegionSelect(meta1, input) && applyRegionSelect(meta2, input)
  }

  def applyRegionConditionNOT(regions: RegionCondition, input: gregion): Boolean = {
    !applyRegionSelect(regions, input)
  }


}
