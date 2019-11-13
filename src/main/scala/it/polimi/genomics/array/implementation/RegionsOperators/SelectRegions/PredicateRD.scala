package it.polimi.genomics.array.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GAttributes, GRegionKey}
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.array.implementation.RegionsOperators.SelectRD._
import it.polimi.genomics.core
import it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.REG_OP
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionCondition}
import it.polimi.genomics.core.DataStructures.RegionCondition._
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.{GDouble, GNull, GString}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.RegionsOperators.PredicateRD.{applyRegionPredicateEQ, castDoubleOrString}
import org.apache.spark.SparkContext

/**
  * Created by Olga Gorlova on 29/10/2019.
  */
object PredicateRD {

  var executor: GMQLArrayExecutor = null

  def optimizeConditionTree(regionCondition: RegionCondition, not: Boolean, filteredMeta: Option[MetaOperator], sc: SparkContext): RegionCondition = {
    regionCondition match {

      case cond: RegionCondition.NOT => {
        NOT(optimizeConditionTree(cond.predicate, not, filteredMeta, sc))
      }

      case cond: RegionCondition.OR =>
        RegionCondition.OR(optimizeConditionTree(cond.first_predicate, not, filteredMeta, sc), optimizeConditionTree(cond.second_predicate, not, filteredMeta, sc))


      case cond: RegionCondition.AND =>
        RegionCondition.AND(optimizeConditionTree(cond.first_predicate, not, filteredMeta, sc), optimizeConditionTree(cond.second_predicate, not, filteredMeta, sc))

      case predicate: RegionCondition.Predicate => {

        val value = predicate.value match {

          case v: MetaAccessor => {
            val meta = executor.implement_md(filteredMeta.get, sc)
            meta.filter(_._2._1.equals(predicate.value.asInstanceOf[MetaAccessor].attribute_name)).distinct.collect
          }

          case v: Any => {
            predicate.value
          }
        }

        predicate.operator match {
          case REG_OP.EQ => RegionCondition.Predicate(predicate.position, RegionCondition.REG_OP.EQ, value)
          case REG_OP.NOTEQ => RegionCondition.Predicate(predicate.position, RegionCondition.REG_OP.NOTEQ, value)
          case REG_OP.GT => RegionCondition.Predicate(predicate.position, RegionCondition.REG_OP.GT, value)
          case REG_OP.GTE => RegionCondition.Predicate(predicate.position, RegionCondition.REG_OP.GTE, value)
          case REG_OP.LT => RegionCondition.Predicate(predicate.position, RegionCondition.REG_OP.LT, value)
          case REG_OP.LTE => RegionCondition.Predicate(predicate.position, RegionCondition.REG_OP.LTE, value)
        }
      }
      case noNeedForOptimization: RegionCondition => {
        noNeedForOptimization
      }
    }
  }

  def applyRegionSelect(regionCondition: RegionCondition, input: GARRAY): Boolean = {
    regionCondition match {
      case chrCond: ChrCondition => input._1._1.equals(chrCond.chr_name)
      case strandCond: StrandCondition => input._1._4.equals(strandCond.strand(0))
      case leftEndCond: LeftEndCondition => applyRegionPredicate(leftEndCond.op, leftEndCond.value, core.GDouble(input._1._2))
      case rightEndCond: RightEndCondition => applyRegionPredicate(rightEndCond.op, rightEndCond.value, core.GDouble(input._1._3))
      case startCond: StartCondition =>
        input._1._4 match {
          case '*' => applyRegionPredicate(startCond.op, startCond.value, core.GDouble(input._1._2))
          case '+' => applyRegionPredicate(startCond.op, startCond.value, core.GDouble(input._1._3))
          case '-' => applyRegionPredicate(startCond.op, startCond.value, core.GDouble(input._1._3))
        }
      case stopCond: StopCondition =>
        input._1._4 match {
          case '*' => applyRegionPredicate(stopCond.op, stopCond.value, core.GDouble(input._1._3))
          case '+' => applyRegionPredicate(stopCond.op, stopCond.value, core.GDouble(input._1._3))
          case '-' => applyRegionPredicate(stopCond.op, stopCond.value, core.GDouble(input._1._2))
        }
      case region_cond: OR => applyRegionConditionOR(region_cond.first_predicate, region_cond.second_predicate, input)
      case region_cond: AND => applyRegionConditionAND(region_cond.first_predicate, region_cond.second_predicate, input)
      case region_cond: NOT => applyRegionConditionNOT(region_cond.predicate, input)
    }
  }

  //Predicate evaluation methods

  def applyRegionCondition(rCondition: RegionCondition, region: GARRAY): Option[GARRAY] ={
    rCondition match {
      case attributePredicate: Predicate =>{
        val flags: Array[Array[Boolean]] = applyAttributePredicate(attributePredicate.operator, attributePredicate.value, region._2._2(attributePredicate.position))
        val allValues = region._2._2.map{ y=> flags.zip(y).flatMap {s=> if (s._1.exists(e => e.equals(true))) Some(s._1.zip(s._2).filter(v => v._1.equals(true)).map(_._2)) else None}}

        if (allValues.last.isEmpty) None
        else {
          val newIds = flags.zip(region._2._1).flatMap{id=>
            val tt = id._1.groupBy(l => l).map(t => (t._1, t._2.length))
            if (tt.contains(true)) Some(id._2._1, tt.get(true).get) else None
          }

          Some(region._1, new GAttributes(newIds, allValues))
        }
      }
      case _ => if (applyRegionSelect(rCondition, region)) Some(region) else None
    }
  }

  def applyMetaFilter(metaIdList: Array[Long], region: GARRAY): Option[GARRAY] ={
    var idsToKeep: Array[Int] = Array()
    var i = -1
    val ids = region._2._1.flatMap { v => i += 1; if (metaIdList.contains(v._1)) { idsToKeep = idsToKeep :+ i; Some(v) } else None }

    if (ids.nonEmpty) {
      val att = region._2._2.map { v => var j = -1; v.flatMap { a => j += 1; if (idsToKeep.contains(j)) Some(a) else None }}
      Some(region._1, new GAttributes(ids, att))
    }
    else None
  }

  def applyRegionPredicate(operator: REG_OP, value: Any, input: core.GValue): Boolean = {
    operator match {
      case REG_OP.EQ => applyRegionPredicateEQ(value, input)
      case REG_OP.NOTEQ => applyRegionPredicateNOTEQ(value, input)
      case REG_OP.GT => applyRegionPredicateGT(value, input)
      case REG_OP.GTE => applyRegionPredicateGTE(value, input)
      case REG_OP.LT => applyRegionPredicateLT(value, input)
      case REG_OP.LTE => applyRegionPredicateLTE(value, input)
    }
  }

  def applyAttributePredicate(operator: REG_OP, value: Any, input: Array[Array[core.GValue]]): Array[Array[Boolean]] = {
    input.map { x=>
      x.map { y =>
        operator match {
          case REG_OP.EQ => applyRegionPredicateEQ(value, y)
          case REG_OP.NOTEQ => applyRegionPredicateNOTEQ(value, y)
          case REG_OP.GT => applyRegionPredicateGT(value, y)
          case REG_OP.GTE => applyRegionPredicateGTE(value, y)
          case REG_OP.LT => applyRegionPredicateLT(value, y)
          case REG_OP.LTE => applyRegionPredicateLTE(value, y)
        }
      }
    }
  }

  def applyRegionPredicateEQ(value: Any, input: core.GValue): Boolean = {
    value match {
      case value: Int => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v.equals(value.toDouble)
      case value: Long => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v.equals(value.toDouble)
      case value: Double => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v.equals(value.toDouble)
      case value: String => input.asInstanceOf[GString].v.equals(value)
    }
  }

  def applyRegionPredicateNOTEQ(value: Any, input: core.GValue): Boolean = {
    value match {
      case value: Int => if (input.isInstanceOf[GNull]) false else !input.asInstanceOf[GDouble].v.equals(value.toDouble)
      case value: Long => if (input.isInstanceOf[GNull]) false else !input.asInstanceOf[GDouble].v.equals(value.toDouble)
      case value: Double => if (input.isInstanceOf[GNull]) false else !input.asInstanceOf[GDouble].v.equals(value.toDouble)
      case value: String => !input.asInstanceOf[GString].v.equals(value)
    }
  }

  def applyRegionPredicateLT(value: Any, input: core.GValue): Boolean = {
    value match {
      case value: Int => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v < value.toDouble
      case value: Long => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v < value.toDouble
      case value: Double => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v < value.toDouble
      case value: String =>
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a < comparison between string. Query: " + value + " < " + input)

    }
  }

  def applyRegionPredicateLTE(value: Any, input: core.GValue): Boolean = {
    value match {
      case value: Int => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v <= value.toDouble
      case value: Long => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v <= value.toDouble
      case value: Double => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v <= value.toDouble
      case value: String =>
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a <= comparison between string. Query: " + value + " <= " + input)
    }
  }

  def applyRegionPredicateGT(value: Any, input: core.GValue): Boolean = {
    value match {
      case value: Int => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v > value.toDouble
      case value: Long => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v > value.toDouble
      case value: Double => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v > value.toDouble
      case value: String =>
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a > comparison between string. Query: " + value + " > " + input)
    }
  }

  def applyRegionPredicateGTE(value: Any, input: core.GValue): Boolean = {
    value match {
      case value: Int => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v >= value.toDouble
      case value: Long => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v >= value.toDouble
      case value: Double => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v >= value.toDouble
      case value: String =>
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a >= comparison between string. Query: " + value + " >= " + input)
    }
  }

  def applyRegionConditionOR(meta1: RegionCondition, meta2: RegionCondition, input: (GRegionKey, GAttributes)): Boolean = {
    applyRegionSelect(meta1, input) || applyRegionSelect(meta2, input)
  }

  def applyRegionConditionAND(meta1: RegionCondition, meta2: RegionCondition, input: (GRegionKey, GAttributes)): Boolean = {
    applyRegionSelect(meta1, input) && applyRegionSelect(meta2, input)
  }

  def applyRegionConditionNOT(regions: RegionCondition, input: (GRegionKey, GAttributes)): Boolean = {
    !applyRegionSelect(regions, input)
  }

  def castDoubleOrString(value: Any) = {
    try {
      value.toString.trim.toDouble
    } catch {
      case e: Throwable => value.toString
    }
  }
}
