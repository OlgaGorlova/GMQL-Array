package it.polimi.genomics.array.implementation.RegionsOperators

import java.{lang, util}

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GArray, GAttributes, GRegionKey}
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.RegionAggregate.{COORD_POS, RegionExtension}
import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.{GDouble, GString, GValue}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.softwaremill.quicklens._
import it.polimi.genomics.avro.myavro.{gregion, idsList, repRec, sampleRec}

import scala.collection.JavaConversions._

/**
  * Created by Olga Gorlova on 17/10/2019.
  *
  * Limitations:
  * - no usage of REMetaAccessor()
  * - not possible to update start/stop using attribute value
  *
  */
object ProjectRD {

  private final val logger = Logger.getLogger(this.getClass);

  def apply(executor: GMQLArrayExecutor, projectedValues : Option[List[Int]], tupleAggregator : Option[List[RegionExtension]], inputDataset : RegionOperator, sc: SparkContext) : RDD[(GRegionKey, GAttributes)] = {

    logger.info("----------------ProjectRD executing...")

    val input = executor.implement_rd(inputDataset, sc)

    execute(projectedValues, tupleAggregator, input, sc)
  }

  def apply(projectedValues : Option[List[Int]], tupleAggregator : Option[List[RegionExtension]], inputDataset : RDD[GARRAY], sc: SparkContext) : RDD[GARRAY] = {

    logger.info("----------------ProjectRD executing...")

    execute(projectedValues, tupleAggregator, inputDataset, sc)
  }

  def applyAvro(projectedValues : Option[List[Int]], tupleAggregator : Option[List[RegionExtension]], inputDataset : RDD[gregion], sc: SparkContext) : RDD[gregion] = {

    logger.info("----------------ProjectRD executing...")

    val ags = if (tupleAggregator.isDefined) tupleAggregator.get else List()
    val extended = if (tupleAggregator.isDefined) {
      inputDataset.flatMap { a => extendRegion3(a, a, tupleAggregator.get)
      }
    }
    else inputDataset

    val prepared = if(projectedValues.isDefined)
      extended.map{a  =>
        val prV = a.getValuesArray.zipWithIndex.flatMap(v=> if (projectedValues.get.contains(v._2)) Some(v._1) else None)
        gregion.newBuilder(a).setValuesArray(prV).build()}
    else if (tupleAggregator.isDefined)
      extended
    else
      extended.map{a=>
        gregion.newBuilder(a).clearValuesArray().build()}
    prepared
  }

  def execute(projectedValues : Option[List[Int]], tupleAggregator : Option[List[RegionExtension]], input : RDD[GARRAY], sc: SparkContext) : RDD[(GRegionKey, GAttributes)] = {

    val ags = if (tupleAggregator.isDefined) tupleAggregator.get else List()
    val extended = if (tupleAggregator.isDefined) {
      input.flatMap { a => extendRegion(a, a, tupleAggregator.get)
      }
    }
    else input

    val prepared = if(projectedValues.isDefined)
      extended.map(a  => (a._1,  new GAttributes(a._2._1, a._2._2.zipWithIndex.flatMap(v=> if (projectedValues.get.contains(v._2)) Some(v._1) else None ) )))
    else if (tupleAggregator.isDefined)
      extended
    else
      extended.map(a=> (a._1, new GAttributes(a._2._1)))

    prepared
  }

  def execute2(projectedValues : Option[List[Int]], tupleAggregator : Option[List[RegionExtension]], input : RDD[GArray], sc: SparkContext) : RDD[GArray] = {

    val ags = if (tupleAggregator.isDefined) tupleAggregator.get else List()
    val extended = if (tupleAggregator.isDefined) {
      input.flatMap { a => extendRegion2(a, a, tupleAggregator.get)
      }
    }
    else input

    val prepared = if(projectedValues.isDefined)
      extended.map(a  => a.modify(_.values).setTo(new GAttributes(a._2._1, a._2._2.zipWithIndex.flatMap(v=> if (projectedValues.get.contains(v._2)) Some(v._1) else None ) )))
    else if (tupleAggregator.isDefined)
      extended
    else
      extended.map(a=> a.modify(_.values).setTo(new GAttributes(a._2._1)))

    prepared
  }

  def computeFunction(r : (GRegionKey, GAttributes), agg : RegionExtension) : GValue = {
    agg.fun( agg.inputIndexes.foldLeft(Array[GValue]())((acc,b) => acc :+ {
      b.asInstanceOf[Int] match {
        case COORD_POS.CHR_POS => new GString(r._1._1)
        case COORD_POS.LEFT_POS => new GDouble(r._1._2)
        case COORD_POS.RIGHT_POS => new GDouble(r._1._3)
        case COORD_POS.STRAND_POS => new GString(r._1._4.toString)
        case COORD_POS.START_POS => if (r._1.strand.equals('+') || r._1.strand.equals('*')) new GDouble(r._1._2) else new GDouble(r._1._3)
        case COORD_POS.STOP_POS => if (r._1.strand.equals('+') || r._1.strand.equals('*')) new GDouble(r._1._3) else new GDouble(r._1._2)
        //        case _: Int => r._2(b.asInstanceOf[Int])
      }
    }))
  }

  def computeFunction2(r : gregion, agg : RegionExtension) : GValue = {
    agg.fun( agg.inputIndexes.foldLeft(Array[GValue]())((acc,b) => acc :+ {
      b.asInstanceOf[Int] match {
        case COORD_POS.CHR_POS => new GString(r.getChr.toString)
        case COORD_POS.LEFT_POS => new GDouble(r.getStart)
        case COORD_POS.RIGHT_POS => new GDouble(r.getStop)
        case COORD_POS.STRAND_POS => new GString(r.getStrand.toString)
        case COORD_POS.START_POS => if (r.getStrand.equals('+') || r.getStrand.equals('*')) new GDouble(r.getStart) else new GDouble(r.getStop)
        case COORD_POS.STOP_POS => if (r.getStrand.equals('+') || r.getStrand.equals('*')) new GDouble(r.getStop) else new GDouble(r.getStart)
        //        case _: Int => r._2(b.asInstanceOf[Int])
      }
    }))
  }

  def extendRegion(out : (GRegionKey, GAttributes), r:(GRegionKey, GAttributes), aggList : List[RegionExtension]) : Option[(GRegionKey, GAttributes)] = {
    if(aggList.isEmpty) {
      //out
      if (out._1._2 >= out._1._3) // if left > right, the region is deleted
      {
        None
      }
      else if (out._1._2 < 0) //if left become < 0, set it to 0
      {
        Some((new GRegionKey(out._1._1, 0, out._1._3, out._1._4), out._2))
      }
      else Some(out)
    }
    else {
      val agg = aggList.head
      agg.output_index match {
        case Some(COORD_POS.CHR_POS) => extendRegion((new GRegionKey(computeFunction(r, agg).toString, out._1._2, out._1._3, out._1._4), out._2),r, aggList.tail)
        case Some(COORD_POS.LEFT_POS) => extendRegion((new GRegionKey(out._1._1, computeFunction(r, agg).toString.toLong, out._1._3, out._1._4), out._2),r, aggList.tail)
        case Some(COORD_POS.RIGHT_POS) => extendRegion((new GRegionKey(out._1._1, out._1._2, computeFunction(r, agg).toString.toLong, out._1._4), out._2),r, aggList.tail)
        case Some(COORD_POS.STRAND_POS) => extendRegion((new GRegionKey(out._1._1, out._1._2, out._1._3, computeFunction(r, agg).toString.charAt(0)), out._2),r, aggList.tail)
        case Some(COORD_POS.START_POS) => {
          if (out._1._4.equals('-')) {
            extendRegion((new GRegionKey(out._1._1, out._1._2, computeFunction(r, agg).toString.toLong, out._1._4), out._2), r, aggList.tail)
          } else
            extendRegion((new GRegionKey(out._1._1, computeFunction(r, agg).toString.toLong, out._1._4, out._1._4), out._2),r, aggList.tail)
        }
        case Some(COORD_POS.STOP_POS) => {
          if (out._1._4.equals('-')) {
            extendRegion((new GRegionKey(out._1._1, computeFunction(r, agg).toString.toLong, out._1._3, out._1._4), out._2), r, aggList.tail)
          } else
            extendRegion((new GRegionKey(out._1._1, out._1._2, computeFunction(r, agg).toString.toLong, out._1._4), out._2), r, aggList.tail)
        }
        case Some(v : Int) => extendRegion((out._1, out._2 ),r, aggList.tail)
        case None => {
          val newValue = computeFunction(r, agg)
          val newAtt: Array[Array[GValue]] = out._2._1.map(v=> Array.fill[GValue](v._2)(newValue))
          extendRegion((out._1, new GAttributes(out._2._1, out._2._2 :+ newAtt)),r, aggList.tail)

        }
      }
    }
  }

  def extendRegion2(out : GArray, r:GArray, aggList : List[RegionExtension]) : Option[GArray] = {
    if(aggList.isEmpty) {
      //out
      if (out._1._2 >= out._1._3) // if left > right, the region is deleted
      {
        None
      }
      else if (out._1._2 < 0) //if left become < 0, set it to 0
      {
        Some(GArray(new GRegionKey(out._1._1, 0, out._1._3, out._1._4), out._2))
      }
      else Some(out)
    }
    else {

      val agg = aggList.head
      agg.output_index match {
        case Some(COORD_POS.CHR_POS) => extendRegion2(out.modify(_.key).setTo(new GRegionKey(computeFunction((r._1,r._2), agg).toString, out._1._2, out._1._3, out._1._4)),r, aggList.tail)
        case Some(COORD_POS.LEFT_POS) => extendRegion2(out.modify(_.key).setTo(new GRegionKey(out._1._1, computeFunction((r._1,r._2), agg).toString.toLong, out._1._3, out._1._4)),r, aggList.tail)
        case Some(COORD_POS.RIGHT_POS) => extendRegion2(out.modify(_.key).setTo(new GRegionKey(out._1._1, out._1._2, computeFunction((r._1,r._2), agg).toString.toLong, out._1._4)),r, aggList.tail)
        case Some(COORD_POS.STRAND_POS) => extendRegion2(out.modify(_.key).setTo(new GRegionKey(out._1._1, out._1._2, out._1._3, computeFunction((r._1,r._2), agg).toString.charAt(0))),r, aggList.tail)
        case Some(COORD_POS.START_POS) => {
          if (out._1._4.equals('-')) {
            extendRegion2(out.modify(_.key).setTo(new GRegionKey(out._1._1, out._1._2, computeFunction((r._1,r._2), agg).toString.toLong, out._1._4)), r, aggList.tail)
          } else
            extendRegion2(out.modify(_.key).setTo(new GRegionKey(out._1._1, computeFunction((r._1,r._2), agg).toString.toLong, out._1._4, out._1._4)),r, aggList.tail)
        }
        case Some(COORD_POS.STOP_POS) => {
          if (out._1._4.equals('-')) {
            extendRegion2(out.modify(_.key).setTo(new GRegionKey(out._1._1, computeFunction((r._1,r._2), agg).toString.toLong, out._1._3, out._1._4)), r, aggList.tail)
          } else
            extendRegion2(out.modify(_.key).setTo(new GRegionKey(out._1._1, out._1._2, computeFunction((r._1,r._2), agg).toString.toLong, out._1._4)), r, aggList.tail)
        }
        case Some(v : Int) => extendRegion2(out,r, aggList.tail)
        case None => {
          val newValue = computeFunction((r._1,r._2), agg)
          val newAtt: Array[Array[GValue]] = out._2._1.map(v=> Array.fill[GValue](v._2)(newValue))
          extendRegion2(out.modify(_.values).setTo(new GAttributes(out._2._1, out._2._2 :+ newAtt)),r, aggList.tail)

        }
      }
    }
  }

  def extendRegion3(out : gregion, r:gregion, aggList : List[RegionExtension]) : Option[gregion] = {

    if(aggList.isEmpty) {
      //out
      if (out.getStart >= out.getStop) // if left > right, the region is deleted
      {
        None
      }
      else if (out.getStart < 0) //if left become < 0, set it to 0
      {
        val newRegion = gregion.newBuilder(out).setChr(out.getChr).setStart(out.getStart).setStop(out.getStop).setStrand(out.getStrand).build()
        Some(newRegion)
      }
      else Some(out)
    }
    else {
      val agg = aggList.head
      agg.output_index match {
        case Some(COORD_POS.CHR_POS) => {
          val newRegion = gregion.newBuilder(out).setChr(computeFunction2(r, agg).toString).setStart(out.getStart).setStop(out.getStop).setStrand(out.getStrand).build()
          extendRegion3(newRegion, r, aggList.tail)
        }
        case Some(COORD_POS.LEFT_POS) => {
          val newRegion = gregion.newBuilder(out).setChr(out.getChr).setStart(computeFunction2(r, agg).toString.toLong).setStop(out.getStop).setStrand(out.getStrand).build()
          extendRegion3(newRegion,r, aggList.tail)
        }
        case Some(COORD_POS.RIGHT_POS) => {
          val newRegion = gregion.newBuilder(out).setChr(out.getChr).setStart(out.getStart).setStop(computeFunction2(r, agg).toString.toLong).setStrand(out.getStrand).build()
          extendRegion3(newRegion,r, aggList.tail)
        }
        case Some(COORD_POS.STRAND_POS) => {
          val newRegion = gregion.newBuilder(out).setChr(out.getChr).setStart(out.getStart).setStop(out.getStop).setStrand(computeFunction2(r, agg).toString).build()
          extendRegion3(newRegion,r, aggList.tail)
        }
        case Some(COORD_POS.START_POS) => {
          if (out.getStrand.equals('-')) {
            val newRegion = gregion.newBuilder(out).setChr(out.getChr).setStart(out.getStart).setStop(computeFunction2(r, agg).toString.toLong).setStrand(out.getStrand).build()
            extendRegion3(newRegion, r, aggList.tail)
          } else
            {
              val newRegion = gregion.newBuilder(out).setChr(out.getChr).setStart(computeFunction2(r, agg).toString.toLong).setStop(out.getStop).setStrand(out.getStrand).build()
              extendRegion3(newRegion,r, aggList.tail)
            }
        }
        case Some(COORD_POS.STOP_POS) => {
          if (out.getStrand.equals('-')) {
            val newRegion = gregion.newBuilder(out).setChr(out.getChr).setStart(computeFunction2(r, agg).toString.toLong).setStop(out.getStop).setStrand(out.getStrand).build()
            extendRegion3(newRegion, r, aggList.tail)
          } else
            {
              val newRegion = gregion.newBuilder(out).setChr(out.getChr).setStart(out.getStart).setStop(computeFunction2(r, agg).toString.toLong).setStrand(out.getStrand).build()
              extendRegion3(newRegion, r, aggList.tail)
            }
        }
        case Some(v : Int) => extendRegion3(out,r, aggList.tail)
        case None => {
          val newValue = {
            val t = computeFunction2(r, agg)
            if (t.isInstanceOf[GDouble]) t.asInstanceOf[GDouble].v else 0
          }
          val newRepArray: util.List[repRec] = out.getIdsList.toArray().map{ v =>
            val rr = Array.fill[Double](v.asInstanceOf[idsList].getRep)(newValue)
            val tt: java.util.List[java.lang.Double] = rr.toList.map(Double.box)
            repRec.newBuilder().setRepArray(tt).build()
          }.toList
          val newAtt = sampleRec.newBuilder().setSampleArray(newRepArray).build()
          val newVal = out.getValuesArray :+ newAtt
//          val newAtt: Array[Array[GValue]] = out._2._1.map(v=> Array.fill[GValue](v._2)(newValue))
          val newRegion = gregion.newBuilder(out).setValuesArray(newVal).build()
          extendRegion3(newRegion,r, aggList.tail)

        }
      }
    }
  }

}
