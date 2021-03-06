package it.polimi.genomics.array.implementation.RegionsOperators

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GArray, GAttributes}
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.avro.myavro.{gregion, idsList, repRec, sampleRec}
import it.polimi.genomics.core.DataStructures.GroupRDParameters.FIELD
import it.polimi.genomics.core.DataStructures.{GroupRDParameters, RegionAggregate, RegionOperator}
import it.polimi.genomics.core.{GDouble, GNull, GValue}
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Created by Olga Gorlova on 17/10/2019.
  *
  * Grouping by coordinates is implicit in the array model. It is possible to group by several attributes.
  *
  * Limitations:
  * - meta_aggregates
  *
  * TODO: fix grouping on attributes (and aggregation) when sample, f.e., has the following: [1; 2; 1; 2]
  */
object GroupRD {
  private final val logger = LoggerFactory.getLogger(this.getClass);


  @throws[SelectFormatException]
  def apply(executor: GMQLArrayExecutor, groupingParameters : Option[List[GroupRDParameters.GroupingParameter]], aggregates : Option[List[RegionAggregate.RegionsToRegion]], inputDataset : RegionOperator, sc : SparkContext) : RDD[GARRAY] = {
    logger.info("----------------GroupRD executing..")

    val ds = executor.implement_rd(inputDataset, sc)

    execute(groupingParameters, aggregates, ds, sc)

  }

  def apply(groupingParameters : Option[List[GroupRDParameters.GroupingParameter]], aggregates : Option[List[RegionAggregate.RegionsToRegion]], inputDataset : RDD[GARRAY], sc : SparkContext) : RDD[GARRAY] = {
    logger.info("----------------GroupRD executing..")

    execute(groupingParameters, aggregates, inputDataset, sc)
  }

  def applyAvro(groupingParameters : Option[List[GroupRDParameters.GroupingParameter]], aggregates : Option[List[RegionAggregate.RegionsToRegion]], inputDataset : RDD[gregion], sc : SparkContext) : RDD[gregion] = {
    logger.info("----------------GroupRD executing..")

    inputDataset.map{r=>
      if (groupingParameters.isDefined) {
        val positions: List[Int] = groupingParameters.get.map{ t => t match {
          case FIELD(pos) => pos
        }}
        val groupedValues = r.getValuesArray.zipWithIndex.flatMap{p=> if (positions.contains(p._2)) Some(p._1) else None}.toArray
        val flags: Array[Boolean] = groupedValues.map { a => a.getSampleArray.map(v => if (v.getRepArray.distinct.length > 1) false else true).toArray }.toArray.transpose.map(f => !f.contains(false))
        val values = groupedValues.map { g => sampleRec.newBuilder().setSampleArray(g.getSampleArray.zip(flags).map(v => if (v._2) repRec.newBuilder().setRepArray(v._1.getRepArray.distinct).build() else v._1).toList).build() }
        val ids = r.getIdsList.zip(flags).map(f => if (f._2) idsList.newBuilder(f._1).setRep(1).build() else f._1).toList

        val aggregated =
          (if (aggregates.isDefined) {
            aggregates.get.foldLeft(new Array[sampleRec](0)) { (z, agg) =>
              val value = r.getValuesArray.get(agg.index).getSampleArray
              val fun = value.map(v=>agg.fun(v.getRepArray.map(GDouble(_)).toList))
              val count = value.map{v=> (v.getRepArray.length, v.getRepArray.foldLeft(0)((x, y) => if (y.isInstanceOf[GNull]) x + 0 else x + 1))}
              val funOut = fun.zip(count).map(f=>agg.funOut(f._1, f._2).asInstanceOf[GDouble].v).map(Double.box).toList.map(v=> repRec.newBuilder().setRepArray(List(v)).build())

              z :+ sampleRec.newBuilder().setSampleArray(funOut).build()
            }.toList
          } else {
            List(sampleRec.newBuilder().build())
          })

        gregion.newBuilder(r).setIdsList(ids).setValuesArray(aggregated).build()
//        GArray(r._1, new GAttributes(ids, values ++ aggregated ))
      }
      else{
        val aggregated =
          (if (aggregates.isDefined) {
            aggregates.get.foldLeft(new Array[sampleRec](0)) { (z, agg) =>
              val value = r.getValuesArray.get(agg.index).getSampleArray
              val fun = value.map(v=>agg.fun(v.getRepArray.map(GDouble(_)).toList))
              val count = value.map{v=> (v.getRepArray.length, v.getRepArray.foldLeft(0)((x, y) => if (y.isInstanceOf[GNull]) x + 0 else x + 1))}
              val funOut = fun.zip(count).map(f=>agg.funOut(f._1, f._2).asInstanceOf[GDouble].v).map(Double.box).toList.map(v=> repRec.newBuilder().setRepArray(List(v)).build())
              z :+ sampleRec.newBuilder().setSampleArray(funOut).build()
            }.toList
          } else {
            List(sampleRec.newBuilder().build())
          })

        gregion.newBuilder(r).setValuesArray(aggregated).build()
      }
    }

  }

  def execute2(groupingParameters : Option[List[GroupRDParameters.GroupingParameter]], aggregates : Option[List[RegionAggregate.RegionsToRegion]], ds : RDD[GArray], sc : SparkContext) : RDD[GArray] = {
    val res : RDD[GArray] =
      ds.map{r=>
        if (groupingParameters.isDefined) {
          val positions: List[Int] = groupingParameters.get.map{ t => t match {
            case FIELD(pos) => pos
          }}
          val groupedValues = r._2._2.zipWithIndex.flatMap{p=> if (positions.contains(p._2)) Some(p._1) else None}
          val flags: Array[Boolean] = groupedValues.map { a => a.map(v => if (v.distinct.length > 1) false else true) }.transpose.map(f => !f.contains(false))
          val values = groupedValues.map { g => g.zip(flags).map(v => if (v._2) v._1.distinct else v._1) }
          val ids = r._2._1.zip(flags).map(f => if (f._2) (f._1._1, 1) else f._1)

          val aggregated: Array[Array[Array[GValue]]] =
            (if (aggregates.isDefined) {
              aggregates.get.foldLeft(new Array[Array[GValue]](0)) { (z, agg) =>
                val value = r._2._2(agg.index)
                val fun = value.map(v=>agg.fun(v.toList))
                val count = value.map{v=> (v.length, v.foldLeft(0)((x, y) => if (y.isInstanceOf[GNull]) x + 0 else x + 1))}
                val funOut = fun.zip(count).map(f=>agg.funOut(f._1, f._2))
                z :+ funOut
              }
            } else {
              new Array[Array[GValue]](0)
            }).map(a=> a.map(Array(_)))

          GArray(r._1, new GAttributes(ids, values ++ aggregated ))
        }
        else{
          val aggregated: Array[Array[Array[GValue]]] =
            (if (aggregates.isDefined) {
              aggregates.get.foldLeft(new Array[Array[GValue]](0)) { (z, agg) =>
                val value = r._2._2(agg.index)
                val fun = value.map(v=>agg.fun(v.toList))
                val count = value.map{v=> (v.length, v.foldLeft(0)((x, y) => if (y.isInstanceOf[GNull]) x + 0 else x + 1))}
                val funOut = fun.zip(count).map(f=>agg.funOut(f._1, f._2))
                z :+ funOut
              }
            } else {
              new Array[Array[GValue]](0)
            }).map(a=> a.map(Array(_)))

          GArray(r._1, new GAttributes(r._2._1, aggregated))
        }
      }

    res
  }

  def execute(groupingParameters : Option[List[GroupRDParameters.GroupingParameter]], aggregates : Option[List[RegionAggregate.RegionsToRegion]], ds : RDD[GARRAY], sc : SparkContext) : RDD[GARRAY] = {
    val res : RDD[GARRAY] =
      ds.map{r=>
        if (groupingParameters.isDefined) {
          val positions: List[Int] = groupingParameters.get.map{ t => t match {
            case FIELD(pos) => pos
          }}
          val groupedValues = r._2._2.zipWithIndex.flatMap{p=> if (positions.contains(p._2)) Some(p._1) else None}
          val flags: Array[Boolean] = groupedValues.map { a => a.map(v => if (v.distinct.length > 1) false else true) }.transpose.map(f => !f.contains(false))
          val values = groupedValues.map { g => g.zip(flags).map(v => if (v._2) v._1.distinct else v._1) }
          val ids = r._2._1.zip(flags).map(f => if (f._2) (f._1._1, 1) else f._1)

          val aggregated: Array[Array[Array[GValue]]] =
            (if (aggregates.isDefined) {
              aggregates.get.foldLeft(new Array[Array[GValue]](0)) { (z, agg) =>
                val value = r._2._2(agg.index)
                val fun = value.map(v=>agg.fun(v.toList))
                val count = value.map{v=> (v.length, v.foldLeft(0)((x, y) => if (y.isInstanceOf[GNull]) x + 0 else x + 1))}
                val funOut = fun.zip(count).map(f=>agg.funOut(f._1, f._2))
                z :+ funOut
              }
            } else {
              new Array[Array[GValue]](0)
            }).map(a=> a.map(Array(_)))

          (r._1, new GAttributes(ids, values ++ aggregated ))
        }
        else{
          val aggregated: Array[Array[Array[GValue]]] =
            (if (aggregates.isDefined) {
              aggregates.get.foldLeft(new Array[Array[GValue]](0)) { (z, agg) =>
                val value = r._2._2(agg.index)
                val fun = value.map(v=>agg.fun(v.toList))
                val count = value.map{v=> (v.length, v.foldLeft(0)((x, y) => if (y.isInstanceOf[GNull]) x + 0 else x + 1))}
                val funOut = fun.zip(count).map(f=>agg.funOut(f._1, f._2))
                z :+ funOut
              }
            } else {
              new Array[Array[GValue]](0)
            }).map(a=> a.map(Array(_)))

          (r._1, new GAttributes(r._2._1, aggregated))
        }
      }

    res
  }

}

