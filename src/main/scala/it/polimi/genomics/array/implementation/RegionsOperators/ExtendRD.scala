package it.polimi.genomics.array.implementation.RegionsOperators

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.GArray
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToMeta
import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.GNull
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  *
  * Take all values of all regions related to the same sample and applies aggregation function. The result is added to Metadata of the sample.
  *
  *
  */
object ExtendRD {

  private final val logger = Logger.getLogger(this.getClass);

  def apply(executor: GMQLArrayExecutor, inputDataset: RegionOperator, aggregators: List[RegionsToMeta], sc: SparkContext): RDD[(Long, (String, String))] = {
    logger.info("----------------ExtendRD executing..")

    val ds = executor.implement_rd(inputDataset, sc)

    execute(ds, aggregators,sc)

  }

  def apply(inputDataset: RDD[GArray], aggregators: List[RegionsToMeta], sc: SparkContext): RDD[(Long, (String, String))] = {
    logger.info("----------------ExtendRD executing..")

    execute2(inputDataset, aggregators,sc)

  }

  def execute(ds: RDD[GARRAY], aggregators: List[RegionsToMeta], sc: SparkContext): RDD[(Long, (String, String))] = {

    val notAssociative = aggregators.flatMap(x => if (!x.associative) Some(x) else None)
    val associative = aggregators.flatMap(x => if (x.associative) Some(x) else None)

    val rddAssociative = if (associative.size > 0) {
      Aggregatable(ds, associative)
    } else sc.emptyRDD[MetaType]

    val rddNotAssociative = if (notAssociative.size > 0) {
      //extract the value from the array
      ds.flatMap { x => val tt = x._2._2; x._2._1.map(_._1).zip(x._2._2.reverse.transpose.map(v => v.reverse)).map(s => (s._1, notAssociative.map(a => s._2(a.inputIndex)).toArray)) }
        .reduceByKey((a, b) => a.zip(b).map(x => x._1 ++ x._2)).cache
        //evaluate aggregations
        .flatMap(v => notAssociative.zip(v._2).map(agg => (v._1, (agg._1.newAttributeName, agg._1.fun(agg._2.toList).toString))))
    } else sc.emptyRDD[MetaType]

    rddAssociative.union(rddNotAssociative)

  }

  def execute2(ds: RDD[GArray], aggregators: List[RegionsToMeta], sc: SparkContext): RDD[(Long, (String, String))] = {

    val notAssociative = aggregators.flatMap(x => if (!x.associative) Some(x) else None)
    val associative = aggregators.flatMap(x => if (x.associative) Some(x) else None)

    val rddAssociative = if (associative.size > 0) {
      ds.flatMap { x =>
        val values = x.values._1.map(_._1).zip(x.values._2.reverse.transpose.map(v => v.reverse)).map(s => (s._1, associative.map(a => s._2(a.inputIndex)).toArray, (1, s._2.map(t => if (t.isInstanceOf[GNull]) 0 else 1).iterator.toArray)))
        values.map { s => var i = -1; (s._1, (associative.map { a => i += 1; a.fun(s._2(i).toList) }.toArray, s._3)) }
      }
        .reduceByKey { (x, y) => var i = -1; (associative.map { a => i += 1; a.fun(List(x._1(i), y._1(i))) }.toArray, (x._2._1 + y._2._1, x._2._2.zip(y._2._2).map(s => s._1 + s._2).iterator.toArray)) }
        .flatMap { x => var i = -1; associative.map { a => i += 1; (x._1, (a.newAttributeName, a.funOut(x._2._1(i), (x._2._2._1, if (x._2._2._2.size > 0) x._2._2._2(a.inputIndex) else 0)).toString)) } }

    } else sc.emptyRDD[MetaType]

    val rddNotAssociative = if (notAssociative.size > 0) {
      //extract the value from the array
      ds.flatMap { x => val tt = x.values._2; x.values._1.map(_._1).zip(x.values._2.reverse.transpose.map(v => v.reverse)).map(s => (s._1, notAssociative.map(a => s._2(a.inputIndex)).toArray)) }
        .reduceByKey((a, b) => a.zip(b).map(x => x._1 ++ x._2)).cache
        //evaluate aggregations
        .flatMap(v => notAssociative.zip(v._2).map(agg => (v._1, (agg._1.newAttributeName, agg._1.fun(agg._2.toList).toString))))
    } else sc.emptyRDD[MetaType]

    rddAssociative.union(rddNotAssociative)

  }


  def Aggregatable(rdd: RDD[GARRAY], aggregator: List[RegionsToMeta]): RDD[(Long, (String, String))] = {

    rdd.flatMap { x =>
      val values = x._2._1.map(_._1).zip(x._2._2.reverse.transpose.map(v => v.reverse)).map(s => (s._1, aggregator.map(a => s._2(a.inputIndex)).toArray, (1, s._2.map(t => if (t.isInstanceOf[GNull]) 0 else 1).iterator.toArray)))
      values.map { s => var i = -1; (s._1, (aggregator.map { a => i += 1; a.fun(s._2(i).toList) }.toArray, s._3)) }
    }
      .reduceByKey { (x, y) => var i = -1; (aggregator.map { a => i += 1; a.fun(List(x._1(i), y._1(i))) }.toArray, (x._2._1 + y._2._1, x._2._2.zip(y._2._2).map(s => s._1 + s._2).iterator.toArray)) }
      .flatMap { x => var i = -1; aggregator.map { a => i += 1; (x._1, (a.newAttributeName, a.funOut(x._2._1(i), (x._2._2._1, if (x._2._2._2.size > 0) x._2._2._2(a.inputIndex) else 0)).toString)) } }

  }

}


