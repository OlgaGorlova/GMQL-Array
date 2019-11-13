package it.polimi.genomics.array.implementation.loaders

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.{GRecordKey, GValue}
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object Export {

  private final val logger = Logger.getLogger(this.getClass);

  def apply(executor: GMQLArrayExecutor,inputDataset:RegionOperator,location:String,sc:SparkContext):RDD[GARRAY]={
    logger.info("----------------Export executing...")
    //    Store(toDecompressed(in), location, sc)
    //    println("Number of regions: " + decodeExtended(in).count())
    //    sc.emptyRDD[GARRAY]

    val input = executor.implement_rd(inputDataset, sc)
//    println("Export: " + input.count())

    input
  }

  def apply(executor: GMQLArrayExecutor,path:String,value: RegionOperator, associatedMeta:MetaOperator, schema : List[(String, PARSING_TYPE)], sc:SparkContext):RDD[GARRAY]={
    logger.info("----------------Export executing...")
    sc.emptyRDD[GARRAY]

  }

  def toRow(rdd: RDD[GARRAY]): RDD[(GRecordKey, Array[GValue])] = {
    val out: RDD[(GRecordKey, Array[GValue])] = rdd.flatMap { x =>

      val values = x._2._2.transpose
      var k = -1;
      if (values.nonEmpty) {
        x._2._1.zip(values).flatMap { sid =>
          k += 1;
          val values = sid._2.transpose
          values.map { res =>
            (new GRecordKey(sid._1._1, x._1._1, x._1._2, x._1._3, x._1._4), res)
          }
        }
      }
      else
        x._2._1.map { sid => (new GRecordKey(sid._1, x._1._1, x._1._2, x._1._3, x._1._4), new Array[GValue](0))}

    }

    out
  }

}
