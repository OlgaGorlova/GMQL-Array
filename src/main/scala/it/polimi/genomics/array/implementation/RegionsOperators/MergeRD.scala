package it.polimi.genomics.array.implementation.RegionsOperators

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GArray, GAttributes}
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.avro.myavro.{gregion, idsList, repRec, sampleRec}
import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  *
  * Limitations (or TODO?):
  * - groupby option
  *
  */
object MergeRD {

  private final val logger = Logger.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor: GMQLArrayExecutor, inputDataset : RegionOperator, sc : SparkContext) : RDD[GARRAY] = {
    logger.info("----------------MergeRD executing..")

    val ds = executor.implement_rd(inputDataset, sc)

    execute(ds, sc)
  }

  def apply(inputDataset : RDD[GARRAY], sc : SparkContext) : RDD[GARRAY] = {
    logger.info("----------------MergeRD executing..")

    execute(inputDataset, sc)
  }

  def applyAvro(inputDataset : RDD[gregion], sc : SparkContext) : RDD[gregion] = {
    logger.info("----------------MergeRD executing..")
    import scala.collection.JavaConversions._
    //union of samples
    inputDataset.map((r) => {

      val mergedSampleIDs = Array(idsList.newBuilder().setId(1l).setRep(r.getIdsList.foldLeft(0){ (acc, z) => acc + z.getRep}).build()).toList
      val mergedValues = r.getValuesArray.map{att =>
        sampleRec.newBuilder().setSampleArray(Array(repRec.newBuilder().setRepArray(att.getSampleArray.flatMap(_.getRepArray).toList).build()).toList).build()
      }

      gregion.newBuilder(r).setIdsList(mergedSampleIDs).setValuesArray(mergedValues).build()
    })

  }

  def execute(ds : RDD[GARRAY], sc : SparkContext) : RDD[GARRAY] = {
    val groupedDs : RDD[GARRAY] =
    //union of samples
      ds.map((r) => {

        val mergedSampleIDs = Array((1l, r._2._1.foldLeft(0){ (acc, z) => acc + z._2}))
        val mergedValues = r._2._2.map{att =>
          Array(att.flatten)
        }
        (r._1, new GAttributes(mergedSampleIDs, mergedValues))
      })

    groupedDs
  }

  def execute2(ds : RDD[GArray], sc : SparkContext) : RDD[GArray] = {
    val groupedDs : RDD[GArray] =
    //union of samples
      ds.map((r) => {

        val mergedSampleIDs = Array((1l, r._2._1.foldLeft(0){ (acc, z) => acc + z._2}))
        val mergedValues = r._2._2.map{att =>
          Array(att.flatten)
        }
        GArray(r._1, new GAttributes(mergedSampleIDs, mergedValues))
      })

    groupedDs
  }



}
