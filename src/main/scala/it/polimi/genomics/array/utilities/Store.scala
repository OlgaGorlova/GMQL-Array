package it.polimi.genomics.array.utilities

import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.spark.implementation.loaders.writeMultiOutputFiles.RDDMultipleTextOutputFormat
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.Map

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object Store {
  def apply(regions: RDD[GRECORD], outputPath: String, sc: SparkContext): RDD[GRECORD] = {
    val outSample = "S"

    val Ids = regions.map(_._1._1).distinct()
    val newIDS: Map[Long, Long] = Ids.zipWithIndex().collectAsMap()
    val newIDSbroad = sc.broadcast(newIDS)
    implicit def orderGrecord: Ordering[GRECORD] = Ordering.by{s => val e = s._1;(e._1,e._2,e._3,e._4)}
    val regionsPartitioner = new HashPartitioner(Ids.count.toInt)
    val keyedRDD =
      regions.sortBy(s=>s._1).map{x =>
        (outSample+"_"+ "%05d".format(newIDSbroad.value.get(x._1._1).getOrElse(x._1._1))+".gdm",
          x._1._2 + "\t" + x._1._3  + "\t" + x._1._4 + "\t" + x._1._5 + "\t" + x._2.mkString("\t"))}
        .partitionBy(regionsPartitioner)

    keyedRDD.saveAsHadoopFile(outputPath,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])

    regions
  }

  def save(regions: RDD[it.polimi.genomics.core.DataTypes.GRECORD], outputPath: String, sc: SparkContext): RDD[it.polimi.genomics.core.DataTypes.GRECORD] = {
    val outSample = "S"

    val Ids = regions.map(_._1._1).distinct()
    val newIDS: Map[Long, Long] = Ids.zipWithIndex().collectAsMap()
    val newIDSbroad = sc.broadcast(newIDS)
    implicit def orderGrecord: Ordering[it.polimi.genomics.core.DataTypes.GRECORD] = Ordering.by{s => val e = s._1;(e._1,e._2,e._3,e._4)}
    val regionsPartitioner = new HashPartitioner(Ids.count.toInt)
    val keyedRDD =
      regions.sortBy(s=>s._1).map{x =>
        (outSample+"_"+ "%05d".format(newIDSbroad.value.get(x._1._1).getOrElse(x._1._1))+".gdm",
          x._1._2 + "\t" + x._1._3  + "\t" + x._1._4 + "\t" + x._1._5 + "\t" + x._2.mkString("\t"))}
        .partitionBy(regionsPartitioner)

    keyedRDD.saveAsHadoopFile(outputPath,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])

    regions
  }
}
