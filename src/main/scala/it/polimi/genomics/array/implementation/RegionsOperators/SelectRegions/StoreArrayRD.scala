package it.polimi.genomics.array.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.writeMultiOutputFiles
import it.polimi.genomics.array.DataTypes.writeMultiOutputFiles.RDDMultipleTextOutputFormat
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.GMQLSchemaCoordinateSystem
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by Olga Gorlova on 29/10/2019.
  */
object StoreArrayRD {

  private final val logger = LoggerFactory.getLogger(StoreArrayRD.getClass);
  private final val ENCODING = "UTF-8"

  @throws[SelectFormatException]
  def apply(executor: GMQLArrayExecutor, path: String, value: RegionOperator, associatedMeta:MetaOperator, schema : List[(String, PARSING_TYPE)], sc: SparkContext): RDD[GARRAY] = {
    val regions = executor.implement_rd(value, sc)
    val meta = executor.implement_md(associatedMeta,sc)

    val conf = new Configuration();
    val dfsPath = new org.apache.hadoop.fs.Path(path);
    val fs = FileSystem.get(dfsPath.toUri(), conf);

    val MetaOutputPath = path + "/meta/"
    val RegionOutputPath = path + "/files/"

    logger.debug(MetaOutputPath)
    logger.debug(RegionOutputPath)
    logger.debug(regions.toDebugString)
    logger.debug(meta.toDebugString)

    val outputFolderName= try{
      new Path(path).getName
    }
    catch{
      case _:Throwable => path
    }

    val outSample = "S"

    val ids = meta.keys.distinct().collect().sorted
    val newIDS = ids.zipWithIndex.toMap
    val newIDSbroad = sc.broadcast(newIDS)

    val regionsPartitioner = new HashPartitioner(ids.length)

    val regionsToStore = regions.map{g=>
      val coord = g._1.chrom + "__" + g._1.start + "__" + g._1.stop + "__" + g._1.strand
      val ids = g._2._1.map(i=> outSample+"_"+ "%05d".format(newIDSbroad.value.get(i._1).get)+":"+i._2).mkString("__")
      val att = g._2._2.map(a=> a.map(s=>s.mkString(":")).mkString("__")).mkString(";")
      ("regions.mgd",">"+coord+";"+ids+";"+att)
    }.partitionBy(regionsPartitioner)


    //    val keyedRDDR = if(Ids.count == regions.partitions) keyedRDD.sortBy{s=>val data = s._2.split("\t"); (data(0),data(3).toLong,data(4).toLong)} else keyedRDD
    //    writeMultiOutputFiles.saveAsMultipleTextFiles(keyedRDD, RegionOutputPath)
    regionsToStore.saveAsHadoopFile(RegionOutputPath,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])

    val metaKeyValue = {
      meta.sortBy(x=>(x._1,x._2)).map(x => (outSample+"_"+ "%05d".format(newIDSbroad.value.get(x._1).get) + ".meta", x._2._1 + "\t" + x._2._2)).partitionBy(regionsPartitioner)
    }

    //    val metaKeyValueR  = if(Ids.count == meta.partitions) metaKeyValue.sortBy(_._2) else metaKeyValue

    //    writeMultiOutputFiles.saveAsMultipleTextFiles(metaKeyValue, MetaOutputPath)
    metaKeyValue.saveAsHadoopFile(MetaOutputPath,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])

    writeMultiOutputFiles.fixOutputMetaLocation(MetaOutputPath)

//    writeMultiOutputFiles.removeExtraFiles(RegionOutputPath)

    regions
  }

}
