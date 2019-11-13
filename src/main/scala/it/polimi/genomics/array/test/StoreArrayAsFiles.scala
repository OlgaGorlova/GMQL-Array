package it.polimi.genomics.array.test

import it.polimi.genomics.array.DataTypes.writeMultiOutputFiles
import it.polimi.genomics.array.DataTypes.writeMultiOutputFiles.RDDMultipleTextOutputFormat
import it.polimi.genomics.array.implementation.RegionsOperators.SelectRegions.StoreArrayRD.logger
import it.polimi.genomics.array.implementation.loaders.Import
import it.polimi.genomics.array.utilities.ArrayKryoRegistrator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by Olga Gorlova on 08/11/2019.
  */
object StoreArrayAsFiles {

  private val logger = LoggerFactory.getLogger(CLI.getClass)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("StoreArrayAsFiles")
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "128")
      .set("spark.kryo.registrator", classOf[ArrayKryoRegistrator].getName)
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.executor.heartbeatInterval", "2000s")
      .set("spark.network.timeout", "10000000")
    //      .set("spark.eventLog.enabled", "true")

    val sc: SparkContext = new SparkContext(conf)
    val inputPath = args(0)
    val outputPath = args(1)

    val ds = Import(inputPath, sc)


    val confHadoop = new Configuration();
    val dfsPath = new org.apache.hadoop.fs.Path(outputPath);
    val fs = FileSystem.get(dfsPath.toUri(), confHadoop);

    val RegionOutputPath = outputPath


    val outputFolderName= try{
      new Path(outputPath).getName
    }
    catch{
      case _:Throwable => outputPath
    }

    val outSample = "S"

    val ids = ds.flatMap(_._2.samples.map(_._1)).distinct().collect().sorted
    val newIDS = ids.zipWithIndex.toMap
    val newIDSbroad = sc.broadcast(newIDS)

    val regionsPartitioner = new HashPartitioner(ids.length)

    val regionsToStore = ds.map{g=>
      val coord = g._1.chrom + "__" + g._1.start + "__" + g._1.stop + "__" + g._1.strand
      val ids = g._2._1.map(i=> outSample+"_"+ "%05d".format(newIDSbroad.value.get(i._1).get)+":"+i._2).mkString("__")
      val att = g._2._2.map(a=> a.map(s=>s.mkString(":")).mkString("__")).mkString(";")
      ("regions.mgd",">"+coord+";"+ids+";"+att)
    }.partitionBy(regionsPartitioner)

    regionsToStore.saveAsHadoopFile(RegionOutputPath,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])


    fs.deleteOnExit(new Path(RegionOutputPath+"_SUCCESS"))

    fs.setVerifyChecksum(false)
    fs.listStatus(new Path(RegionOutputPath),new PathFilter {
      override def accept(path: Path): Boolean = {fs.delete(new Path(path.getParent.toString + "/."+path.getName +".crc"));true}
    })
  }

}
