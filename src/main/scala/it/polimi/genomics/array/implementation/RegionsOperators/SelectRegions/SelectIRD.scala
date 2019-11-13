package it.polimi.genomics.array.implementation.RegionsOperators.SelectRegions

import java.nio.charset.StandardCharsets

import com.google.common.hash.Hashing
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GAttributes, GRegionKey}
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.array.implementation.RegionsOperators.SelectRegions.PredicateRD
import it.polimi.genomics.array.implementation.RegionsOperators.SelectRegions.PredicateRD._
import it.polimi.genomics.array.implementation.loaders.ArrayLoaders._
import it.polimi.genomics.core
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataStructures.RegionCondition._
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.GMQLLoader
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import org.apache.commons.io.FilenameUtils


/**
  * Created by Olga Gorlova on 28/10/2019.
  */
object SelectIRD {


  private final val logger = LoggerFactory.getLogger(this.getClass);
  var executor: GMQLArrayExecutor = null

  @throws[SelectFormatException]
  def apply(executor: GMQLArrayExecutor, regionCondition: Option[RegionCondition], filteredMeta: Option[MetaOperator], loader: GMQLLoader[Any, Any, Any, Any], URIs: List[String], repo: Option[String], sc: SparkContext): RDD[GARRAY] = {
    PredicateRD.executor = executor
    val optimized_reg_cond = if (regionCondition.isDefined) Some(PredicateRD.optimizeConditionTree(regionCondition.get, false, filteredMeta, sc))
    else {
      None
    }
    logger.info("----------------SelectIRD ")

    val conf = new Configuration();
    val path = new org.apache.hadoop.fs.Path(URIs.head);
    val fs = FileSystem.get(path.toUri(), conf);

    val files: List[String] =

      URIs.flatMap { dirInput =>
        val uri = new Path(dirInput)
        if (fs.isDirectory(uri)) {
          fs.listStatus(new Path(dirInput), new PathFilter {
            override def accept(path: Path): Boolean = if (path.toString.endsWith(".meta")) fs.exists(new Path(path.toString)) else false
          }).map(x => x.getPath.toString).toList;
        } else if (fs.exists(uri)) List(dirInput)
        else None
      }

    val inputURIs = files.map { x =>
      val uri = x //.substring(x.indexOf(":") + 1, x.size).replaceAll("/", "");
      val name = FilenameUtils.removeExtension(new Path(uri).getName)
      val toHash = name.replaceAll("/","")
      Hashing.md5().newHasher().putString(toHash, StandardCharsets.UTF_8).hash().asLong() -> x
    }.toMap
    val metaIdList = executor.implement_md(filteredMeta.get, sc).keys.distinct.collect
    val selectedURIs: Array[String] = metaIdList.map{ x =>
      FilenameUtils.removeExtension(new Path(inputURIs.get(x).get).getName)
//      inputURIs.get(x).get
    }

    def parser (x: (Option[Array[String]], String)) = loader.asInstanceOf[GMQLLoader[(Option[Array[String]], String), Option[GARRAY], (Long, String), Option[MetaType]]].region_parser(x)


    val applyRegionSelect: (RegionCondition, GARRAY) => Option[GARRAY] = (regionCondition: RegionCondition, region: GARRAY) => {
      regionCondition match {
        case attributePredicate: Predicate =>{
          val flags: Array[Array[Boolean]] = PredicateRD.applyAttributePredicate(attributePredicate.operator, attributePredicate.value, region._2._2(attributePredicate.position))
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
        case _ => if (PredicateRD.applyRegionSelect(regionCondition, region)) Some(region) else None
      }
    }

    if (selectedURIs.size > 0)
      sc forPath (path.toUri.getPath) LoadRegionsCombineFiles(parser, PredicateRD.applyRegionCondition  , optimized_reg_cond, Some(selectedURIs.mkString(";"))) cache
    else {
      logger.warn("One input select is empty..")
      sc.emptyRDD[GARRAY]
    }
  }


}
