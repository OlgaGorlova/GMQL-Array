package it.polimi.genomics.array.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.implementation.RegionsOperators.SelectRD
import it.polimi.genomics.array.implementation.loaders.ArrayLoaders._
import it.polimi.genomics.core.DataTypes.{MetaType}
import it.polimi.genomics.core.GMQLLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by Olga Gorlova on 29/10/2019.
  */
object ReadRD {

  private final val logger = LoggerFactory.getLogger(SelectRD.getClass);

  def apply(paths: List[String], loader: GMQLLoader[Any, Any, Any, Any], sc: SparkContext): RDD[GARRAY] = {
//    def parser(x: (Long, String)) = loader.asInstanceOf[GMQLLoader[(Long, String), Option[GARRAY], (Long, String), Option[MetaType]]].region_parser(x)

    def parser (x: (Option[Array[String]], String)) = loader.asInstanceOf[GMQLLoader[(Option[Array[String]], String), Option[GARRAY], (Long, String), Option[MetaType]]].region_parser(x)

    val conf = new Configuration();
    val path = new org.apache.hadoop.fs.Path(paths.head);
    val fs = FileSystem.get(path.toUri(), conf);

    var files = paths.flatMap { dirInput =>
      val file = new Path(dirInput)
      if (fs.isDirectory(file))
        fs.listStatus(file, new PathFilter {
          override def accept(path: Path): Boolean = if (path.toString.endsWith(".meta")) fs.exists(new Path(path.toString)) else false
        }).map(x => x.getPath.toString).toList;
      else List(dirInput)
    }.filter(x=> !x.endsWith(".schema") || !x.endsWith(".xml"))

    sc.forPath(path.toUri.getPath).LoadRegionsCombineFiles(parser)

  }
}
