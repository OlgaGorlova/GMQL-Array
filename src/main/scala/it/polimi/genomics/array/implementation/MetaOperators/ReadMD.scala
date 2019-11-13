package it.polimi.genomics.array.implementation.MetaOperators

import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.{DataTypes, GMQLLoader}
import it.polimi.genomics.spark.implementation.loaders.Loaders
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object ReadMD {
  private final val logger = Logger.getLogger(this.getClass);

  def apply(paths: List[String], loader: GMQLLoader[Any, Any, Any, Any], sc : SparkContext) : RDD[MetaType] = {
    logger.info("----------------ReadMD executing..")

    def parser(x: (Long, String)) =
      loader
        .asInstanceOf[GMQLLoader[(Long, String), Option[DataTypes.GRECORD], (Long, String), Option[DataTypes.MetaType]]]
        .meta_parser(x)

    val conf = new Configuration()
    val path = new Path(paths.head)
    val fs = FileSystem.get(path.toUri(), conf)

    var files =
      paths.flatMap { dirInput =>
        val file = new Path(dirInput)
        if (fs.isDirectory(file))
          fs.listStatus(file,new PathFilter {
            override def accept(path: Path): Boolean = if (path.toString.endsWith(".meta")) fs.exists(new Path(path.toString)) else false
          }).map(x => x.getPath.toString).toList
        else List(dirInput)
      }

    val metaPath = files.map(x=>x).mkString(",")
    Loaders forPath(sc, metaPath) LoadMetaCombineFiles (parser)

//    val meta: RDD[(Long, (String, String))] = sc.parallelize(Seq(
//      (1l, ("bla", "1")),
//      (1l, ("filename", "1l")),
//      (2l, ("bla", "1")),
//      (2l, ("filename", "2l")),
//      (3l, ("bla", "3")),
//      (3l, ("filename", "3l"))
//    ))

//    meta
  }

}
