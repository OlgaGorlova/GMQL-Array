package it.polimi.genomics.array.implementation


import java.io.{BufferedWriter, OutputStreamWriter}
import java.util.concurrent.TimeUnit

import it.polimi.genomics.GMQLServer.Implementation
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GAttributes, GRegionKey}
import it.polimi.genomics.array.implementation.MetaOperators._
import it.polimi.genomics.array.implementation.MetaOperators.GroupOperator._
import it.polimi.genomics.array.implementation.RegionsOperators._
import it.polimi.genomics.array.implementation.RegionsOperators.SelectRegions._
import it.polimi.genomics.array.implementation._
import it.polimi.genomics.array.implementation.loaders.{CustomArrayParser, Export, Import}
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.Direction
import it.polimi.genomics.core.DataStructures.GroupMDParameters.TopParameter
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.MetaAggregate.{MetaAggregateFunction, MetaExtension}
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionExtension, RegionsToMeta}
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.loaders._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  */

object GMQLArrayExecutor{
  type GMQL_ARRAY = (Array[(GRegionKey, GAttributes)], Array[(Long, (String, String))], List[(String, PARSING_TYPE)])
}
class GMQLArrayExecutor(val binSize : BinSize = BinSize(),
                        sc: SparkContext,
                        outputFormat:GMQLSchemaFormat.Value = GMQLSchemaFormat.TAB,
                        outputCoordinateSystem: GMQLSchemaCoordinateSystem.Value = GMQLSchemaCoordinateSystem.Default,
                        stopContext:Boolean = true) extends Implementation with java.io.Serializable{
  final val logger = Logger.getLogger(this.getClass)
  final val ENCODING = "UTF-8"
  final val ms = System.currentTimeMillis();
  var regionDag: List[String] = List()
  var metaDag: List[String] = List()

  var fs:FileSystem = null


  def go()={
    logger.debug(to_be_materialized.toString())
    println(to_be_materialized.toString())
    val startTime = System.currentTimeMillis();
    implementation()
    println("Execution time: " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime) + " sec.")
  }

  override def take(iRVariable: IRVariable, n: Int) = {}
  override def collect(variable: IRVariable)= {}

  override def stop(): Unit = {
    sc.stop()
  }

  def getParser(name : String,dataset:String) : GMQLLoaderBase = {
    val path = ""

    object GencodeParser extends BedParser("\t", 0, 1, 2, Some(3),
      Some(Array((4, ParsingType.STRING),(5, ParsingType.STRING),(6, ParsingType.DOUBLE),(7, ParsingType.STRING),(8, ParsingType.STRING),(9, ParsingType.STRING),
        (10, ParsingType.STRING),(11, ParsingType.STRING),(12, ParsingType.STRING),(13, ParsingType.STRING),(14, ParsingType.STRING),(15, ParsingType.STRING),
        (16, ParsingType.STRING),(17, ParsingType.DOUBLE),(18, ParsingType.STRING),(19, ParsingType.DOUBLE),(20, ParsingType.STRING),(21, ParsingType.STRING),(22, ParsingType.STRING),
        (23, ParsingType.STRING),(24, ParsingType.STRING),(25, ParsingType.STRING),(26, ParsingType.STRING)))) {

      schema = List(("source", ParsingType.STRING),
        ("feature", ParsingType.STRING),
        ("score", ParsingType.DOUBLE),
        ("frame", ParsingType.STRING),
        ("gene_id", ParsingType.STRING),
        ("transcript_id", ParsingType.STRING),
        ("gene_type", ParsingType.STRING),
        ("gene_status", ParsingType.STRING),
        ("gene_name", ParsingType.STRING),
        ("entrez_gene_id", ParsingType.STRING),
        ("transcript_type", ParsingType.STRING),
        ("transcript_status", ParsingType.STRING),
        ("transcript_name", ParsingType.STRING),
        ("exon_number", ParsingType.DOUBLE),
        ("exon_id", ParsingType.STRING),
        ("level", ParsingType.DOUBLE),
        ("tag", ParsingType.STRING),
        ("ccdsid", ParsingType.STRING),
        ("havana_gene", ParsingType.STRING),
        ("havana_transcript", ParsingType.STRING),
        ("protein_id", ParsingType.STRING),
        ("ont", ParsingType.STRING),
        ("transcript_support_level", ParsingType.STRING)
      )
    }

    name.toLowerCase() match {
      case "bedscoreparser" => BedScoreParser
      case "annparser" => ANNParser
      case "broadpeaksparser" => BroadPeaksParser
      case "broadpeaks" => BroadPeaksParser
      case "narrowpeakparser" => NarrowPeakParser
      case "testorderparser" =>testOrder
      case "narrowpeak" => NarrowPeakParser
      case "narrow" => NarrowPeakParser
      case "bedtabseparatedparser" => BedParser
      case "rnaseqparser" => RnaSeqParser
      case "customparser" => (new CustomParser).setSchema(dataset)
      case "broadprojparser" => BroadProjParser
      case "basicparser" => BasicParser
      case "gencodeparser" => GencodeParser
      case "default" =>  (new CustomArrayParser).setSchema(dataset)
      case _ => {logger.warn("unable to find " + name + " parser, try the default one"); getParser("default",dataset)}
    }
  }

  def implement_md(ro : MetaOperator, sc: SparkContext ) : RDD[(Long, (String, String))] = {
    if(ro.intermediateResult.isDefined){
      ro.intermediateResult.get.asInstanceOf[RDD[(Long, (String, String))]]
    } else {
      metaDag = metaDag ++ List(ro.operatorName)
      val res =
        ro match {
          case IRStoreMD(path, value,_) => StoreMD(this, path, value, sc)
          case IRReadMD(path, loader,_) => ReadMD(path, loader, sc)
          case IRReadMEMMD(metaRDD) => ReadMEMMD(metaRDD)
          case IRPurgeMD(regionDataset, inputDataset) => PurgeMD(this, regionDataset, inputDataset, sc)
          case IRSelectMD(metaCondition, inputDataset) => SelectMD(this, metaCondition, inputDataset, sc)
          case IRSemiJoin(externalMeta: MetaOperator, joinCondition: MetaJoinCondition, inputDataset: MetaOperator) => SemiJoinMD(this, externalMeta, joinCondition, inputDataset, sc)
          case IRProjectMD(projectedAttributes: Option[List[String]], metaAggregator: Option[MetaExtension], all_but_flag:Boolean, inputDataset: MetaOperator) => ProjectMD(this, projectedAttributes, metaAggregator, all_but_flag, inputDataset, sc)
          case IRAggregateRD(aggregator: List[RegionsToMeta], inputDataset: RegionOperator) => ExtendRD(this, inputDataset, aggregator, sc)
          case IRCombineMD(grouping: OptionalMetaJoinOperator, leftDataset: MetaOperator, rightDataset: MetaOperator, region_builder, leftName : String, rightName : String) => CombineMD(this, grouping, leftDataset, rightDataset,region_builder, leftName, rightName, sc)
          case IRDiffCombineMD(grouping: OptionalMetaJoinOperator, leftDataset: MetaOperator, rightDataset: MetaOperator, leftName : String, rightName : String) => DiffCombineMD(this, grouping, leftDataset, rightDataset, leftName, rightName, sc)
          case IRUnionAggMD(leftDataset: MetaOperator, rightDataset: MetaOperator, leftName : String, rightName : String) => UnionAggMD(this, leftDataset, rightDataset, leftName, rightName, sc)
          case IRUnionMD(leftDataset: MetaOperator, rightDataset: MetaOperator, leftName : String, rightName : String) => UnionMD(this, leftDataset, rightDataset, leftName, rightName, sc)
          case IROrderMD(ordering: List[(String, Direction)], newAttribute: String, topParameter: TopParameter, inputDataset: MetaOperator) => OrderMD(this, ordering, newAttribute, topParameter, inputDataset, sc)
          case IRGroupMD(keys: MetaGroupByCondition, aggregates: Option[List[MetaAggregateFunction]], groupName: String, inputDataset: MetaOperator, region_dataset: RegionOperator) => GroupMD(this, keys, aggregates, groupName, inputDataset, region_dataset, sc)
          case IRMergeMD(dataset: MetaOperator, groups: Option[MetaGroupOperator]) => MergeMD(this, dataset, groups, sc)
          case IRCollapseMD(grouping : Option[MetaGroupOperator], inputDataset : MetaOperator) => CollapseMD(this, grouping, inputDataset, sc)
        }
      ro.intermediateResult = Some(res)
      res
    }
  }

  def implement_rd(ro : RegionOperator, sc: SparkContext ) : RDD[GARRAY] = {
    if(ro.intermediateResult.isDefined){
      ro.intermediateResult.get.asInstanceOf[RDD[GARRAY]]
    } else {
      regionDag = regionDag ++ List(ro.operatorName)
      val res =
        ro match {
          case IRStoreRD(path, value, meta, schema,_) => /*Export(this, value,path, sc)*/ StoreArrayRD(this, path, value, meta, schema, sc)
          case IRReadRD(path, loader,_) => /*Import(path.head, sc)*/ ReadRD(path, loader, sc)
          case IRReadMEMRD(metaRDD) => ReadMEMRD(metaRDD)
          case IRSelectRD(regionCondition: Option[RegionCondition], filteredMeta: Option[MetaOperator], inputDataset: RegionOperator) =>
            inputDataset match {
              case IRReadRD(path, loader,_) =>
                if(filteredMeta.isDefined) {
                  SelectIRD(this, regionCondition, filteredMeta, loader,path,None, sc)
                }
                else SelectRD(this, regionCondition, filteredMeta, inputDataset, sc)
              case _ => SelectRD(this, regionCondition, filteredMeta, inputDataset, sc)
            }
//          case IRSelectRD(regionCondition: Option[RegionCondition], filteredMeta: Option[MetaOperator], inputDataset: RegionOperator) => SelectRD(this, regionCondition, filteredMeta, inputDataset, sc)
          case IRPurgeRD(metaDataset: MetaOperator, inputDataset: RegionOperator) => PurgeRD(this, metaDataset, inputDataset, sc)
          case irCover:IRRegionCover => GenometricCover(this, irCover.cover_flag, irCover.min, irCover.max, irCover.input_dataset, binSize.Cover, irCover.aggregates, sc)
          case IRUnionRD(schemaReformatting: List[Int], leftDataset: RegionOperator, rightDataset: RegionOperator) => UnionRD(this, schemaReformatting, leftDataset, rightDataset, sc)
          case IRMergeRD(dataset: RegionOperator, groups: Option[MetaGroupOperator]) => MergeRD(this, dataset, sc)
          case IRGroupRD(groupingParameters: Option[List[GroupRDParameters.GroupingParameter]], aggregates: Option[List[RegionAggregate.RegionsToRegion]], regionDataset: RegionOperator) => GroupRD(this, groupingParameters, aggregates, regionDataset, sc)
          case IROrderRD(ordering: List[(Int, Direction)], topPar: TopParameter, inputDataset: RegionOperator) => OrderRD(this, ordering: List[(Int, Direction)], topPar, inputDataset, sc)
          case irJoin:IRGenometricJoin => GenometricJoin(this, irJoin.left_dataset, irJoin.right_dataset, irJoin.region_builder, None, binSize.Join, sc)
          case irMap:IRGenometricMap => GenometricMap(this, irMap.reference, irMap.samples,binSize.Map,sc)
          case IRDifferenceRD(metaJoin: OptionalMetaJoinOperator, leftDataset: RegionOperator, rightDataset: RegionOperator,exact:Boolean) => GenometricDifference(this, leftDataset, rightDataset, binSize.Map, exact, sc)
          case IRProjectRD(projectedValues: Option[List[Int]], tupleAggregator: Option[List[RegionExtension]], inputDataset: RegionOperator, inputDatasetMeta: MetaOperator) => ProjectRD(this, projectedValues, tupleAggregator, inputDataset, sc)
//          case _ => logger.warn("Operation is not supported!");
        }
      ro.intermediateResult = Some(res)
      res
    }
  }

  def get_dag(ro : RegionOperator, sc: SparkContext ) = {
//    println(ro.operatorName); get_dag(ro, sc)
  }

  @throws[SelectFormatException]
  def implement_mgd(mgo : MetaGroupOperator, sc : SparkContext) : RDD[(Long, Long)] ={
    if(mgo.intermediateResult.isDefined){
      mgo.intermediateResult.get.asInstanceOf[RDD[(Long, Long)]]
    } else {
      val res =
        mgo match {
          case IRGroupBy(groupAttributes: MetaGroupByCondition, inputDataset: MetaOperator) => MetaGroupMGD(this, groupAttributes, inputDataset, sc)
        }
      mgo.intermediateResult = Some(res)
      res
    }
  }

  @throws[SelectFormatException]
  def implement_mjd(mjo : OptionalMetaJoinOperator, sc : SparkContext) : RDD[(Long, Array[Long])] = {
    if(mjo.isInstanceOf[NoMetaJoinOperator]){
      if(mjo.asInstanceOf[NoMetaJoinOperator].operator.intermediateResult.isDefined)
        mjo.asInstanceOf[NoMetaJoinOperator].operator.intermediateResult.get.asInstanceOf[RDD[(Long, Array[Long])]]
      else {
        val res =
          mjo.asInstanceOf[NoMetaJoinOperator].operator match {
            case IRJoinBy(condition :  MetaJoinCondition, left_dataset : MetaOperator, right_dataset : MetaOperator) => MetaJoinMJD2(this, condition, left_dataset, right_dataset,true, sc)
          }
        mjo.asInstanceOf[NoMetaJoinOperator].operator.intermediateResult = Some(res)
        res
      }
    }else {
      if(mjo.asInstanceOf[SomeMetaJoinOperator].operator.intermediateResult.isDefined)
        mjo.asInstanceOf[SomeMetaJoinOperator].operator.intermediateResult.get.asInstanceOf[RDD[(Long, Array[Long])]]
      else {
        val res =
          mjo.asInstanceOf[SomeMetaJoinOperator].operator match {
            case IRJoinBy(condition: MetaJoinCondition, leftDataset: MetaOperator, rightDataset: MetaOperator) => MetaJoinMJD2(this, condition, leftDataset, rightDataset,false, sc)
          }
        mjo.asInstanceOf[SomeMetaJoinOperator].operator.intermediateResult = Some(res)
        res
      }
    }
  }

  def implementation(): Unit = {

    try {
      for (variable <- to_be_materialized) {



        val metaRDD = implement_md(variable.metaDag, sc)
        val regionRDD = Export.toRow(implement_rd(variable.regionDag, sc))
//        val regionRDD = implement_rd(variable.regionDag, sc)

        val variableDir = variable.metaDag.asInstanceOf[IRStoreMD].path.toString
        val MetaOutputPath =  variableDir + "/meta/"
        val RegionOutputPath = variableDir + "/files/"
        logger.debug("meta out: "+MetaOutputPath)
        logger.debug("region out "+ RegionOutputPath)


        val outputFolderName= try{
          new Path(variableDir).getName
        }
        catch{
          case _:Throwable => variableDir
        }

        val conf = new Configuration();
        val path = new org.apache.hadoop.fs.Path(RegionOutputPath);
        fs = FileSystem.get(path.toUri(), conf);

//        if(testingIOFormats){
//          metaRDD.map(x=>x._1+","+x._2._1 + "," + x._2._2).saveAsTextFile(MetaOutputPath)
//          regionRDD.map(x=>x._1+"\t"+x._2.mkString("\t")).saveAsTextFile(RegionOutputPath)
//        }

        // store schema
        storeSchema(GMQLSchema.generateSchemaXML(variable.schema,outputFolderName,outputFormat, outputCoordinateSystem),variableDir)

//        println("Regions count: " + regionRDD.count())
//        regionRDD.collect().foreach(e=> println(e._1.toString() + "\t" + e._2.mkString("\t")))

        println("DAG:")
        println(metaDag)
        println(regionDag)

//        get_dag(variable.regionDag, sc)


//        println("Meta count: " + metaRDD.count())
//        metaRDD.collect().foreach(e=> println(e))

        fs.deleteOnExit(new Path(RegionOutputPath+"_SUCCESS"))

        fs.setVerifyChecksum(false)
        fs.listStatus(new Path(RegionOutputPath),new PathFilter {
          override def accept(path: Path): Boolean = {fs.delete(new Path(path.getParent.toString + "/."+path.getName +".crc"));true}
        })

      }
    } catch {
      case e : SelectFormatException => {
        logger.error(e.getMessage)
        e.printStackTrace()
        throw e
      }
      case e : Throwable => {
        logger.error(e.getMessage)
        e.printStackTrace()
        throw e
      }
    } finally {
      if (stopContext) {
        sc.stop()
      }
      // We need to clear the set of materialized variables if we are in interactive mode
      to_be_materialized.clear()
      logger.debug("Total Spark Job Execution Time : "+(System.currentTimeMillis()-ms).toString)
    }

  }

  //Other usefull methods

  def castDoubleOrString(value : Any) : Any = {
    try{
      value.toString.toDouble
    } catch {
      case e : Throwable => value.toString
    }
  }

  def storeSchema(schema: String, path : String)= {
    val schemaPath = path+"/files/schema.xml"
    val br = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(schemaPath)), "UTF-8"));
    br.write(schema)
    br.close()
  }
}
