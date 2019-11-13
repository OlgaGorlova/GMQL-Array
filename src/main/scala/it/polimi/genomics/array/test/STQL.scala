package it.polimi.genomics.array.test

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.array.implementation.GMQLArrayExecutor.GMQL_ARRAY
import it.polimi.genomics.array.implementation.RegionsOperators.{GenometricMap, GroupRD, SelectRD}
import it.polimi.genomics.array.implementation.loaders.Import
import it.polimi.genomics.array.utilities.ArrayKryoRegistrator
import it.polimi.genomics.compiler.{CompilerException, Translator}
import it.polimi.genomics.core.DataStructures.RegionCondition.{Predicate, REG_OP}
import it.polimi.genomics.spark.implementation.loaders.CustomParser
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Olga Gorlova on 22/10/2019.
  */
object STQL {

  var sc: SparkContext = _

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("STQL")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "128")
//      .set("spark.kryo.registrator", classOf[ArrayKryoRegistrator].getName)
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.executor.heartbeatInterval", "2000s")
      .set("spark.network.timeout", "10000000")
      .set("spark.executor.extraClassPath", "file://Users/olha/IdeaProjects/GMQL-Array/target/uber-GMQL-Array-2.0-SNAPSHOT.jar")

    sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

//    sc.addJar("./target/uber-GMQL-Array-2.0-SNAPSHOT.jar")


    val runner = new GMQLArrayExecutor(sc=sc, stopContext = false)
    val server = new GmqlServer(runner)

    val mainPath = if (args.isEmpty) "/Users/olha/WORK/BENCHMARKS/STQL/SQ4/" else args(0)
    val query = if (args.isEmpty) "SQ4" else args(1)
    val alg = if (args.isEmpty) "array" else args(2)

    val sq1 = "HIS = SELECT() /Users/olha/WORK/BENCHMARKS/STQL/SQ1/Obj/ENCODE;\n" +
      "BIN = SELECT() /Users/olha/WORK/BENCHMARKS/STQL/SQ1/Obj/BIN;\n" +
      "BIN_HIS_MAP = MAP() BIN HIS;\n" +
      "RESULT = SELECT(region: count_BIN_HIS > 0) BIN_HIS_MAP;\n" +
      "MATERIALIZE RESULT INTO /Users/olha/WORK/BENCHMARKS/STQL/SQ1/Obj/RES;"

    val sq2 = "CSHL = SELECT(parser: BedScoreParser) /Users/olha/WORK/BENCHMARKS/STQL/SQ2/Obj/ENCODE2/;\n" +
      "GENE = SELECT(parser: GencodeParser) /Users/olha/WORK/BENCHMARKS/STQL/SQ2/Obj/GENCODE2/;\n" +
      "GENE_DISTICT = GROUP() GENE;\n" +
      "CSHL_MAP = MAP() GENE_DISTICT CSHL;\n" +
      "RESULT = SELECT(region: count_GENE_DISTICT_CSHL > 0) CSHL_MAP;\n" +
      "MATERIALIZE RESULT INTO /Users/olha/WORK/BENCHMARKS/STQL/SQ2/Obj/RES;"

    val sq3 = "H3K4me1 = SELECT() /Users/olha/WORK/BENCHMARKS/STQL/SQ3/H3K4me1;\n" +
      "H3K27ac = SELECT() /Users/olha/WORK/BENCHMARKS/STQL/SQ3/H3K27ac;\n" +
      "RESULT = JOIN(DISTANCE < 0; output: INT) H3K4me1 H3K27ac; \n" +
      "MATERIALIZE RESULT INTO /Users/olha/WORK/BENCHMARKS/STQL/SQ3/Obj/RES;"

    val sq4 = "CSHL = SELECT() /Users/olha/WORK/BENCHMARKS/STQL/SQ4/ENCODE;\n" +
      "GENE = SELECT() /Users/olha/WORK/BENCHMARKS/STQL/SQ4/GENCODE;\n" +
      "CSHL1 = COVER(1,ANY) CSHL;\n" +
      "U1 = UNION() CSHL1 CSHL1;\n" +
      "GENE1 = COVER(1,ANY) GENE;\n" +
      "U = UNION() U1 GENE1;\n" +
      "RESULT = COVER(2,2) U;\n" +
      "MATERIALIZE RESULT INTO /Users/olha/WORK/BENCHMARKS/STQL/SQ4/Obj/RES;"


//    val translator1 = new Translator(server, "")
//
//    val test_double_select = ""
//    try {
//      if (translator1.phase2(translator1.phase1(sq2))) {
//        server.run()
//        //server.getDotGraph()
//      }
//    } catch {
//      case e: CompilerException => println(e.getMessage)
//    }
//    println("\n\nQuery" +"\n" + sq2 + "\n\n")


//    val path = "/Users/olha/WORK/BENCHMARKS/STQL/SQ2/"
//    val startTime = System.currentTimeMillis()
//    val cshl = server.READ(path + "Obj/ENCODE").USING (new CustomParser().setSchema(path + "Obj/ENCODE"))
//    val gene = server.READ(path + "Obj/GENCODE").USING (new CustomParser().setSchema(path + "Obj/GENCODE"))
//
//    val gene_distinct = gene.GROUP(None, None, "_group", None, None)
//    val cshl_map = gene_distinct.MAP(None,List(),cshl, None, None, None)
//
//    val res = cshl_map.SELECT(Predicate(0, REG_OP.GT, 0))
//
//    val output = server setOutputPath path+"res" COLLECT res
//    server.run()
//
//    println("SQ2: " + output.asInstanceOf[GMQL_ARRAY]._1.length)
//    println("Execution time for SQ2 array-based: " + (System.currentTimeMillis() - startTime) / 1000)


    Import("/Users/olha/WORK/BENCHMARKS/STQL/SQ2/ENCODE/", sc).saveAsObjectFile("/Users/olha/WORK/BENCHMARKS/STQL/SQ2/Obj/ENCODE2")
    Import("/Users/olha/WORK/BENCHMARKS/STQL/SQ2/GENCODE/", sc).saveAsObjectFile("/Users/olha/WORK/BENCHMARKS/STQL/SQ2/Obj/GENCODE2")

  }


}
