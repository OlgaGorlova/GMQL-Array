package it.polimi.genomics.array.test

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.compiler.{CompilerException, Translator}
import org.apache.spark.{SparkConf, SparkContext}
import it.polimi.genomics.array.implementation._
import it.polimi.genomics.array.utilities.ArrayKryoRegistrator


/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object TestEngine {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
      .setAppName("test Graph")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "128")
      .set("spark.kryo.registrator", classOf[ArrayKryoRegistrator].getName)
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.executor.heartbeatInterval", "2000s")
      .set("spark.network.timeout", "10000000")
//      .set("spark.eventLog.enabled", "true")

    var sc: SparkContext = new SparkContext(conf)

    val runner = new GMQLArrayExecutor(sc=sc, stopContext = false)
    val server = new GmqlServer(runner)

    //    val input = "/Users/olha/IdeaProjects/MultiDimensional-DMS/src/main/resources/ref"
    //    val s: IRVariable = server READ input USING (new CustomParser().setSchema(input))
    //
    //    val select = s.SELECT(Predicate(2, REG_OP.GT, 0))
    //
    //    val extend = select.EXTEND(List(DefaultRegionsToMetaFactory.get("COUNT", Some("num_reg"))))
    //
    //    server setOutputPath "" MATERIALIZE extend
    //    server.run()

    val testQuery = "DATASET = SELECT(region: signal >= 3) /Users/olha/IdeaProjects/MultiDimensional-DMS/src/main/resources/ref;\n" +
      "RESULT = EXTEND(region_count AS COUNT()) DATASET;\n" +
      "MATERIALIZE RESULT INTO RESULT;"

    val testUnion = "A = SELECT() /Users/olha/IdeaProjects/MultiDimensional-DMS/src/main/resources/ref/;\n" +
      "B = SELECT() /Users/olha/IdeaProjects/MultiDimensional-DMS/src/main/resources/exp/;\n"+
      "RESULT = UNION() A B;\n" +
      "MATERIALIZE RESULT INTO /Users/olha/IdeaProjects/MultiDimensional-DMS/src/main/resources/compiler/;"

    val testGroup = "A = SELECT() /Users/olha/IdeaProjects/MultiDimensional-DMS/src/main/resources/ref/;\n" +
      "RESULT = GROUP() A;\n" +
      "MATERIALIZE RESULT INTO /Users/olha/IdeaProjects/MultiDimensional-DMS/src/main/resources/compiler/;"

    val testDataFormat = "A = SELECT(age > 25; region: att1 > 5) /Users/olha/WORK/BENCHMARKS/DataFormat/small_160MB_3att_5samples/files;\n" +
      "B = PROJECT(att1, att3) A;\n" +
      "C = ORDER(region_order: att1) B;\n" +
      "MATERIALIZE C INTO /Users/olha/WORK/BENCHMARKS/DataFormat/small_res_order/;"

    val translator1 = new Translator(server, "")

    val test_double_select = ""
    try {
      if (translator1.phase2(translator1.phase1(testDataFormat))) {
        server.run()
        //server.getDotGraph()
      }
    } catch {
      case e: CompilerException => println(e.getMessage)
    }

    //    val server2 = new GmqlServer(new GMQLSparkExecutor(sc=sc))
    //    val translator2 = new Translator(server2, "")
    ////    val test_double_select = ""
    //    try {
    //      if (translator2.phase2(translator2.phase1(testUnion))) {
    //        server2.run()
    //        //server.getDotGraph()
    //      }
    //    } catch {
    //      case e: CompilerException => println(e.getMessage)
    //    }

    println("\n\nQuery" +"\n" + testDataFormat + "\n\n")

  }

}
