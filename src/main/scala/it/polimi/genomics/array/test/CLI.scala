package it.polimi.genomics.array.test

import java.io._

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.array.utilities.ArrayKryoRegistrator
import it.polimi.genomics.compiler.{CompilerException, Translator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Olga Gorlova on 08/11/2019.
  */
object CLI {

  import org.slf4j.LoggerFactory

  private val logger = LoggerFactory.getLogger(CLI.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("CLI")
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

    val runner = new GMQLArrayExecutor(sc=sc, stopContext = false)
    val server = new GmqlServer(runner)

    var scriptPath: String = null

    val sFile = new File(args(0))
    if (!sFile.exists()) {
      logger.error(s"Script file not found $scriptPath")
    }
    scriptPath = sFile.getPath
    logger.debug("scriptpath set to: " + scriptPath)

    val script = readScriptFile(scriptPath)

    val translator1 = new Translator(server, "")

    val test_double_select = ""
    try {
      if (translator1.phase2(translator1.phase1(script))) {
        server.run()
        //server.getDotGraph()
      }
    } catch {
      case e: CompilerException => println(e.getMessage)
    }

    println("\n\nQuery" +"\n" + script + "\n\n")
  }

  def readScriptFile(file: String): String = {
    try {
      scala.io.Source.fromFile(file).mkString
    } catch {
      case ex: Throwable => logger.warn("File not found"); "NOT FOUND SCRIPT FILE."
    }
  }

}
