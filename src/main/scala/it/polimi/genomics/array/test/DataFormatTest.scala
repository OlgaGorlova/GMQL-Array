package it.polimi.genomics.array.test

import com.google.common.hash.Hashing
import it.polimi.genomics.array.DataTypes.{GAttributes, GRegionKey}
import it.polimi.genomics.core._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Olga Gorlova on 25/10/2019.
  */
object DataFormatTest {

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


    val path = "/Users/olha/WORK/BENCHMARKS/DataFormat/array_t2"
    var schema: Array[(String,String)] = Array()
    val input = sc.textFile("/Users/olha/WORK/BENCHMARKS/DataFormat/array_t2")
    val sch = input.filter(_.startsWith("#")).collect().map{v=>
      val line = v.substring(1, v.length-1).split("__")
      line.foreach{l=>
        val values = l.split("=")
        schema = schema:+(values(0),values(1))
      }
    }

    val parse = input.filter(_.startsWith(">")).flatMap{x=>
      if (x.startsWith(">")) {
        val line = x.substring(1, x.length-1).split(";").toIterator
        val coord = line.next().split("__")
        val ids = line.next().split("__").map{ v =>
          val spl = v.split(":")
          (Hashing.md5().newHasher().putString(path+"%"+spl.head).hash().asLong(), spl.last.toInt)
        }

        val att = (for (i <- 0 until schema.size) yield {
          val nextAtt: Array[Array[GValue]] = line.next().split("__").map{ a=>
            val tt: Array[String] = a.split(":")
            val ss: Array[GValue] = tt.map{ t=>
            schema(i)._2.toLowerCase() match {
              case "int" => GInt(t.toInt)
              case "double" => GDouble(t.toDouble)
              case "string" => GString(t)
              case "long" => GInt(t.toInt)
              case "null" => GNull()
              case _ => GNull()
            }
          }
            ss
          }
          nextAtt
        }).toArray

        Some(new GRegionKey(coord(0), coord(1).toLong,coord(2).toLong, coord(3).charAt(0)), new GAttributes(ids, att))
      }
      else None
    }


    parse.count()
    parse.foreach(println(_))
    println(schema.mkString("\t"))
    val samplesMap = parse.map(_._2._1.map(s=>s._1).toList).flatMap(identity).distinct().zipWithIndex().map{s=>
      val newName = f"S_${s._2}%02d"
      s._1 -> newName
    }.collectAsMap()

    val toSave = parse.map{g=>
      val coord = g._1.chrom + "__" + g._1.start + "__" + g._1.stop + "__" + g._1.strand
      val ids = g._2._1.map(i=> samplesMap(i._1)+":"+i._2).mkString("__")
      val att = g._2._2.map(a=> a.map(s=>s.mkString(":")).mkString("__")).mkString(";")
      ">"+coord+";"+ids+";"+att
    }

    val schemaToSave = sc.parallelize(Seq("#"+schema.map(s=> s._1+"="+s._2).mkString("__")+";"))

    schemaToSave.union(toSave).foreach(println(_))

  }

}
