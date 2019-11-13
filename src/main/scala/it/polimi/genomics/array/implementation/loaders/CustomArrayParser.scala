package it.polimi.genomics.array.implementation.loaders

import java.io.InputStream

import com.google.common.hash.Hashing
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GAttributes, GRegionKey}
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.exception.ParsingException
import it.polimi.genomics.core._
import it.polimi.genomics.array.implementation.loaders.ArrayParserHelper.parseRegion
import it.polimi.genomics.spark.utilities.FSConfig
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

/**
  * Created by Olga Gorlova on 25/10/2019.
  */

object ArrayParserHelper {
  /**
    * returns parsed data as GValue by using parsingType and the value.
    *
    * @param parsingType
    * @param value
    * @return
    */
  def parseRegion(parsingType: ParsingType.Value, value: String): GValue = {
    parsingType match {
      case ParsingType.STRING | ParsingType.CHAR => GString(value.trim)
      case ParsingType.INTEGER | ParsingType.LONG | ParsingType.DOUBLE =>
        value.toLowerCase match {
          case "null" | "." =>
            GNull()
          case _ =>
            //No need to trim
            GDouble(value.toDouble)
        }
      case ParsingType.NULL => GNull()
      case _ => throw new Exception("Unknown ParsingType")
    }
  }

}

/**
  * GMQL Array Parser, it is a parser that parse array-based text files,
  *
  * @param schemaNames  [[Array]] of the attributes names, this is [[Option]] and can be [[None]]. The Array has tuple of (names as [[String]],[[ParsingType]])
  */
class ArrayParser(metaDelimiter: String, schemaNames: Option[Array[(String, ParsingType.PARSING_TYPE)]]) extends GMQLLoader[(Option[Array[String]], String), Option[GARRAY], (Long, String), Option[DataTypes.MetaType]] with java.io.Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[ArrayParser])
  var parsingType: GMQLSchemaFormat.Value = GMQLSchemaFormat.TAB
  var coordinateSystem: GMQLSchemaCoordinateSystem.Value = GMQLSchemaCoordinateSystem.ZeroBased
  final val spaceDelimiter: String = " "
  final val semiCommaDelimiter: String = ";"
  final val underscoreDelimiter: String = "__"
  final val colonDelimiter: String = ":"
  final val newRegionDelimiter: String = ">"
  final val schemaDelimiter: String = "#"


  @transient
  private lazy val otherGtf: Map[String, (PARSING_TYPE, Int)] =
    schema
      .zipWithIndex
      .map(x => x._1._1.toUpperCase -> (x._1._2, x._2))
      .toMap

  @transient
  private lazy val otherGtfSize: Int = otherGtf.size

  @deprecated
  def calculateMapParameters(namePosition: Option[Seq[String]] = None): Unit = {}

  /**
    * Meta Data Parser to parse String to GMQL META TYPE (ATT, VALUE)
    *
    * @param t
    * @return
    */
  @throws(classOf[ParsingException])
  override def meta_parser(t: (Long, String)): Option[DataTypes.MetaType] = {
    val s = t._2.split(metaDelimiter, -1)

    if (s.length != 2 && s.length != 0 && !s.startsWith("#")) {
      throw ParsingException.create("The following metadata entry is not in the correct format: \n[" + t._2 + "]\nCheck the spacing.")
    } else if (s.length == 0) {
      None
    } else {
      Some((t._1, (s(0), s(1))))
    }
  }

  /**
    * Parser of String to GMQL Spark GRECORD
    *
    * @param inputLine [[String]] line to be parsed.
    * @return [[GARRAY]] as GMQL record representation.
    */
  @throws(classOf[ParsingException])
  override def region_parser(inputLine: (Option[Array[String]], String)): Option[GARRAY] = {
    import ArrayParserHelper._
    try {

      val metaIds = inputLine._1.getOrElse(Array())
      var metaNames: Array[Int] = Array()
      if (inputLine._2.startsWith(newRegionDelimiter)) {
        val line = inputLine._2.substring(1, inputLine._2.length - 1).split(semiCommaDelimiter).toIterator
        val coord = line.next().split(underscoreDelimiter)
        var i = -1
        val ids = line.next().split(underscoreDelimiter).flatMap { v =>
          i += 1
          val spl = v.split(colonDelimiter)
          if (metaIds.contains(spl.head)) {
            val toHash = spl.head.replaceAll("/", "")
            Some(Hashing.md5().newHasher().putString(toHash,java.nio.charset.StandardCharsets.UTF_8).hash().asLong(), spl.last.toInt)
          }
          else {
            metaNames = metaNames :+ i
            None
          }
        }


        val att: Array[Array[Array[GValue]]] = (for (i <- 0 until schema.size) yield {
          var j = -1
          line.next().split(underscoreDelimiter).flatMap{a =>
            j += 1;
            if (!metaNames.contains(j))
              Some(a.split(colonDelimiter).map(t=>parseRegion(schema(i)._2, t)))
            else None}
        }).toArray

        Some(new GRegionKey(coord(0), coord(1).toLong, coord(2).toLong, coord(3).charAt(0)), new GAttributes(ids, att))
      }
      else None

    }
    catch {
      case e: Throwable =>

        if (!inputLine._2.startsWith("#")) {
          // Launched exception
          var exceptionMessage = "The following region is not compliant with the provided schema: \n [" + inputLine._2.substring(1, inputLine._2.length - 1).split(semiCommaDelimiter).toIterator.next() + "]\n"
          if (e.isInstanceOf[IllegalArgumentException] || e.isInstanceOf[NumberFormatException]) {
            exceptionMessage += "\n Wrong type: " + e.getClass.getCanonicalName.replace("java.lang.", "") + " " + e.getMessage
          }

          throw ParsingException.create(exceptionMessage, e)

        } else {
          None
        }
    }
  }

}


/**
  * Custom parser that reads the schema xml file and then provide the schema internally for parsing the data using the BED PARSER
  */
class CustomArrayParser extends ArrayParser("\t",  None) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[CustomArrayParser]);

  def setSchema(dataset: String): ArrayParser = {

    val path: Path = new Path(dataset);
    val fs: FileSystem = FileSystem.get(path.toUri(), FSConfig.getConf);

    //todo: remove this hard fix used for remote execution
    val XMLfile: InputStream =
      if (!fs.exists(new Path(dataset + (if (!dataset.endsWith("xml")) "/schema.xml" else ""))))
        fs.open(new Path(dataset + (if (!dataset.endsWith("xml")) "/test.schema" else "")))
      else
        fs.open(new Path(dataset + (if (!dataset.endsWith("xml")) "/schema.xml" else "")))
    var schematype = GMQLSchemaFormat.TAB
    var coordinatesystem = GMQLSchemaCoordinateSystem.Default
    var schema: Array[(String, ParsingType.Value)] = null

    try {
      val schemaXML = XML.load(XMLfile);
      val cc = (schemaXML \\ "field")
      schematype = GMQLSchemaFormat.getType((schemaXML \\ "gmqlSchema").head.attribute("type").get.head.text.trim.toLowerCase())
      val coordSysAttr = (schemaXML \\ "gmqlSchema").head.attribute("coordinate_system")
      coordinatesystem = GMQLSchemaCoordinateSystem.getType(if (coordSysAttr.isDefined) coordSysAttr.get.head.text.trim.toLowerCase() else "default")
      schema = cc.map(x => (x.text.trim, ParsingType.attType(x.attribute("type").get.head.text))).toArray
    } catch {
      case x: Throwable => x.printStackTrace(); logger.error(x.getMessage); throw new RuntimeException(x.getMessage)
    }

    coordinatesystem match {
      case GMQLSchemaCoordinateSystem.ZeroBased => coordinateSystem = GMQLSchemaCoordinateSystem.ZeroBased
      case GMQLSchemaCoordinateSystem.OneBased => coordinateSystem = GMQLSchemaCoordinateSystem.OneBased
      case _ => coordinateSystem = GMQLSchemaCoordinateSystem.Default
    }

    schematype match {
      case GMQLSchemaFormat.VCF => {
        parsingType = GMQLSchemaFormat.VCF

        if (coordinateSystem == GMQLSchemaCoordinateSystem.Default) coordinateSystem = GMQLSchemaCoordinateSystem.OneBased

        val valuesPositions = schema.zipWithIndex.flatMap { x =>
          val name = x._1._1
          if (checkCoordinatesName(name)) None
          else Some(x._2 + 2, x._1._2)
        }

        val valuesPositionsSchema = schema.flatMap { x =>
          val name = x._1
          if (checkCoordinatesName(name)) None
          else Some(x._1, x._2)
        }.toList

        val other: Array[(Int, ParsingType.Value)] = if (valuesPositions.length > 0)
          (5, ParsingType.DOUBLE) +: valuesPositions
        else
          Array((5, ParsingType.DOUBLE))

//        chrPos = 0
//        startPos = 1
//        stopPos = 1
//        strandPos = None
//        otherPos = Some(other)

        this.schema = valuesPositionsSchema
      }
      case GMQLSchemaFormat.GTF => {
        parsingType = GMQLSchemaFormat.GTF

        if (coordinateSystem == GMQLSchemaCoordinateSystem.Default) coordinateSystem = GMQLSchemaCoordinateSystem.OneBased

        val valuesPositions: Array[(Int, ParsingType.Value)] = schema.flatMap { x =>
          val name = x._1.toUpperCase();
          if (name.equals("SEQNAME") || name.equals("SOURCE") || name.equals("FEATURE") || name.equals("FRAME") || name.equals("SCORE") || checkCoordinatesName(name)) None
          else Some(8, x._2)
        }

        val valuesPositionsSchema: Seq[(String, ParsingType.Value)] = schema.flatMap { x =>
          val name = x._1.toUpperCase();
          if (name.equals("SEQNAME") || name.equals("SOURCE") || name.equals("FEATURE") || name.equals("FRAME") || name.equals("SCORE") || checkCoordinatesName(name)) None
          else Some(x._1, x._2)
        }.toList


        val other: Array[(Int, ParsingType.Value)] = if (valuesPositions.length > 0)
          Array[(Int, ParsingType.Value)]((1, ParsingType.STRING), (2, ParsingType.STRING), (5, ParsingType.DOUBLE), (7, ParsingType.STRING)) ++ valuesPositions
        else
          Array((5, ParsingType.DOUBLE))

//        chrPos = 0
//        startPos = 3
//        stopPos = 4
//        strandPos = Some(6)
//        otherPos = Some(other)

        this.schema = List(("source", ParsingType.STRING), ("feature", ParsingType.STRING), ("score", ParsingType.DOUBLE), ("frame", ParsingType.STRING)) ++ valuesPositionsSchema
      }

      case _ => {

        if (coordinateSystem == GMQLSchemaCoordinateSystem.Default) coordinateSystem = GMQLSchemaCoordinateSystem.ZeroBased

        val schemaWithIndex = schema.zipWithIndex
        val chrom = schemaWithIndex.filter(x => (x._1._1.toUpperCase().equals("CHROM") || x._1._1.toUpperCase().equals("CHROMOSOME") || x._1._1.toUpperCase().equals("CHR")))
        val start = schemaWithIndex.filter(x => (x._1._1.toUpperCase().equals("START") || x._1._1.toUpperCase().equals("LEFT")))
        val stop = schemaWithIndex.filter(x => (x._1._1.toUpperCase().equals("STOP") || x._1._1.toUpperCase().equals("RIGHT") || x._1._1.toUpperCase().equals("END")))
        val strand = schemaWithIndex.filter(x => (x._1._1.toUpperCase().equals("STR") || x._1._1.toUpperCase().equals("STRAND")))

        var missing = 0
        val chromPosition = if (chrom.size > 0) chrom.head._2
        else {
          missing += 1;
          0
        }
        val startPosition = if (start.size > 0) start.head._2
        else {
          missing += 1;
          1
        }
        val stopPosition = if (stop.size > 0) stop.head._2
        else {
          missing += 1;
          2
        }
        val strPosition = if (strand.size > 0) Some(strand.head._2 + missing)
        else {
          logger.warn("Strand is not specified in the XML schema file, the default strand (which is * is selected.")
          None
        } //in this case strand considered not present

        val valuesPositions = schemaWithIndex.flatMap { x =>
          val name = x._1._1;
          if (checkCoordinatesName(name)) None
          else Some(x._2 + missing, x._1._2)
        }
        val valuesPositionsSchema = schemaWithIndex.flatMap { x =>
          val name = x._1._1;
          if (checkCoordinatesName(name)) None
          else Some(x._1)
        }
//        chrPos = chromPosition;
//        startPos = startPosition;
//        stopPos = stopPosition;
//        strandPos = strPosition;
//        otherPos = Some(valuesPositions)


        this.schema = valuesPositionsSchema.toList
      }
    }

    this

  }

  /**
    * check the name of the coordinates in the schema xml file.
    *
    * @param fieldName the column name in the schemas
    * @return return True when the column is a coordinate column, otherwise false.
    */
  def checkCoordinatesName(fieldName: String): Boolean = {
    fieldName.toUpperCase().equals("CHROM") || fieldName.toUpperCase().equals("CHROMOSOME") ||
      fieldName.toUpperCase().equals("CHR") || fieldName.toUpperCase().equals("START") ||
      fieldName.toUpperCase().equals("STOP") || fieldName.toUpperCase().equals("LEFT") ||
      fieldName.toUpperCase().equals("RIGHT") || fieldName.toUpperCase().equals("END") ||
      fieldName.toUpperCase().equals("STRAND") || fieldName.toUpperCase().equals("STR")
  }
}

