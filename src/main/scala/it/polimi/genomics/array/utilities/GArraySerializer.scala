package it.polimi.genomics.array.utilities

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GAttributes, GRegionKey}
import it.polimi.genomics.core._

/**
  * Created by Olga Gorlova on 29/10/2019.
  */
object GArraySerializer extends Serializer[GARRAY]{


  override def write(kryo: Kryo, output: Output, t: (GRegionKey, GAttributes)): Unit = {
    // write genomic coordinates
    output.writeString(t._1.chrom)
    output.writeLong(t._1.start)
    output.writeLong(t._1.stop)
    output.writeChar(t._1.strand)

    // write list of IDs,
    // but first write number of samples
    output.writeInt(t._2._1.length)
    // Split ids and counts, write them separately
    // First write array of ids
    output.writeLongs(t._2._1.map(_._1))
    // Then array of counts
    output.writeInts(t._2._1.map(_._2))

    // write number of attributes first
    output.writeInt(t._2._2.length)

    // iterate over attributes and samples and write arrays of values
    t._2._2.foreach{a=>
      val parsingType = a.head.head match {
        case GInt(t) => ParsingType.INTEGER
        case GDouble(t) => ParsingType.DOUBLE
        case GString(t) => ParsingType.STRING
        case GNull() => ParsingType.NULL
      }

      // write parsing type
      output.writeString(parsingType.toString)

      // write samples values
      a.foreach{s=>
        parsingType match {
          case ParsingType.INTEGER => output.writeInts(s.map(v=> v.toString.toInt))
          case ParsingType.DOUBLE => output.writeDoubles(s.map(v=> v.toString.toDouble))
          case ParsingType.STRING => output.writeString(s.mkString(";"))
          case ParsingType.NULL => output.writeString(s.mkString(";"))

        }
      }
    }

  }

  override def read(kryo: Kryo, input: Input, aClass: Class[(GRegionKey, GAttributes)]): (GRegionKey, GAttributes) = {

    // coordinates
    val chr = input.readString()
    val start = input.readLong()
    val stop = input.readLong()
    val strand = input.readChar()

    // ids
    val numSamples = input.readInt()
    val ids = input.readLongs(numSamples)
    val counts = input.readInts(numSamples)
    val samples = ids.zip(counts)

    // attributes
    val numAtt = input.readInt()

    val att = (for (i <- 0 until numAtt) yield {
      val parsingType = ParsingType.attType(input.readString())
      val samplesValues = counts.map{ s=>
       val values: Array[GValue] = parsingType match {
          case ParsingType.INTEGER => input.readInts(s).map(v=> GInt(v))
          case ParsingType.DOUBLE => input.readDoubles(s).map(v=> GDouble(v))
          case ParsingType.STRING => input.readString().split(";").map(v=> GString(v))
          case ParsingType.NULL => input.readString().split(";").map(v=> GNull())
        }
        values
      }
      samplesValues
    }).toArray

    (new GRegionKey(chr, start, stop, strand), new GAttributes(samples, att))
  }
}
