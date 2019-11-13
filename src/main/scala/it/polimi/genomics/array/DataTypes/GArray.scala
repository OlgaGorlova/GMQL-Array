package it.polimi.genomics.array.DataTypes

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import it.polimi.genomics.core._

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
case class GArray (key:GRegionKey,values:GAttributes)
  extends Serializable {

  def _1: GRegionKey = key
  def _2: GAttributes = values

  def this() = this(new GRegionKey("chr", 0, 0, '.'), new GAttributes(Array[(Long,Int)](), Array[Array[Array[it.polimi.genomics.core.GValue]]]()))

  override def toString(): String = {
    val reg = key.toString() + "\t" + values.toString()
    reg
  }

//  override def write(kryo: Kryo, output: Output): Unit = {
//    // write genomic coordinates
//    output.writeString(this.key.chrom)
//    output.writeLong(this.key.start)
//    output.writeLong(this.key.stop)
//    output.writeChar(this.key.strand)
//
//    // write list of IDs,
//    // but first write number of samples
//    output.writeInt(this.values._1.length)
//    // Split ids and counts, write them separately
//    // First write array of ids
//    output.writeLongs(this.values._1.map(_._1))
//    // Then array of counts
//    output.writeInts(this.values._1.map(_._2))
//
//    // write number of attributes first
//    output.writeInt(this.values._2.length)
//
//    // iterate over attributes and samples and write arrays of values
//    this.values._2.foreach{a=>
//      val parsingType = a.head.head match {
//        case GInt(t) => ParsingType.INTEGER
//        case GDouble(t) => ParsingType.DOUBLE
//        case GString(t) => ParsingType.STRING
//        case GNull() => ParsingType.NULL
//      }
//
//      // write parsing type
//      output.writeString(parsingType.toString)
//
//      // write samples values
//      a.foreach{s=>
//        parsingType match {
//          case ParsingType.INTEGER => output.writeInts(s.map(v=> v.toString.toInt))
//          case ParsingType.DOUBLE => output.writeDoubles(s.map(v=> v.toString.toDouble))
//          case ParsingType.STRING => output.writeString(s.mkString(";"))
//          case ParsingType.NULL => output.writeString(s.mkString(";"))
//
//        }
//      }
//    }
//  }
//
//  override def read(kryo: Kryo, input: Input): Unit = {
//    // coordinates
//    val chr = input.readString()
//    val start = input.readLong()
//    val stop = input.readLong()
//    val strand = input.readChar()
//
//    // ids
//    val numSamples = input.readInt()
//    val ids = input.readLongs(numSamples)
//    val counts = input.readInts(numSamples)
//    val samples = ids.zip(counts)
//
//    // attributes
//    val numAtt = input.readInt()
//
//    val att = (for (i <- 0 until numAtt) yield {
//      val parsingType = ParsingType.attType(input.readString())
//      val samplesValues = counts.map{ s=>
//        val values: Array[GValue] = parsingType match {
//          case ParsingType.INTEGER => input.readInts(s).map(v=> GInt(v))
//          case ParsingType.DOUBLE => input.readDoubles(s).map(v=> GDouble(v))
//          case ParsingType.STRING => input.readString().split(";").map(v=> GString(v))
//          case ParsingType.NULL => input.readString().split(";").map(v=> GNull())
//        }
//        values
//      }
//      samplesValues
//    }).toArray
//
//    this(new GRegionKey(chr, start, stop, strand), new GAttributes(samples, att))
//  }

}

case class GAttributes (samples:Array[(Long, Int)],att:Array[Array[Array[it.polimi.genomics.core.GValue]]]) extends Serializable{

  def _1: Array[(Long, Int)] = samples
  def _2: Array[Array[Array[it.polimi.genomics.core.GValue]]] = att

  def this() = this (Array[(Long,Int)](), Array[Array[Array[it.polimi.genomics.core.GValue]]]())
  def this(samples:Array[(Long,Int)]) = this (samples, Array[Array[Array[it.polimi.genomics.core.GValue]]]())
  //  def this(samples:Array[(Long,Int)], att: Array[(Array[Array[GValue]], Int)]) = this (samples, att)

  override def toString(): String = {
    val reg = samples.mkString(" | ") + "\t"

    att match {
      case _: Array[Array[Array[it.polimi.genomics.core.GValue]]] => return reg + "\t" + att.iterator.map(y=> "["+y.map(z=> "("+z.mkString("; ")+")").mkString(", ")+"]" ).mkString("\t|\t");
    }
    reg
  }

  def canEqual(a: Any) = a.isInstanceOf[GAttributes]

  override def equals(that: Any): Boolean =
    that match {
      case that: GAttributes => {
        that.canEqual(this) &&
          this.samples.deep == that.samples.deep &&
          this.att.corresponds(that.att){_.deep == _.deep}
      }
      case _ => false
    }
}


class GFeatures (att:Array[Array[it.polimi.genomics.core.GValue]], count: Int)
  extends Tuple2(att,count)
    with Serializable {

  def this() = this (Array[Array[it.polimi.genomics.core.GValue]](), 0)
  def this(att:Array[Array[it.polimi.genomics.core.GValue]]) = this (att, 0)

  override def toString(): String = {
    val reg = att.iterator.map(y=> "(" + y.mkString("; ")+")").mkString("\t") + "\t" + count;

    reg
  }

}

class GSamples (id: Long, count: Int)
  extends Tuple2(id,count)
    with Serializable {

  def this() = this (0l, 0)
  def this(id:Long) = this (id, 0)
  def this(count:Int) = this (0l, count)

  override def toString(): String = {
    val reg = "(" + id + ", " + count + ")"
    reg
  }

//  class GAttributes2 (samples:Array[(Long, Int)],att:Array[Array[Array[it.polimi.genomics.core.GValue]]])
//    extends Tuple2(samples,att)
//      with KryoSerializable {
//
//    def this() = this (Array[(Long,Int)](), Array[Array[Array[it.polimi.genomics.core.GValue]]]())
//    def this(samples:Array[(Long,Int)]) = this (samples, Array[Array[Array[it.polimi.genomics.core.GValue]]]())
//    //  def this(samples:Array[(Long,Int)], att: Array[(Array[Array[GValue]], Int)]) = this (samples, att)
//
//    override def toString(): String = {
//      val reg = samples.mkString(" | ") + "\t"
//
//      att match {
//        case _: Array[Array[Array[it.polimi.genomics.core.GValue]]] => return reg + "\t" + att.iterator.map(y=> "["+y.map(z=> "("+z.mkString("; ")+")").mkString(", ")+"]" ).mkString("\t|\t");
//      }
//      reg
//    }
//
//    override def write(kryo: Kryo, output: Output): Unit = {
//        kryo.writeClassAndObject(output, this)
//    }
//
//    override def read(kryo: Kryo, input: Input): Unit = {
//      this = kryo.readClassAndObject(input)
//    }
//
//  }

}



