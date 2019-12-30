package it.polimi.genomics.array.DataTypes

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
case class GRegionKey (chrom:String,start:Long,stop:Long,strand:Char)
  extends Ordered[GRegionKey]
  with Serializable
{
  def _1: String = chrom
  def _2: Long = start
  def _3: Long = stop
  def _4: Char = strand

  def this()= this("chr",0,0,'.')

  def compare(o: GRegionKey): Int = {
    if (this.chrom.equals(o._1))
      if (this.start == o._2)
        if (this.stop == o._3)
          this.stop compare o._4
        else this.stop compare o._3
      else this.start compare o._2
    else this.chrom compare o._1
  }

  override def toString ():String ={
    val reg = chrom + "\t" + start + "\t" + stop+"\t"+strand;
    //    values match {
    //      case _: Array[Any] => return reg+"\t"+values.iterator.mkString("\t");
    //    }
    reg
  }
}

case class GRegionKey2 (chrom:String,start:Long,stop:Long,strand:String)
  extends Ordered[GRegionKey]
    with Serializable
{
  def _1: String = chrom
  def _2: Long = start
  def _3: Long = stop
  def _4: String = strand

  def this()= this("chr",0,0,".")

  def compare(o: GRegionKey): Int = {
    if (this.chrom.equals(o._1))
      if (this.start == o._2)
        if (this.stop == o._3)
          this.stop compare o._4
        else this.stop compare o._3
      else this.start compare o._2
    else this.chrom compare o._1
  }

  override def toString ():String ={
    val reg = chrom + "\t" + start + "\t" + stop+"\t"+strand;
    //    values match {
    //      case _: Array[Any] => return reg+"\t"+values.iterator.mkString("\t");
    //    }
    reg
  }
}

//case class GRegionKey2 (chrom:String,start:Long,stop:Long,strand:Char)
//  extends Ordered[GRegionKey]
//    with KryoSerializable
//{
//  def _1: String = chrom
//  def _2: Long = start
//  def _3: Long = stop
//  def _4: Char = strand
//
//  def this()= this("chr",0,0,'.')
//
//  def compare(o: GRegionKey): Int = {
//    if (this.chrom.equals(o._1))
//      if (this.start == o._2)
//        if (this.stop == o._3)
//          this.stop compare o._4
//        else this.stop compare o._3
//      else this.start compare o._2
//    else this.chrom compare o._1
//  }
//
//  override def toString ():String ={
//    val reg = chrom + "\t" + start + "\t" + stop+"\t"+strand;
//    //    values match {
//    //      case _: Array[Any] => return reg+"\t"+values.iterator.mkString("\t");
//    //    }
//    reg
//  }
//
//  override def write(kryo: Kryo, output: Output): Unit = {
//    output.writeString(chrom)
//    output.writeLong(start)
//    output.writeLong(stop)
//    output.writeChar(strand)
//    kryo.writeClassAndObject(output, this)
//  }
//
//  override def read(kryo: Kryo, input: Input): Unit = {
//    val region = new GRegionKey2(input.readString(), input.readLong(),input.readLong(), input.readChar())
//    kryo.reference(region)
//    this = kryo.readClassAndObject(input)
//  }
//}