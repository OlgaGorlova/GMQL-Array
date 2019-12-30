package it.polimi.genomics.array.implementation.RegionsOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GAttributes, GRegionKey}
import it.polimi.genomics.core.DataStructures.RegionAggregate
import it.polimi.genomics.core.DataTypes.{FlinkRegionType, GRECORD}
import it.polimi.genomics.core.{GDouble, GNull, GRecordKey, GValue}
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 21/11/2019.
  */
object GMAP4 {
  private final val logger = Logger.getLogger(this.getClass)


  type Grecord = (String, Long, Long, Char, Array[GValue])

  @throws[SelectFormatException]
  def apply(aggregator : List[RegionAggregate.RegionsToRegion], flat : Boolean, summit:Boolean, ref : RDD[Grecord], exp : RDD[GARRAY], BINNING_PARAMETER : Long) : RDD[GARRAY] = {
    logger.info("----------------GMAP4 executing..")
    val expBinned = binDS(exp,aggregator,BINNING_PARAMETER)
    val refBinnedRep = binDS2(ref,BINNING_PARAMETER)

    //    refBinnedRep.collect().foreach(println _)
    //    expBinned.collect().foreach(println _)
    val RefExpJoined = refBinnedRep.leftOuterJoin(expBinned)
      .flatMap { grouped =>
        val key = grouped._1;
        val ref = grouped._2._1;
        val exp = grouped._2._2
        val aggregation = Hashing.md5().newHasher().putString(key._1 + ref._1 + ref._2+ref._3+ref._4.mkString("/"),java.nio.charset.Charset.defaultCharset()).hash().asLong()
        if (!exp.isDefined) {
          None
        } else {
          val e = exp.get
          if(/* cross */
          /* space overlapping */
            (ref._1 < e._2 && e._1 < ref._2)
              && /* same strand */
              (ref._3.equals('*') || e._3.equals('*') || ref._3.equals(e._3))
              && /* first comparison */
              ((ref._1/BINNING_PARAMETER).toInt.equals(key._2) ||  (e._1/BINNING_PARAMETER).toInt.equals(key._2))
          )
            Some(aggregation, ((key._1, ref._1, ref._2, ref._3), ref._4, exp.get._4, exp.get._1, exp.get._2, exp.get._1, exp.get._2, (1, exp.get._4._2.flatMap(a => a.flatMap(s=> s.map(v=> if (v.isInstanceOf[GNull]) 0 else 1)) ).iterator.toArray)))
          else None
        }
      }

    val reduced  = RefExpJoined.reduceByKey{(l,r)=>
      val values =
        if(l._3._2.size > 0 && r._3._2.size >0) {
          var i = -1;
          val dd = aggregator.map{a=> i+=1
            val agval = l._3._2(i).flatMap(s=> s.map(v=> v)) ++ r._3._2(i).flatMap(s=> s.map(v=> v))
            a.fun(agval.toList)
          }.toArray
          new GAttributes(l._3._1, dd.map(a=> Array(Array(a))))
        } else if(r._3._2.size >0)
          r._3
        else l._3
      val startMin =Math.min(l._4, r._4)
      val endMax = Math.max(l._5, r._5)
      val startMax = Math.max(l._6, r._6)
      val endMin = Math.min(l._7, r._7)

      (l._1,l._2,values,startMin, endMax, if(startMax < endMin) startMax else 0L, if(endMin > startMax) endMin else 0L, (l._8._1+r._8._1, l._8._2.zip(r._8._2).map(s=>s._1+s._2).iterator.toArray))

    }

    //Aggregate Exp Values (reduced)

    val output = reduced.map{res =>
      var i = -1;
      val start : Double = if(flat) res._2._4 else res._2._1._3
      val end : Double = if (flat) res._2._5 else res._2._1._4

      val newVal:Array[GValue] = aggregator.map{f=>
        i = i+1;
        val valList = if(res._2._3._2.size >0)res._2._3._2(i).head.head else {GDouble(0.0000000000000001)};
        f.funOut(valList,(res._2._8._1, if(res._2._3._2.size >0) res._2._8._2(i) else 0))
      }.toArray

      /*// Jaccard 1
          :+ { if(res._2._5-res._2._4 != 0){ GDouble(Math.abs((end.toDouble-start)/(res._2._5-res._2._4))) } else { GDouble(0) } }
          // Jaccard 2
          :+ { if(res._2._5-res._2._4 != 0){ GDouble(Math.abs((res._2._7.toDouble-res._2._6)/(res._2._5-res._2._4))) } else { GDouble(0) } }*/

      val newRegion = new GRegionKey(res._2._1._1, start.toLong, end.toLong,res._2._1._4)
      val newAtt = res._2._2.map(a=> Array(Array(a))) ++ newVal.map(a=> Array(Array(a)))
      (newRegion, new GAttributes(Array((0l, 1)), newAtt)
      )
    }

    output

  }

  def binDS(rdd : RDD[GARRAY],aggregator: List[RegionAggregate.RegionsToRegion], bin: Long)
  : RDD[((String, Int), (Long, Long, Char, GAttributes))] =
    rdd.flatMap { x =>
      if (bin > 0) {
        val startbin = (x._1._2 / bin).toInt
        val stopbin = (x._1._3 / bin).toInt
        val newVal = aggregator
          .map((f: RegionAggregate.RegionsToRegion) => {
            x._2._2(f.index)
          }).toArray
        //          println (newVal.mkString("/"))
        for (i <- startbin to stopbin)
          yield ((x._1._1, i), (x._1._2, x._1._3, x._1._4, new GAttributes(x._2._1, newVal)))
      } else
      {
        val newVal = aggregator
          .map((f: RegionAggregate.RegionsToRegion) => {
            x._2._2(f.index)
          }).toArray
        //          println (newVal.mkString("/"))
        Some((x._1._1, 0), (x._1._2, x._1._3, x._1._4, new GAttributes(x._2._1, newVal)))
      }
    }

  def binDS2(rdd:RDD[Grecord],bin: Long )
  : RDD[((String, Int), ( Long, Long, Char, Array[GValue]))] =
    rdd.flatMap { x =>
      if (bin > 0) {
        val startbin = (x._2 / bin).toInt
        val stopbin = (x._3 / bin).toInt
        (startbin to stopbin).map(i =>
          ((x._1, i), ( x._2, x._3, x._4, x._5))
        )
      }else
      {
        List(((x._1, 0), ( x._2, x._3, x._4, x._5)))
      }
    }

}
