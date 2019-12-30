package it.polimi.genomics.array.implementation.RegionsOperators

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GAttributes, GRegionKey}
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.RegionAggregate.{COORD_POS, RegionExtension}
import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.{GDouble, GString, GValue}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  *
  * This version allows to update start/stop with attribute value but this makes the implementation extremely slow.
  *
  */
object ProjectRD2 {

  private final val logger = Logger.getLogger(this.getClass);

  def apply(executor: GMQLArrayExecutor, projectedValues : Option[List[Int]], tupleAggregator : Option[List[RegionExtension]], inputDataset : RegionOperator, sc: SparkContext) : RDD[(GRegionKey, GAttributes)] = {

    logger.info("----------------ProjectRD executing...")

    val input = executor.implement_rd(inputDataset, sc)

    execute(projectedValues, tupleAggregator, input, sc)
  }

  def apply(projectedValues : Option[List[Int]], tupleAggregator : Option[List[RegionExtension]], inputDataset : RDD[GARRAY], sc: SparkContext) : RDD[GARRAY] = {

    logger.info("----------------ProjectRD executing...")

    execute(projectedValues, tupleAggregator, inputDataset, sc)
  }

  def execute(projectedValues : Option[List[Int]], tupleAggregator : Option[List[RegionExtension]], input : RDD[GARRAY], sc: SparkContext) : RDD[GARRAY] = {

    val extended = if (tupleAggregator.isDefined) {
      input.flatMap { a => extendRegion(a, tupleAggregator.get)
      }
    }
    else input

    val prepared = if(projectedValues.isDefined)
      extended.map(a  => (a._1,  new GAttributes(a._2._1, a._2._2.zipWithIndex.flatMap(v=> if (projectedValues.get.contains(v._2)) Some(v._1) else None ) )))
    else if (tupleAggregator.isDefined)
      extended
    else
      extended.map(a=> (a._1, new GAttributes(a._2._1)))

    val out = prepared.reduceByKey { (l, r) =>
      val an= l._2.zip(r._2).map { att =>
        val ggg = r._1.zipWithIndex.map { rId =>
          if (l._1.contains(rId._1)) {
            val lll = att._1.zipWithIndex.map{bbb=> if (bbb._2 == l._1.indexOf(rId._1)) bbb._1 ++ att._2(rId._2) else bbb._1}
            lll
          }
          else {
            //            val ww = att._1 ++ att._2
            att._1 ++ att._2
          }
        }
        ggg
      }.flatten

      val idn: Array[(Long, Int)] = r._1.flatMap { rId =>
        if (!l._1.contains(rId)) {
          l._1 :+ rId
        }
        else l._1
      }

      new GAttributes(idn, an)
    }


    out
  }


  def computeFunction(r : (GRegionKey, GAttributes), agg : RegionExtension) : (Array[(GValue, GAttributes)]) = {
    val ci = agg.inputIndexes.zipWithIndex.flatMap{b=>
      if (b._1.asInstanceOf[Int] < 0)
      {
        val tt = b._1.asInstanceOf[Int] match {
          case COORD_POS.CHR_POS => new GString(r._1._1)
          case COORD_POS.LEFT_POS => new GDouble(r._1._2)
          case COORD_POS.RIGHT_POS => new GDouble(r._1._3)
          case COORD_POS.STRAND_POS => new GString(r._1._4.toString)
          case COORD_POS.START_POS => if (r._1.strand.equals('+') || r._1.strand.equals('*')) new GDouble(r._1._2) else new GDouble(r._1._3)
          case COORD_POS.STOP_POS => if (r._1.strand.equals('+') || r._1.strand.equals('*')) new GDouble(r._1._3) else new GDouble(r._1._2)
        }
        Some((tt,b._2))
      }
      else None
    }


    val kkk: Array[Array[(Int, GValue)]] = r._2._2.map{ v=>
      v.zipWithIndex.flatMap(t=> t._1.map(l=> (t._2, l)))
    }.transpose

    val ai4: Array[Array[(GValue, Int)]] = kkk.map{ k=>
      agg.inputIndexes.zipWithIndex.foldLeft(Array[(GValue, Int)]()) { (acc, b) =>

        if (b._1.asInstanceOf[Int] > -1) {
          acc :+ (k(b._1.asInstanceOf[Int])._2, b._2)
        }
        else acc

      }
    }

    val toAgg3 = ai4.map{ e=>
      if (ci.nonEmpty) {val rrrr = ci ++ e; rrrr.sortBy(_._2).map(_._1)} else e.sortBy(_._2).map(_._1).toList
    }


    val aNew = if (ai4.nonEmpty){
      var i = -1;
      r._2._1.flatMap{id=>
        i += 1;
        var j = -1;
        r._2._2.head(i).map { g =>
          j += 1;
          val attN = r._2._2.map(a => (Array(Array(a(i)(j)))))
          new GAttributes(Array(id), attN)
        }
      }
    }
    else Array(r._2)


    val out = toAgg3.map{v=>
      val list = v.toArray
      agg.fun(list)
    }.toArray
    out.zip(aNew)
  }

  def extendRegion(input : (GRegionKey, GAttributes), aggList : List[RegionExtension]) : Array[(GRegionKey, GAttributes)] = {

    var regionCache: List[(GRegionKey, GAttributes)] = List(input)
    var temp: List[(GRegionKey, GAttributes)] = List(input)

    var i = -1;
    val gg = aggList.flatMap { agg =>
      i += 1;
      val func = computeFunction(input, agg)
      agg.output_index match {
        case Some(COORD_POS.CHR_POS) => {
          if (i > 0) {
            temp = temp.zip(func).map{v => (new GRegionKey(v._2._1.toString, v._1._1.start, v._1._1.stop, v._1._1.strand), v._2._2) }
          } else temp = func.map{v=> (new GRegionKey(v._1.toString, input._1.start, input._1.stop, input._1.strand), v._2)}.toList
          regionCache = temp
          regionCache
        }
        case Some(COORD_POS.LEFT_POS) => {
          if (i > 0) {
            temp = temp.zip(func).map{v => (new GRegionKey(v._1._1.chrom, v._2._1.toString.toLong, v._1._1.stop, v._1._1.strand), v._2._2) }
          } else temp = func.map{v=> (new GRegionKey(input._1.chrom, v._1.toString.toLong, input._1.stop, input._1.strand), v._2)}.toList

          regionCache = temp
          regionCache
        }
        case Some(COORD_POS.RIGHT_POS) => {
          if (i > 0) {
            temp = temp.zip(func).map{v => (new GRegionKey(v._1._1.chrom, v._1._1.start, v._2._1.toString.toLong, v._1._1.strand), v._2._2) }
          } else temp = func.map{v=> (new GRegionKey(input._1.chrom, input._1.start, v._1.toString.toLong, input._1.strand), v._2)}.toList
          regionCache = temp
          regionCache
        }
        case Some(COORD_POS.STRAND_POS) => {
          if (i > 0) {
            temp = temp.zip(func).map{v => (new GRegionKey(v._1._1.chrom, v._1._1.start, v._1._1.stop, v._2._1.toString.charAt(0)), v._2._2) }
          } else temp = func.map{v=> (new GRegionKey(input._1.chrom, input._1.start, input._1.stop, v._1.toString.charAt(0)), v._2)}.toList
          regionCache = temp
          regionCache
        }
        case Some(COORD_POS.START_POS) => {
          if (input._1._4.equals('-')) {
            if (i > 0) {
              temp = temp.zip(func).map{v => (new GRegionKey(v._1._1.chrom, v._1._1.start, v._2._1.toString.toLong, v._1._1.strand), v._2._2) }
            } else temp = func.map{v=> (new GRegionKey(input._1.chrom, input._1.start, v._1.toString.toLong, input._1.strand), v._2)}.toList
            regionCache = temp
            regionCache
          } else {
            if (i > 0) {
              temp = temp.zip(func).map{v => (new GRegionKey(v._1._1.chrom, v._2._1.toString.toLong, v._1._1.stop, v._1._1.strand), v._2._2) }
            } else temp = func.map{v=> (new GRegionKey(input._1.chrom, v._1.toString.toLong, input._1.stop, input._1.strand), v._2)}.toList
            regionCache = temp
            regionCache
          }
        }
        case Some(COORD_POS.STOP_POS) => {
          if (input._1._4.equals('-')) {
            if (i > 0) {
              temp = temp.zip(func).map{v => (new GRegionKey(v._1._1.chrom, v._2._1.toString.toLong, v._1._1.stop, v._1._1.strand), v._2._2) }
            } else temp = func.map{v=> (new GRegionKey(input._1.chrom, v._1.toString.toLong, input._1.stop, input._1.strand), v._2)}.toList
            regionCache = temp
            regionCache
          } else {
            if (i > 0) {
              temp = temp.zip(func).map{v => (new GRegionKey(v._1._1.chrom, v._1._1.start, v._2._1.toString.toLong, v._1._1.strand), v._2._2) }
            } else temp = func.map{v=> (new GRegionKey(input._1.chrom, input._1.start, v._1.toString.toLong, input._1.strand), v._2)}.toList
            regionCache = temp
            regionCache
          }
        }
        case Some(v: Int) => {
          if (i > 0) {
            temp = temp.zip(func).map{f =>
              val dd = f._2._2._2.zipWithIndex.map { a => if (a._2 == v) a._1.map(_.map(w => f._2._1)) else a._1 }
              (new GRegionKey(f._1._1.chrom, f._1._1.start, f._2._1.toString.toLong, f._1._1.strand), new GAttributes(f._2._2._1, dd))
            }
          } else temp = func.map{t=>
            val dd = t._2._2.zipWithIndex.map { a => if (a._2 == v) a._1.map(_.map(w => t._1)) else a._1 }
            (new GRegionKey(input._1.chrom, input._1.start, t._1.toString.toLong, input._1.strand), new GAttributes(t._2._1, dd))}.toList
          regionCache = temp
          regionCache
        }

        case None => {
          if (i > 0) {
            temp = temp.zip(func).map{v =>
              (v._1._1, new GAttributes(v._2._2._1, v._2._2._2 :+ Array(Array(v._2._1))))}
          } else temp = func.map{v=>
            (input._1, new GAttributes(input._2._1, input._2._2 :+ Array(Array(v._1))))}.toList

          regionCache = temp
          regionCache
        }


      }
    }

    temp = List()
    regionCache.toArray
  }

}

