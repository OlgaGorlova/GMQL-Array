package it.polimi.genomics.array.implementation.MetaOperators.GroupOperator

import java.nio.charset.StandardCharsets

import com.google.common.hash.Hashing
import it.polimi.genomics.array.implementation.GMQLArrayExecutor
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.{AttributeEvaluationStrategy, Default, Exact, FullName}
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object MetaGroupMGD {

  private final val logger = Logger.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor: GMQLArrayExecutor, condition: MetaGroupByCondition, inputDataset: MetaOperator, sc: SparkContext): RDD[(Long, Long)] = {
    executor.implement_md(inputDataset, sc).MetaWithGroups(condition.attributes)
  }

  implicit class MetaGroup(rdd: RDD[(Long, (String, String))]) {
    def MetaWithGroups(condition: List[AttributeEvaluationStrategy]): RDD[(Long, Long)] = {
      rdd.filter{ x =>
        condition.foldLeft(false)((r,c) =>
          r | {
            if (c.isInstanceOf[FullName]) {
              (x._2._1.equals(c.asInstanceOf[FullName].attribute.toString()) || x._2._1.endsWith("." + c.asInstanceOf[FullName].attribute.toString()))
            }
            else if (c.isInstanceOf[Exact]) {
              x._2._1.equals(c.asInstanceOf[Exact].attribute.toString())
            }
            else {
              (x._2._1.equals(c.asInstanceOf[Default].attribute.toString()) || x._2._1.endsWith("." + c.asInstanceOf[Default].attribute.toString()))
            }
          }
        )
      }.map{x=>
        condition.flatMap{att=>
          if (att.isInstanceOf[FullName]) {
            if (x._2._1.equals(att.asInstanceOf[FullName].attribute) || x._2._1.endsWith("." + att.asInstanceOf[FullName].attribute))
              Some((x._1, (x._2._1, x._2._2)))
            else None
          }
          else if (att.isInstanceOf[Exact]) {
            if (x._2._1.equals(att.asInstanceOf[Exact].attribute))
              Some((x._1, (att.asInstanceOf[Exact].attribute.toString(), x._2._2)))
            else None
          }
          else {
            if (x._2._1.equals(att.asInstanceOf[Default].attribute) || x._2._1.endsWith("." + att.asInstanceOf[Default].attribute))
              Some((x._1, (att.asInstanceOf[Default].attribute.toString(), x._2._2)))
            else None
          }
        }.head
      }
        .groupByKey()
        .flatMap { x =>
          val itr = x._2.toList.distinct
          if (!itr.iterator.hasNext) None
          else {
            Some(x._1, Hashing.md5.newHasher().putString(
              itr.groupBy(_._1).map(d => (d._1, d._2.map(e => e._2).sorted.mkString("§"))).toList.sortBy(_._1).mkString("£")
              , StandardCharsets.UTF_8).hash().asLong)
          }
        }
      //output is (SampleID, Group)

    }
  }

}

