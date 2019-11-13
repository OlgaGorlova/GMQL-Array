package it.polimi.genomics.array.implementation.MetaOperators

import it.polimi.genomics.core.DataTypes.MetaType
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object ReadMEMMD {

  def apply (metaRDD:Any): RDD[MetaType] = {
    metaRDD.asInstanceOf[RDD[MetaType]]
  }

}
