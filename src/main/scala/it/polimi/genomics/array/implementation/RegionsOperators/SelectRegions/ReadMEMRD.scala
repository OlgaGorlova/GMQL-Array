package it.polimi.genomics.array.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import org.apache.spark.rdd.RDD

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object ReadMEMRD {

  def apply(regionDS:Any): RDD[GARRAY] = {
    regionDS.asInstanceOf[RDD[GARRAY]]

  }

}
