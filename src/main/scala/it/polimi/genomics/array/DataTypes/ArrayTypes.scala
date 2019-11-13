package it.polimi.genomics.array.DataTypes

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
object ArrayTypes {

  /**
    * Data Type used for region operations in Spark
    * We use Key / Value
    * where the Key is the GRegionKey and the value is the GAttributes
    */

  type GARRAY = (GRegionKey,GAttributes)

}
