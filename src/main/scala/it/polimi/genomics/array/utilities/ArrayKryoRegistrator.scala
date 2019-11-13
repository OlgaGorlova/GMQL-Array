package it.polimi.genomics.array.utilities

import com.esotericsoftware.kryo.Kryo
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import it.polimi.genomics.array.DataTypes.{GArray, GAttributes, GRegionKey}
import it.polimi.genomics.core.GValue
import org.apache.log4j.Logger
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by Olga Gorlova on 17/10/2019.
  */
class ArrayKryoRegistrator extends KryoRegistrator{

  /**
    * The Constant logger.
    */
  final val logger = Logger.getLogger(this.getClass);

  override def registerClasses(kryo: Kryo): Unit = {

    //    logger.info("Registering custom serializers for array types")

    kryo.register(classOf[GRegionKey])
    kryo.register(classOf[GValue])
    kryo.register(classOf[GAttributes])
    kryo.register(classOf[GArray])
//    kryo.register(classOf[GArray], GArraySerializer)
//    kryo.register(classOf[GSamples])
//    kryo.register(classOf[GFeatures])


  }


}
