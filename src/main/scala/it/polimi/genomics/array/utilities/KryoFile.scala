package it.polimi.genomics.array.utilities

import java.io._
import java.util.zip.Deflater

import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.serializers.DeflateSerializer
import com.twitter.chill.{AllScalaRegistrar, EmptyScalaKryoInstantiator}
import it.polimi.genomics.array.DataTypes.ArrayTypes.GARRAY
import org.apache.hadoop.io.compress.DeflateCodec
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer

import scala.reflect.ClassTag

/**
  * Created by Olga Gorlova on 11/11/2019.
  */
object KryoFile {

  def toBytes(o: GARRAY): Array[Byte] = {
    val kryo = (new EmptyScalaKryoInstantiator).newKryo()
    val clazz: Class[GARRAY] = classOf[GARRAY]
    val deflateSerializer = new DeflateSerializer(kryo.getDefaultSerializer(clazz))
    deflateSerializer.setCompressionLevel(Deflater.BEST_COMPRESSION)
    kryo.register(clazz, deflateSerializer)
    val bao = new ByteArrayOutputStream()
    val output = new KryoOutput(1024*1024)
    output.setOutputStream(bao)
    kryo.writeClassAndObject(output, o)
    output.close()
    // We are ignoring key field of sequence file
    bao.toByteArray
  }

  def toObject(bytes:  Array[Byte] ): GARRAY = {
    val kryo = (new EmptyScalaKryoInstantiator).newKryo()
    //val clazz: Class[Person] = classOf[Person]
    //val deflateSerializer = new DeflateSerializer(kryo.getDefaultSerializer(clazz))
    //deflateSerializer.setCompressionLevel(Deflater.BEST_COMPRESSION)
    //kryo.register(clazz, deflateSerializer)
    new AllScalaRegistrar().apply(kryo)
    val input = new KryoInput()
    input.setBuffer(bytes)
    val data = kryo.readClassAndObject(input)
    val dataObject = data.asInstanceOf[GARRAY]
    dataObject
  }

  /*
   * Used to write as Object file using kryo serialization
   */
  def saveAsKryoObjectFile[T: ClassTag](rdd: RDD[T], path: String) {

    val kryoSerializer = new KryoSerializer(rdd.context.getConf)
    rdd.mapPartitions(iter => iter.grouped(10)
      .map(_.toArray))
      .map ( splitArray => {
        val kryo = kryoSerializer.newKryo()
        //create output stream and plug it to the kryo output
        val bao = new ByteArrayOutputStream()
        val output = kryoSerializer.newKryoOutput()
        output.setOutputStream(bao)
        kryo.writeClassAndObject(output, splitArray)
        output.close()
        val byteWritable = new BytesWritable(bao.toByteArray)
        (NullWritable.get(), byteWritable)
      }).saveAsSequenceFile(path, Some(classOf[DeflateCodec]))

//    rdd.mapPartitions(iter => {
//
//      val kryo = (new EmptyScalaKryoInstantiator).newKryo()
//      //val clazz: Class[Person] = classOf[Person]
//      //val deflateSerializer = new DeflateSerializer(kryo.getDefaultSerializer(clazz))
//      //deflateSerializer.setCompressionLevel(Deflater.BEST_COMPRESSION)
//      //kryo.register(clazz, deflateSerializer)
//      new AllScalaRegistrar().apply(kryo)
//      iter.grouped(100).map(it => {
//        val bao = new ByteArrayOutputStream()
//        val output = new KryoOutput(1024*1024)
//        output.setOutputStream(bao)
//        kryo.writeClassAndObject(output, it.toArray)
//        output.close()
//        // We are ignoring key field of sequence file
//        val byteWritable = new BytesWritable(bao.toByteArray)
//        (NullWritable.get(), byteWritable)
//      })
//    }).saveAsSequenceFile(path, Some(classOf[DeflateCodec]))
    //, Some(classOf[DeflateCodec])
  }

  /*
   * Method to read from object file which is saved kryo format.
   */
  def objectKryoFile[T](sc: SparkContext, path: String, minPartitions: Int = 2)(implicit ct: ClassTag[T]) = {

    val kryoSerializer = new KryoSerializer(sc.getConf)

    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions).mapPartitions(iter => {
      //val kryo = (new EmptyScalaKryoInstantiator).newKryo()
      val kryo = kryoSerializer.newKryo()
      //val clazz: Class[Person] = classOf[Person]
      //val deflateSerializer = new DeflateSerializer(kryo.getDefaultSerializer(clazz))
      //deflateSerializer.setCompressionLevel(Deflater.BEST_COMPRESSION)
      //kryo.register(clazz, deflateSerializer)
      //new AllScalaRegistrar().apply(kryo)
      iter.flatMap(x => {
        val input = new KryoInput()
        input.setBuffer(x._2.getBytes)
        val data = kryo.readClassAndObject(input)
        val dataObject = data.asInstanceOf[Array[T]]
        dataObject
      })
    })

  }

  def objectFile[T](sc: SparkContext, path: String, minPartitions: Int = 10)(implicit ct: ClassTag[T]) = {
    val kryoSerializer = new KryoSerializer(sc.getConf)
    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable],
      minPartitions)
      .flatMap(x => {
        val kryo = kryoSerializer.newKryo()
        val input = new KryoInput()
        input.setBuffer(x._2.getBytes)
        val data = kryo.readClassAndObject(input)
        val dataObject = data.asInstanceOf[Array[T]]
        dataObject
      })
  }

  def saveAsObjectFile[T: ClassTag](rdd: RDD[T],path: String) {
    rdd.mapPartitions(iter => iter.grouped(100).map(_.toArray))
      .map(x => (NullWritable.get(), new BytesWritable(serialize(x))))
      .saveAsSequenceFile(path, Some(classOf[DeflateCodec]))
  }

//  def objectFile[T: ClassTag](sc: SparkContext,
//                              path: String,
//                              minPartitions: Int = 2
//                             ): RDD[T] = {
//    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
//      .flatMap(x => deserialize[Array[T]](x._2.getBytes, sc.getClass.getClassLoader))
//  }

  /** Serialize an object using Java serialization */
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  /** Deserialize an object using Java serialization */
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, loader)
    }
    ois.readObject.asInstanceOf[T]
  }

}

