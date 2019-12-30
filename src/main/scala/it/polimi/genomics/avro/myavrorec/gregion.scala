package it.polimi.genomics.avro.myavrorec

/**
  * Created by Olga Gorlova on 20/11/2019.
  */
case class gregion(chr: String, start: Long, stop: Long, strand: String, idsList: List[idsList], valuesArray: List[sampleRec])

case class idsList(id: Long, rep: Int)

case class sampleRec(sampleArray: List[repRec])

case class repRec(repArray: List[Double])