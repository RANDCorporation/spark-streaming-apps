
package org.desh.utils

// scala imports
import scala.collection.mutable.ListBuffer

// kafka imports
import org.rand.kafkaproducer.ImageBlock
import kafka.serializer._
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

// spark imports
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.avro.generic._
import org.apache.avro.specific._
import org.apache.avro.io._
import org.apache.commons.codec.digest.DigestUtils


// AvroUtil decodes the Kafka message (which was compressed via Avro originally!)
object AvroUtil extends Serializable {
    // grab the data reader as a class variable
    val reader = new GenericDatumReader[ImageBlock](ImageBlock.getClassSchema())
            
    // decode the message
    def decodeMessage(bytes:Array[Byte]): ImageBlock = 
    {
        val decoder = DecoderFactory.get.binaryDecoder(bytes, null)
        println("reader: " + reader.getClass.toString())
        (reader.read(null, decoder)).asInstanceOf[ImageBlock] //This cast should not be necessary
    }
}
