
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


// HashUtil returns the hash for the data in a Kafka message
object HashUtil extends Serializable {
    def hashMessage(message: ImageBlock): String = 
    {
        DigestUtils.md5Hex(message.getData().array())
    }
}
