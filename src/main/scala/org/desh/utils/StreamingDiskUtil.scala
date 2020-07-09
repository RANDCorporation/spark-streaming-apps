
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


/** handles highwater mark stuff */
object StreamingDiskUtil extends Serializable {
    // determine the current high water mark
    
    def updateImageGeometry(statement_names: java.sql.PreparedStatement, filename: String): Unit = 
    {
        // add the new data for the image file to the statement for tsk_image_names
        // NOTE: fields: obj_id bigint, name text, sequence integer, size bigint
        val fileNumber = Integer.parseInt(filename.substring(filename.lastIndexOf(".") + 1))
        
        println("Inserted into tsk_image_names: " + filename)
        
        statement_names.setInt(1, 1)
        statement_names.setString(2, filename)
        statement_names.setInt(3, fileNumber)
        statement_names.execute()
    }                          
    def updateImageSizes(statement_sizes: java.sql.PreparedStatement, filename: String, length: Integer): Unit = 
    {
        println("Inserted into tsk_image_sizes: " + filename)

        // add the new data for the image file to the statement for tsk_image_sizes
        // NOTE: fields: obj_id bigint, name text, sequence integer, size bigint, highwatermark integer
        val fileNumber = Integer.parseInt(filename.substring(filename.lastIndexOf(".") + 1))
        statement_sizes.setInt(1, 1)
        statement_sizes.setString(2, filename)
        statement_sizes.setInt(3, fileNumber)
        statement_sizes.setInt(4, length)
        statement_sizes.execute()
    }
}
