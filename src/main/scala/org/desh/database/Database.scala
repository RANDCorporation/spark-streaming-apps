
package org.desh.database

import org.desh.utils._
import scala.sys.process._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// java imports
import java.util.{ArrayList,List,Properties,SortedSet,Date}
import java.io.{File,FileOutputStream}
import java.sql.{DriverManager,Connection,PreparedStatement, Timestamp}

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

/** Database helper functions */
object Database {

    /** returns a connection to the database, keeping track of the connection string for us */
    def getConnection(strServer : String, strPort : String, strCaseDB : String, strDBUser : String = "postgres", strDBPasswd : String = "postgres") : Connection = {
        return DriverManager.getConnection("jdbc:postgresql://" + strServer + ":" + strPort + "/" + strCaseDB, strDBUser, strDBPasswd)
    }
}
