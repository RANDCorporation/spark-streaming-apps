import java.nio.ByteBuffer

import scala.sys.process._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.desh.utils.CaseUtil

// java imports
import java.util.{ArrayList,List,Properties,SortedSet,Date}
import java.io.{File,FileOutputStream}
import java.sql.{DriverManager,Connection,PreparedStatement, Timestamp}
import java.nio.channels.ClosedChannelException

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
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.avro.generic._
import org.apache.avro.specific._
import org.apache.avro.io._
import org.apache.commons.codec.digest.DigestUtils

// autopsy imports
import org.sleuthkit.autopsy.ingest.IngestManager
import org.sleuthkit.autopsy.core.UserPreferences
import org.sleuthkit.datamodel.CaseDbConnectionInfo
import org.sleuthkit.datamodel.TskData.DbType
import org.sleuthkit.datamodel.SleuthkitCase
import org.sleuthkit.datamodel.SleuthkitJNI
import org.sleuthkit.autopsy.datamodel.ContentUtils
import org.sleuthkit.autopsy.casemodule.ClusterCase
import org.sleuthkit.autopsy.coreutils.PlatformUtil
import org.sleuthkit.autopsy.keywordsearch.Server

// Keyword Search imports
import org.sleuthkit.autopsy.keywordsearch.KeywordSearch
import org.sleuthkit.autopsy.keywordsearch.KeywordSearchJobSettings
import org.sleuthkit.autopsy.keywordsearch.KeywordSearchIngestModule

// HashDB imports
import org.sleuthkit.autopsy.modules.hashdatabase.HashDbIngestModule
import org.sleuthkit.autopsy.modules.hashdatabase.HashDbManager
import org.sleuthkit.autopsy.modules.hashdatabase.HashLookupModuleSettings
import org.sleuthkit.autopsy.casemodule.Case

// RAND imports
//import clustercasereader.ClusterCaseMapper

// the Cluster Case Mapper was created by @zwinkelm to help rebuild logical files from HDD image pieces

//
// Helper Classes (TODO: pull these out of the notebook and into a JAR)
//

// HashUtil returns the hash for the data in a Kafka message
object HashUtil extends Serializable {
  def hashMessage(message: ImageBlock): String =
    {
      DigestUtils.md5Hex(message.getData().array())
    }
}

// AvroUtil decodes the Kafka message (which was compressed via Avro originally!)
object AvroUtil extends Serializable {
  // grab the data reader as a class variable
  val reader = new GenericDatumReader[GenericData.Record](ImageBlock.SCHEMA$)

  // decode the message
  def decodeMessage(bytes: Array[Byte]): ImageBlock =
    {
      val decoder = DecoderFactory.get.binaryDecoder(bytes, null)
      val genericData = reader.read(null, decoder)
      return new ImageBlock(genericData.get(0).asInstanceOf[CharSequence], genericData.get(1).asInstanceOf[CharSequence], genericData.get(2).asInstanceOf[ByteBuffer])
    }
}

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

/**
* This streaming program is currently the bulk of the work.
* It polls every x seconds for new HDD image data being sent via Kafka messages.
* It takes the data, does some magic, and sends off the HDD image files to the worker nodes.
* The worker nodes, in turn, are asked to start rebuilding logical files from the HDD images.
*/
class StreamingProgram (@transient ssco: StreamingContext) extends Serializable {
  // NOTE: "transient" means "don't serialize this" 
  // this is the streaming program being run every x seconds (on just the head node?)

  /** returns a connection to the database, keeping track of the stupid connection string for us */
  def getConnection(strServer: String, strPort: String, strCaseDB: String): Connection = {
    Class.forName("org.postgresql.Driver")
    return DriverManager.getConnection("jdbc:postgresql://" + strServer + ":" + strPort + "/" + strCaseDB, "postgres", "postgres")
  }

  /**
   * Connect to the PostgreSQL database, clear the original image data,
   * and create a new data table which holds the image data information + the high water mark information.
   */
  def prepareCase(strServer: String, strPort: String, strCaseDB: String) {
    // open the connection to the database
    @transient val cPrepare = this.getConnection(strServer, strPort, strCaseDB)

    // clear the image file names
    val clear_names = cPrepare.prepareStatement("delete from tsk_image_names")
    clear_names.execute()

    // create the new table (image table + high water mark information)
    val create = cPrepare.prepareStatement("create table if not exists tsk_image_sizes (obj_id bigint, name text, sequence integer, size bigint)")
    create.execute()

    val create_idx_1 = cPrepare.prepareStatement("create index if not exists tsk_image_sizes_seq on tsk_image_sizes (sequence)")
    create_idx_1.execute()

    val create_idx_2 = cPrepare.prepareStatement("create index if not exists tsk_image_sizes_id on tsk_image_sizes (obj_id)")
    create_idx_2.execute()

    // clear the new table
    // TODO : is this really necessary? I'm assuming we're doing it because we're not building the table from scratch for each run for real...
    val clear_sizes = cPrepare.prepareStatement("delete from tsk_image_sizes")
    clear_sizes.execute()

    // close the connection to the database
    cPrepare.close()
  }

  def prepareErrorHandling(strServer: String, strPort: String, strCaseDB: String) {
    // open the connection to the database
    @transient val ePrepare = this.getConnection(strServer, strPort, strCaseDB)

    val drop_error_table = ePrepare.prepareStatement("drop table if exists tsk_streaming_file_errors")
    drop_error_table.execute()
    // clear the image file names
    val create_error_table = ePrepare.prepareStatement("create table if not exists tsk_streaming_file_errors (file_id bigint, error_offset bigint, hw bigint)")
    create_error_table.execute()

    val index_error_table = ePrepare.prepareStatement("""create index idx_tsk_streaming_file_errors on tsk_streaming_file_errors ("error_offset")""")
    index_error_table.execute()

    index_error_table.close()

    // close the connection to the database
    ePrepare.close()
  }
  def prepareResidentFileTable(ePrepare: Connection) {
    val populate_file_table = ePrepare.prepareStatement("""insert into desh_files(file_id, status, resident, error_status) 
select t1.obj_id, 0, 1, 0 from tsk_files t1 
left outer join tsk_file_layout t2
on t1.obj_id = t2.obj_id where t2.obj_id is null
and dir_type != 3""" ) 

    populate_file_table.execute()
  }
  def prepareFileTable(strServer: String, strPort: String, strCaseDB: String) {
    // open the connection to the database
    @transient val ePrepare = this.getConnection(strServer, strPort, strCaseDB)

    val drop_file_table = ePrepare.prepareStatement("drop table if exists desh_files")
    drop_file_table.execute()
    // clear the image file names
    val create_file_table = ePrepare.prepareStatement("create table if not exists desh_files( file_id bigint, resident integer, status integer, added timestamp, hw_before bigint, hw_after bigint, removed timestamp, started timestamp, finished timestamp, error_status integer ) ")
    create_file_table.execute()

    val index_file_table = ePrepare.prepareStatement("""create index desh_files_idx1 on desh_files("file_id")""")
    index_file_table.execute()

    index_file_table.close()

    // close the connection to the database
    ePrepare.close()
  }
  def prepareHighwater(strServer: String, strPort: String, strCaseDB: String) {
    // open the connection to the database
    val hwPrepare = this.getConnection(strServer, strPort, strCaseDB)

    val highwater = hwPrepare.createStatement()

    highwater.execute("create table if not exists highwatermark ( previous bigint, current bigint, current_seq bigint, current_name text, status integer )")
    highwater.execute("create table if not exists highwatermark_log ( previous bigint, current bigint, current_seq bigint, current_name text )")
    highwater.execute("delete from highwatermark")
    highwater.execute("insert  into highwatermark values ( -1 , -1, -1, '', 0 )")

    highwater.execute("""
create or replace function get_highwater_mark() returns setof bigint as $$
declare 
    chunks RECORD;
    hw_table RECORD;
    mft_entry RECORD;
    hw bigint:= 0;
    hw_seq bigint:= 0;
    hw_name text:= 0;
    file_hw_before bigint:= 0;
    file_hw_after bigint:= 0;
    prev_seq bigint:= -1;
BEGIN
select * from highwatermark into hw_table;
select max(byte_start + byte_len) as mft_hw from tsk_files t1, tsk_file_layout t2 where t1.obj_id = t2.obj_id and name = '$MFT' and parent_path = '/' into mft_entry;
for chunks in select row_number() over (order by sequence asc ) as row_num, * from tsk_image_sizes order by sequence asc LOOP
    if chunks.row_num != chunks.sequence + 1 then
        exit;
    else
        hw= hw + chunks.size;
        hw_seq = chunks.sequence;
        hw_name = chunks.name;
    end if;
END LOOP;
IF hw > hw_table.current and (mft_entry is null or mft_entry.mft_hw < hw) THEN
    file_hw_before = hw_table.current;
    file_hw_after = hw;
    insert into highwatermark_log (previous, current, current_seq, current_name ) values ( hw_table.current, hw, hw_seq, hw_name);
    update highwatermark set previous = hw_table.current, current = hw, current_seq=hw_seq, current_name=hw_name;
END IF;
return query select t1.obj_id as obj_id from tsk_file_layout t1, tsk_files t2, tsk_fs_info t3 where t1.obj_id = t2.obj_id and t2.dir_type != 3 and t2.fs_obj_id = t3.obj_id and name not like '$%' group by t1.obj_id having max(byte_start + byte_len + img_offset) >= file_hw_before and max(byte_start + byte_len + img_offset) < file_hw_after;
END;
$$ LANGUAGE plpgsql;
""" );

    hwPrepare.close()
  }
  /**
   * This is the main method of the Spark streaming program.
   * Via some voodoo magic that @zwinkelm might be able to explain, stuff at the top is run once,
   * while stuff below the "createDirectStream()" command seems to be run each iteration (every x seconds).
   * How and why is still a mystery to @akarode.
   *
   * This method is meant to recieve the image files from the Kafka queue, store them as appropriate,
   * and currently it also rebuilds the logical files from the HDD image files.
   */
  def run(case_topic: String, prepare: Boolean, strLocalMemFS: String, strLocalFS: String, strFilesystem: String, sc: SparkContext, strServer: String, strSolrServer: String, strPortKafka: String, strKafkaServer: String, intPartitions: Int, strPortDB: String) {

    val strPortSOLR = "8983"

    // given the topic, grab the autopsy case name and the file path to it
    val strCaseName = case_topic.replace("topic_case_", "")
    val strCaseDisplayName = strCaseName.dropRight(16)
    val strCaseDir = strFilesystem + strCaseDisplayName
    val strCaseFilePath = strCaseDir + File.separator + strCaseDisplayName + ".aut"
    val makeDirCommand = "mkdir -p " + strLocalFS + "chunks/" + case_topic
    makeDirCommand.!!
    val chmodCommand = "chmod 777 " + strLocalFS + "chunks/" + case_topic
    chmodCommand.!!

    // Solr stores its index in a data directory, create based off of the
    // case's topic name
    val solrDataDir = strCaseDir + "/deshcluster.rand.org/ModuleOutput/keywordsearch/data/"

    // solr can create the index directory in the data dir, but not the directory
    // where the index needs to be created so create the data dir and make permissions
    // available for solr to write to
    val solrDataDirFile = new File(solrDataDir)
    if (!solrDataDirFile.exists()) {
      val solrMakeDirCommand = "mkdir -p " + solrDataDir
      solrMakeDirCommand.!!
      val solrChmodCommand = "chmod -R 777 " + solrDataDir
      solrChmodCommand.!!
    }

    val strCaseDB = CaseUtil.getDatabaseName(strCaseFilePath)

    // prepare all the database information for this run
    if (prepare) this.prepareCase(strServer, strPortDB, strCaseDB)

    this.prepareErrorHandling(strServer, strPortDB, strCaseDB)
    this.prepareHighwater(strServer, strPortDB, strCaseDB)
    this.prepareFileTable(strServer, strPortDB, strCaseDB)

    println("Driver 1")
    // TODO : obviously this is some sort of Kafka parameter line, but I don't know what the heck it's doing
    val kafkaParams = Map[String, String](("metadata.broker.list", strKafkaServer + ":" + strPortKafka), ("auto.offset.reset", "smallest"))

    // create a stream given the stream from above, the kafka parameters, and the split set of topics, ignoring the Kakfa headers ("_._2")
    var messages = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssco, kafkaParams, Set(case_topic)).map(_._2)

    println("Driver 2")
    // given the list of messages, decode them with our Avro utility
    val clusterHashedImageBlockMessages = messages.map(bytes => {
      AvroUtil.decodeMessage(bytes)
    })

    println("Driver 3")

    // TODO : we really should be verifying the hashes
    //val verificationHashes = clusterHashedImageBlockMessages.map(message => (message.getFilename(), message.getLocalHash(), HashUtil.hashMessage(message)))
    //verificationHashes.saveAsTextFiles("/mnt/glusterfs/hashed_image_blocks")

    // each set of decoded messages was stored in an RDD, so operate on the RDD
    val postInsertRDDs = clusterHashedImageBlockMessages.transform { rdd =>

      // transform to abstract file IDs and repartition the rdd
      val fileCounts = rdd.mapPartitions(itImageBlocks => {

        val timestamps = new ArrayList[java.util.Date]
        val tags = new ArrayList[String]
        val aggregateGroups = new ArrayList[String]

        // create the list of files to return
        // NOTE: the actual return type will be an iterator over this list, not the list itself

        // open a connection to the database, and prepare the statement to write the image file data to it (and the high water mark data)
        val partition_connection = this.getConnection(strServer, strPortDB, strCaseDB)
        val statement_names = partition_connection.prepareStatement("insert into tsk_image_names values (?, ?, ?)")
        val statement_sizes = partition_connection.prepareStatement("insert into tsk_image_sizes values (?, ?, ?, ?)")

        println("database connection")

        clock("database connection", timestamps, tags, aggregateGroups)

        var lbFileCounts = new ListBuffer[Long]()
        var lngFiles = 0L
        // loop over each of the image blocks
        while (itImageBlocks.hasNext) {

          // grab the image block
          val imageBlock = itImageBlocks.next()

          // write the image block to outputs
          // create the filename on the gluster filesystem and write the image file
          val filename = strLocalFS + "chunks/" + case_topic + "/" + imageBlock.getFilename()
          val filedata = imageBlock.getData().array()

          val filehash = HashUtil.hashMessage(imageBlock)
          clock("hash file", timestamps, tags, aggregateGroups, "hashing")

          val outputStream = new FileOutputStream(filename)
          outputStream.write(filedata)
          outputStream.close()
          println("Wrote chunk: " + imageBlock.getFilename())
          clock("write to filesytem", timestamps, tags, aggregateGroups, "filesystem")

          StreamingDiskUtil.updateImageGeometry(statement_names, filename)
          clock("update geometry", timestamps, tags, aggregateGroups, "database")
          StreamingDiskUtil.updateImageSizes(statement_sizes, filename, filedata.length)
          clock("update sizes", timestamps, tags, aggregateGroups, "database")

          // logging
          val increment = filedata.length
          println("Inserted: " + filename +
            "\tSize: " + increment +
            "\tHash 1: " + imageBlock.getLocalHash() +
            "\tHash 2: " + filehash)
          lngFiles += 1L
        }

        lbFileCounts += lngFiles

        clock("files inserted", timestamps, tags, aggregateGroups)

        partition_connection.close()
        // return the list iterator
        lbFileCounts.toList.iterator
      })
      fileCounts
    }

    postInsertRDDs.repartition(1).foreachRDD { rdd =>
      val rddAbstractFileIDs = rdd.mapPartitions(itFileCounts => {

        val timestamps = new ArrayList[java.util.Date]
        val tags = new ArrayList[String]
        val aggregateGroups = new ArrayList[String]

        val partition_connection = this.getConnection(strServer, strPortDB, strCaseDB)
        // NOTE/TODO: need to check end edge case
        val insert_files = partition_connection.prepareStatement("insert into desh_files (file_id, resident, status, added, hw_before, hw_after, error_status ) values ( ?,0,0,?, ?, ?, 0) ")
        val available_files = partition_connection.prepareStatement("select get_highwater_mark() as obj_id")
        val rsFiles = available_files.executeQuery()

        val hw_log = partition_connection.prepareStatement("select previous, current from highwatermark")
        var hw_before = 0L 
        var hw_after = 0L 
        val hw_log_rs = hw_log.executeQuery() 
        while( hw_log_rs.next() ){ 
          hw_before = hw_log_rs.getLong( "previous" ) 
          hw_after = hw_log_rs.getLong( "current" ) 
        } 

        val image_size = partition_connection.prepareStatement( "select size from tsk_image_info" )
        val image_size_rs = image_size.executeQuery()
        var image_size_val = 0L;
        while( image_size_rs.next() ){
          image_size_val = image_size_rs.getLong( "size" )
        }

        if( (image_size_val/4) >= hw_before && (image_size_val/4) < hw_after ){
          prepareResidentFileTable( partition_connection )
        }

        var reset_resident_errors = false
        var done = false
        if( (image_size_val/2) >= hw_before && (image_size_val/2) < hw_after ){
          reset_resident_errors = true 
          println( "half way reset of errors" )
        }
        else if( image_size_val <= hw_after ){
          reset_resident_errors = true 
          println( "end reset of errors" )
          val done_flag = partition_connection.prepareStatement("update highwater set status = 1")
          done_flag.execute()
          done = true
        }

        if( reset_resident_errors ){
          val reset = partition_connection.prepareStatement( "update desh_files set status = 0 where error_status = 1 and resident = 1")
          reset.execute()
        }

                
        var lbFileIDs = new ListBuffer[Long]()

        clock("hw mark", timestamps, tags, aggregateGroups)

        var batchCount = 0
        var batchLimit = 1000
        var batchDate = new Date
        var batchTimestamp = new Timestamp(batchDate.getTime())
        while (rsFiles.next()) {
          val fileID = rsFiles.getLong("obj_id")
          lbFileIDs += fileID
          insert_files.setLong(1, fileID)
          insert_files.setTimestamp(2, batchTimestamp)
          insert_files.setLong(3, hw_before)
          insert_files.setLong(4, hw_after)
          insert_files.addBatch()
          batchCount = batchCount + 1
          if (batchCount >= batchLimit) {
            insert_files.executeBatch()
            batchCount = 0
          }
        }
        if (batchCount > 0) {
          insert_files.executeBatch()
        }

        clock("file layout entries list created", timestamps, tags, aggregateGroups)
        // logging
        println("Received: " + lbFileIDs.size + " File IDs")

        clock("map partition done", timestamps, tags, aggregateGroups)
        showClock(timestamps, tags, aggregateGroups)
        partition_connection.close()
        // return the list iterator
        if( done ){
          ssco.stop(false, true)
        }
        lbFileIDs.toList.iterator
      })
      val num_files = rddAbstractFileIDs.count()
      println("processed " + num_files + " files")
    }
    println("Driver 6")
    // start the streaming job
    ssco.start()
    ssco.awaitTermination()

  }

  def showClock(timestamps: ArrayList[java.util.Date], tags: ArrayList[String], aggregateGroups: ArrayList[String]) {
    println("\n\n\nclock\n\n")

    var aggregateTime = new java.util.HashMap[String, Long]

    var prevTime = -1L

    for (index <- 0 until timestamps.size()) {
      val tag = tags.get(index)
      val time = timestamps.get(index)
      val aggregateGroup = aggregateGroups.get(index)

      if (prevTime == -1L) {
        println("\telapsed: 0\t" + tag + "\t" + time)
      } else {
        val elapsed = time.getTime() - prevTime

        if (aggregateGroup != null) {
          var newTime = elapsed

          if (aggregateTime.containsKey(aggregateGroup)) {
            newTime += aggregateTime.get(aggregateGroup)
          }
          aggregateTime.put(aggregateGroup, newTime)
        } else {
          println("\telapsed: " + elapsed + "\t" + tag + "\t" + time)
        }

      }
      prevTime = time.getTime()
    }

    val itGroups = aggregateTime.keySet().iterator()
    println("\ngroups\n")

    while (itGroups.hasNext()) {
      val group = itGroups.next()
      val time = aggregateTime.get(group)
      println("\t" + time + "\t" + group)
    }

    println("\n\n\nclock done\n\n")
  }
  def clock(tag: String, timestamps: ArrayList[java.util.Date], tags: ArrayList[String], aggregateGroups: ArrayList[String], aggregateGroup: String = null, date: java.util.Date = null) {
    if (date == null) {
      timestamps.add(new java.util.Date())
    } else {
      timestamps.add(date)
    }
    tags.add(tags.size() + " " + tag)
    aggregateGroups.add(aggregateGroup)
  }
  /** method to stop the streaming job */
  def stop() {
    this.ssco.stop(false, true)
  }
}

object NotSimpleApp {
  def main(args: Array[String]) {
    var intSeconds = args(9).toInt
    val conf = new SparkConf().setAppName("Not Simple Application")
    val sc = new SparkContext(conf)
    val ssco = new StreamingContext(sc, Seconds(intSeconds))
    //var case_str = "topic_case_zev_test_12"
    var case_str = args(0)
    var strLocalMemFS = args(1)
    var strLocalFS = args(2)
    var strFilesystem = args(3)

    var strServer = args(4)
    var strSolrServer = args(5)
    var strPortKafka = args(6)
    strPortKafka = "9092"
    var strKafkaServer = args(7)
    var intPartitions = args(8).toInt
    var strPortDB = "5432"
    if (args.length > 10) {
      strPortDB = args(10)
    }

    val streamProg = new StreamingProgram(ssco)
    streamProg.run(case_str, true, strLocalMemFS, strLocalFS, strFilesystem, sc, strServer, strSolrServer, strPortKafka, strKafkaServer, intPartitions, strPortDB)
  }
}
