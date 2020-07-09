
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

object DatabasePrepare {

    /** 
    * Connect to the PostgreSQL database, clear the original image data,
    * and create a new data table which holds the image data information + the high water mark information.
    */
    def prepareCase(strServer : String, strPort : String, strCaseDB : String) {
        // open the connection to the database
        @transient val cPrepare = Database.getConnection(strServer, strPort, strCaseDB)
        
        // clear the image file names
        val clear_names = cPrepare.prepareStatement("delete from tsk_image_names")
        clear_names.execute()

        // create the new table (image table + high water mark information)
        val create = cPrepare.prepareStatement("create table if not exists tsk_image_sizes (obj_id bigint, name text, sequence integer, size bigint)")
        create.execute()

        // clear the new table
        // TODO : is this really necessary? I'm assuming we're doing it because we're not building the table from scratch for each run for real...
        val clear_sizes = cPrepare.prepareStatement("delete from tsk_image_sizes")
        clear_sizes.execute()

        // close the connection to the database
        cPrepare.close()
    }

    def prepareErrorHandling(strServer : String, strPort : String, strCaseDB : String) {
        // open the connection to the database
        @transient val ePrepare = Database.getConnection(strServer, strPort, strCaseDB)
        
        val drop_error_table = ePrepare.prepareStatement("drop table if exists tsk_streaming_file_errors")
        drop_error_table.execute()
        // clear the image file names
        val create_error_table = ePrepare.prepareStatement("create table if not exists tsk_streaming_file_errors (file_id bigint, error_offset bigint)")
        create_error_table.execute()
        
        val index_error_table = ePrepare.prepareStatement("""create index idx_tsk_streaming_file_errors on tsk_streaming_file_errors ("error_offset")""")
        index_error_table.execute()
        
        index_error_table.close()

        // close the connection to the database
        ePrepare.close()
    }
    def prepareFileTable(strServer : String, strPort : String, strCaseDB : String) {
        // open the connection to the database
        @transient val ePrepare = Database.getConnection(strServer, strPort, strCaseDB)
        
        val drop_file_table = ePrepare.prepareStatement("drop table if exists desh_files")
        drop_file_table.execute()
        // clear the image file names
        val create_file_table = ePrepare.prepareStatement("create table if not exists desh_files( file_id bigint, status integer, added timestamp, removed timestamp, started timestamp, finished timestamp ) ")
        create_file_table.execute()
        
        val index_file_table = ePrepare.prepareStatement("""create index desh_files_idx1 on desh_files("file_id")""")
        index_file_table.execute()
        
        index_file_table.close()

        // close the connection to the database
        ePrepare.close()
    }
    def prepareHighwater(strServer : String, strPort : String, strCaseDB : String) {
        // open the connection to the database
        val hwPrepare = Database.getConnection(strServer, strPort, strCaseDB)
        
        val highwater = hwPrepare.createStatement()
        
        highwater.execute( "create table if not exists highwatermark ( previous bigint, current bigint, current_seq bigint, current_name text )")
        highwater.execute( "delete from highwatermark")
        highwater.execute( "insert  into highwatermark values ( -1 , -1, -1, '' )")
        
        highwater.execute( """
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
    update highwatermark set previous = hw_table.current, current = hw, current_seq=hw_seq, current_name=hw_name;
END IF;
return query select t1.obj_id as obj_id from tsk_file_layout t1, tsk_files t2, tsk_fs_info t3 where t1.obj_id = t2.obj_id and t2.dir_type != 3 and t2.fs_obj_id = t3.obj_id and name not like '$%' group by t1.obj_id having max(byte_start + byte_len + img_offset) >= file_hw_before and max(byte_start + byte_len + img_offset) < file_hw_after;
END;
$$ LANGUAGE plpgsql;
""" );
        
        hwPrepare.close()
    }
}
