
package org.desh.utils

import scala.sys.process._
import scala.collection.JavaConversions._

import org.sleuthkit.autopsy.coreutils.Logger
import org.desh.worker.pipeline.PipelineStreamProcessor
import org.desh.worker.FileWorkerAppStuff

// java imports
import java.util.{ArrayList,List,Properties,SortedSet,Date}
import java.io.{File,FileOutputStream}
import java.sql.{DriverManager,Connection,PreparedStatement,Timestamp}

// scala imports
import scala.collection.mutable.ListBuffer

import org.apache.commons.codec.digest.DigestUtils

// autopsy imports
import org.sleuthkit.autopsy.modules.hashdatabase.ClusterHashDbIngestModule
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

// HashDB imports
import org.sleuthkit.autopsy.casemodule.Case
import java.util.logging.Level


// the Cluster Case Mapper was created by @zwinkelm to help rebuild logical files from HDD image pieces

/** handles autopsy module running stuff */
class AutopsyWrapper(processor: PipelineStreamProcessor, hashIngestModule: ClusterHashDbIngestModule, stuff: FileWorkerAppStuff) extends Serializable {

    /** process a file with a particular file ID */
    def processFile(longFileID: Long, error_insert: PreparedStatement, error_status_update: PreparedStatement): ArrayList[java.util.Date] = 
    {
        val dates = new ArrayList[java.util.Date]
        dates.add( new java.util.Date() )
        // logging
        println("\t\tAttempting to Rebuild File ID: " + longFileID + "...")
        // get the abstract file from the case data (given the ID) and writ the abstract file as a logical file to the filesystem
        dates.add( new java.util.Date() )

        val abstractFile = stuff.db.getAbstractFileById(longFileID)

        // attempt to hash the file
        val hashResult = hashIngestModule.debugProcess(abstractFile)

        println("Hash Result for " + abstractFile.toString() + ": " + hashResult)

        // if it failed, store error
        if (hashResult.indexOf("Error") != -1) {

            var offset = 0: Long
            val offset_pattern_1 = """ext2fs_dinode_load: Inode [\d]+ from ([\d]+)""".r.unanchored
            val offset_pattern_2 = """tsk_fs_attr_read_type: offset: ([\d]+)\s+Len: [\d]+""".r.unanchored
            hashResult match {
              case offset_pattern_1(offset_match) => offset = offset_match.toLong
              case offset_pattern_2(offset_match) => offset = offset_match.toLong
              case _ => ;
            }

            error_insert.setLong(1, longFileID)
            error_insert.setLong(2, offset)
            error_insert.setLong(3, stuff.currentHW)
            error_insert.execute()
            error_status_update.setLong(1, longFileID)
            error_status_update.execute()

            println("File is not available yet, skipping. " + abstractFile.toString())

        // otherwise try to process in pipeline
        } else {

            // process file through pipeline
            val processResult = processor.processFile(longFileID)
            dates.add( new java.util.Date() )
            // If file failed to process, store error
            if (processResult.indexOf("Error") != -1 || processResult.indexOf("Exception") != -1) {
                var offset = 0:Long
                val offset_pattern_1 = """ext2fs_dinode_load: Inode [\d]+ from ([\d]+)""".r.unanchored
                val offset_pattern_2 = """tsk_fs_attr_read_type: offset: ([\d]+) Len: [\d]+""".r.unanchored
                processResult match {
                    case offset_pattern_1( offset_match ) =>  offset = offset_match.toLong
                    case offset_pattern_2( offset_match ) =>  offset = offset_match.toLong
                    case _ => ;
                }   
                error_insert.setLong( 1, longFileID )
                error_insert.setLong( 2, offset )
                error_insert.execute()
            }   
            // Perform keyword search on abstract file.
            dates.add( new java.util.Date() )
            // log result
            println(processResult)

        }

        return dates
    }
}
