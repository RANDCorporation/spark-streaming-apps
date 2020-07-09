
package org.desh.worker

import org.desh.worker.pipeline.PipelineStreamProcessor
import org.desh.utils.Clock.{clock, showClock}
import org.desh.utils.AutopsyWrapper
import org.desh.utils.CaseUtil
import org.desh.database.Database
import org.openide.util.Lookup

import scala.sys.process._
import scala.collection.JavaConversions._
import org.sleuthkit.autopsy.coreutils.Logger
import org.sleuthkit.autopsy.keywordsearch.KeywordSearchModuleFactory
import org.sleuthkit.autopsy.modules.hashdatabase.HashDbManager.HashDb

// java imports
import java.util.{ ArrayList, List, Properties, SortedSet, Date }
import java.io.{ File, FileOutputStream }
import java.sql.{ DriverManager, Connection, PreparedStatement, Timestamp }
import java.nio.file.Files
import java.nio.file.FileSystems
import java.nio.file.StandardCopyOption

// scala imports
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap

import org.apache.commons.codec.digest.DigestUtils

// autopsy imports
import org.sleuthkit.autopsy.casemodule.CaseMetadata
import org.sleuthkit.autopsy.ingest.IngestManager
import org.sleuthkit.autopsy.core.UserPreferences
import org.sleuthkit.autopsy.core.RuntimeProperties
import org.sleuthkit.datamodel.CaseDbConnectionInfo
import org.sleuthkit.datamodel.TskData.DbType
import org.sleuthkit.datamodel.SleuthkitCase
import org.sleuthkit.datamodel.SleuthkitJNI
import org.sleuthkit.datamodel.Image
import org.sleuthkit.autopsy.datamodel.ContentUtils
import org.sleuthkit.autopsy.casemodule.ClusterCase
import org.sleuthkit.autopsy.coreutils.PlatformUtil
import org.sleuthkit.autopsy.keywordsearch.Server

// Keyword Search imports
import org.sleuthkit.autopsy.keywordsearch.KeywordSearch
import org.sleuthkit.autopsy.keywordsearch.KeywordSearchJobSettings
import org.sleuthkit.autopsy.keywordsearch.KeywordSearchIngestModule

// HashDB imports
import org.sleuthkit.autopsy.modules.hashdatabase.ClusterHashDbIngestModule
import org.sleuthkit.autopsy.modules.hashdatabase.HashDbManager
import org.sleuthkit.autopsy.modules.hashdatabase.HashLookupModuleSettings
import org.sleuthkit.autopsy.casemodule.Case
import java.util.logging.Level

class FileWorkerAppStuff(
  var caseMetadata: CaseMetadata = null,
  var db: SleuthkitCase = null,
  var currentHW: Long = -1L,
  var strCaseDir: String = null,
  var caseImages: HashMap[java.lang.Long,Image] = new HashMap[java.lang.Long,Image](),
  var openedCase: ClusterCase = null,
  var autopsyWrapper: AutopsyWrapper = null
);

object FileWorkerApp {

    def main(args: Array[String]) {

        // parse CLI arguments
        val workerArgs = new FileWorkerArgs()
        workerArgs.parse(args)
        println("FileWorkerApp Arguments: \n" + workerArgs.toString())

        val stuff = new FileWorkerAppStuff()

        RuntimeProperties.setRunningWithGUI(false)

        if (args.length > 6) {
            Thread.sleep(1000 * workerArgs.sleep)
        }

        val logDir = workerArgs.strCaseDir/*= if (workerArgs.logDirectory != null) {
            workerArgs.logDirectory
        }
        else {
            workerArgs.strCaseDir
        }*/

        if (args.length > 7) {
            Logger.setLogDirectory(logDir)
        }

        SleuthkitJNI.blnDigi_ = true;
        
        val logger = Logger.getLogger("FileWorkerApp")

        logger.log(Level.SEVERE, "Initializing FileWorkerApp")

        val strCaseName = workerArgs.strCaseName
        val strCaseDir = workerArgs.strCaseDir
        var strCaseFilePath = workerArgs.strCaseFilePath

        // check ready
        checkReady(workerArgs.strDatabase, workerArgs.strDatabasePort, strCaseFilePath, strCaseName)

        var strBackupCaseFilePath = strCaseFilePath + "_backup"
        val strCaseDB = CaseUtil.getDatabaseName(strBackupCaseFilePath)

        val conn = Database.getConnection(workerArgs.strDatabase, workerArgs.strDatabasePort, workerArgs.strCaseDB, workerArgs.strDatabaseUser, workerArgs.strDatabasePasswd)
        var run = true
        val done = conn.prepareStatement("update desh_files set status = 3, finished = ? where file_id = ?")
        val start = conn.prepareStatement("update desh_files set status = 2, started = ? where file_id = ?")
        val error_insert = conn.prepareStatement("insert into tsk_streaming_file_errors values (?, ?)")
        val error_status_update = conn.prepareStatement("update desh_files set error_status = 1 where file_id = ?")
        val hw = conn.prepareStatement("select current_seq from highwatermark")

        // initialize pipeline stream processor
        val processor = new PipelineStreamProcessor(workerArgs)
        processor.initialize()

        // open case
        openCase(workerArgs, done, start, error_insert, conn, stuff)

        // HashDB Ingest Module for verifying availability of logical files
        val knownGood = new ArrayList[HashDb]()
        val knownBad = new ArrayList[HashDb]()
        val hashSettings = new HashLookupModuleSettings(true, knownGood, knownBad)
        val hashIngestModule = new ClusterHashDbIngestModule(hashSettings, stuff.openedCase.getSleuthkitCase())

        // initialize the processor's ingest job
        processor.startIngestJob();

        val autopsyWrapper = new AutopsyWrapper(processor, hashIngestModule, stuff)

        var lastHW: Long = -1;
        while (run) {
            println("refreshing cases")
            refreshCaseImages( stuff )

            println("checking hw")

            val hwRS = hw.executeQuery()
            while (hwRS.next()) {
		stuff.currentHW = hwRS.getLong("current_seq");
	        println("\t Current HW:" + stuff.currentHW)
		if (stuff.currentHW > lastHW) {
		  lastHW = stuff.currentHW;
		}
            }
            val files = getNextFiles(conn)
            if (files.size == 0) {
                println("\tNo files to process - taking a 10 second nap")
                Thread.sleep(10 * 1000)
            } else {
                processFiles(autopsyWrapper, workerArgs, stuff, files, done, start, error_insert, error_status_update, conn)
            }
        }

        closeCase(stuff)
    }

    def checkReady(strServer: String, strPort: String, strCaseFilePathOrig: String, strCaseName: String) = {
        var ready = false
        while (!ready) {

            if (Files.exists(FileSystems.getDefault().getPath(strCaseFilePathOrig))) 
            {
                val strBackupPath = strCaseFilePathOrig + "_backup"
                Files.copy(FileSystems.getDefault().getPath(strCaseFilePathOrig), FileSystems.getDefault().getPath(strBackupPath), StandardCopyOption.REPLACE_EXISTING)
                var strCaseFilePath = strBackupPath
                val strCaseDB = CaseUtil.getDatabaseName(strCaseFilePath)

                var connected = false
                while (!connected) {
                    var conn: Connection = null
                    try {
                        Class.forName("org.postgresql.Driver")
                        println("connecting to db:" + strServer, strPort, strCaseDB)
                        conn = Database.getConnection(strServer, strPort, strCaseDB)
                        println("checking for desh_files table")
                        val check = conn.createStatement()
                        check.executeQuery("select count(*) from desh_files")
                        connected = true
                        ready = true
                        println("check ready")
                    } catch {
                        case ex: Throwable => {
                            println("database check connection exception - sleeping for 10 sec")
                            println(ex.toString())
                            Thread.sleep(10000)
                        }
                    } finally {
                        if (conn != null) { conn.close() }
                    }
                }

            } else {
                println("Case file doesn't exist yet - sleeping for 10 sec")
                Thread.sleep(10000)
            }
        }
    }

    def getNextFiles(conn: Connection): ArrayList[Long] = {
        println("getting next files to process")
        conn.setAutoCommit(false);
        val get_next = conn.prepareStatement("select * from desh_files where status = 0 limit 1")
        val claim = conn.prepareStatement("update desh_files set status = 1, removed = ? where file_id = ?")
        val next = get_next.executeQuery()
        val file_ids = new ArrayList[Long]
        val removedDate = new Date
        val removedTimestamp = new Timestamp(removedDate.getTime())
        while (next.next()) {
            val file_id = next.getLong("file_id")
            println("\tfile id:" + file_id)
            claim.setTimestamp(1, removedTimestamp)
            claim.setLong(2, file_id)
            claim.execute()
            file_ids.add(file_id)
        }
        conn.commit()
        conn.setAutoCommit(true);
        return file_ids
    }

    def processFile(longFileID: Long, autopsyWrapper: AutopsyWrapper, error_insert: PreparedStatement, error_status: PreparedStatement,
        timestamps: ArrayList[java.util.Date], tags: ArrayList[String], aggregateGroups: ArrayList[String]) {
        try {
            val dates = autopsyWrapper.processFile(longFileID, error_insert, error_status)
            clock("start logical file", timestamps, tags, aggregateGroups, "start", date = dates.get(0))
            clock("abstract file", timestamps, tags, aggregateGroups, "abstract", date = dates.get(1))
            clock("hash logical file", timestamps, tags, aggregateGroups, "hash logical", date = dates.get(2))
            clock("keyword search logical file", timestamps, tags, aggregateGroups, "keyword search logical", date = dates.get(3))
        } catch {
            case ex: Throwable => {
                println("Exception/Error processing: " + longFileID)
                println( ex.toString() )
                ex.printStackTrace();
            }
        }
    }

    def refreshCaseImages(stuff: FileWorkerAppStuff) {
        println( "refreshing case images" )

        val images = stuff.db.getImages();

        for (image <- images ) {
            println("processing image:" + image.getId())

            if( stuff.caseImages.containsKey( image.getId() ) == false ){
                println("new image: " + image.getId() )
                if( image.getPaths() != null && image.getPaths().length > 0 ){
                    println("has paths: " + image.getPaths().length )
                    image.getImageHandle()
                    stuff.caseImages.put( image.getId(), image )
                }
            }
            else{
                println("existing image: " + image.getId() + " appending new files" )
                val existingImage = stuff.caseImages( image.getId() )
                existingImage.appendImageFiles()
                println("existing image: " + image.getId() + " new count:" + existingImage.getPaths().length )
            }
        }
    }

    def openCase(workerArgs: FileWorkerArgs, done: PreparedStatement, start: PreparedStatement, error_insert: PreparedStatement, conn: Connection, stuff: FileWorkerAppStuff) {

        val strCaseName = workerArgs.strCaseName
        stuff.strCaseDir = workerArgs.strCaseFilePath
        val strCaseDB = CaseUtil.getDatabaseName(workerArgs.strCaseFilePath)
        val info = new CaseDbConnectionInfo(workerArgs.strDatabase, workerArgs.strDatabasePort, workerArgs.strDatabaseUser, workerArgs.strDatabasePasswd, DbType.POSTGRESQL)
        UserPreferences.setDatabaseConnectionInfo(info)
        //RuntimeProperties.setRunningWithGUI(false)
        val casePath = FileSystems.getDefault().getPath(workerArgs.strCaseFilePath)
        stuff.caseMetadata = new CaseMetadata(casePath)
        val caseName = stuff.caseMetadata.getCaseName()
        val caseNumber = stuff.caseMetadata.getCaseNumber()
        val examiner = stuff.caseMetadata.getExaminer()
        val caseType = stuff.caseMetadata.getCaseType()
        val caseDir = stuff.caseMetadata.getCaseDirectory()
        stuff.db = SleuthkitCase.openCase(stuff.caseMetadata.getCaseDatabaseName(), UserPreferences.getDatabaseConnectionInfo(), caseDir)
        stuff.openedCase = new ClusterCase(stuff.caseMetadata)
    }
    
    def closeCase(stuff: FileWorkerAppStuff) {
        // close all the Autopsy stuff too
        stuff.db.close()
        // causes problems because of write
        stuff.openedCase.getServices().getFileManager().close()
        stuff.openedCase.getServices().getTagsManager().close()
        if (stuff.openedCase.getServices().getKeywordSearchService() != null) {
            stuff.openedCase.getServices().getKeywordSearchService().close()
        }
    }

    def processFiles(autopsyWrapper: AutopsyWrapper, workerArgs: FileWorkerArgs, stuff: FileWorkerAppStuff, files: ArrayList[Long], done: PreparedStatement, start: PreparedStatement, error_insert: PreparedStatement, error_status_update: PreparedStatement, conn: Connection) {

        val strCaseName = workerArgs.strCaseName
        val strCaseDir = workerArgs.strCaseDir
        val strCaseFilePath = workerArgs.strCaseFilePath
        val strCaseDB = workerArgs.strCaseDB

        val timestamps = new ArrayList[java.util.Date]
        val tags = new ArrayList[String]
        val aggregateGroups = new ArrayList[String]
        clock( "partitionStart", timestamps, tags, aggregateGroups )
        clock( "partitionCaseOpen", timestamps, tags, aggregateGroups )

        clock("partitionPipelineSetup", timestamps, tags, aggregateGroups)
        // logging
        println("  starting records...")
        // after we've written in all the partition information, operate on the individual kafka messages
        // create the autopsy wrapper

        clock( "partitionFilesProcessed", timestamps, tags, aggregateGroups )
        // process each file
        files.foreach(longFileID => 
        {
                val startDate = new Date
                val startTimestamp = new Timestamp( startDate.getTime() )
                start.setTimestamp(1, startTimestamp )
                start.setLong( 2, longFileID ) 
                start.execute()
                processFile( longFileID, autopsyWrapper, error_insert, error_status_update, timestamps, tags, aggregateGroups )
                val doneDate = new Date
                val doneTimestamp = new Timestamp( doneDate.getTime() )
                done.setTimestamp(1, doneTimestamp )
                done.setLong( 2, longFileID ) 
                done.execute()
        })
        // close partition connection_2
        clock( "partitionDone", timestamps, tags, aggregateGroups )
        showClock( timestamps, tags, aggregateGroups )
        println( "Driver 5")
        println( "Driver 6")
    }
}

