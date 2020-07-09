
package org.desh.worker.pipeline

import org.desh.worker.FileWorkerArgs
import java.util.ArrayList
import java.util.Collections
import java.util.List

import org.apache.commons.lang.exception.ExceptionUtils
import org.sleuthkit.autopsy.casemodule.Case
import org.sleuthkit.autopsy.casemodule.CaseActionException
import org.sleuthkit.autopsy.casemodule.ClusterCase
import org.sleuthkit.autopsy.coordinationservice.CoordinationService
import org.sleuthkit.autopsy.core.UserPreferences
import org.sleuthkit.autopsy.core.RuntimeProperties
import org.sleuthkit.autopsy.coreutils.PlatformUtil
import org.sleuthkit.autopsy.events.MessageServiceConnectionInfo
import org.sleuthkit.autopsy.ingest.DataSourceIngestJob
import org.sleuthkit.autopsy.ingest.FileIngestPipeline
import org.sleuthkit.autopsy.ingest.FileIngestPipeline.PipelineModule
import org.sleuthkit.autopsy.ingest.IngestJob
import org.sleuthkit.autopsy.ingest.IngestJobSettings
import org.sleuthkit.autopsy.ingest.IngestManager
import org.sleuthkit.autopsy.keywordsearch.KeywordSearch
import org.sleuthkit.autopsy.keywordsearch.Server
import org.sleuthkit.datamodel.AbstractFile
import org.sleuthkit.datamodel.CaseDbConnectionInfo
import org.sleuthkit.datamodel.Content
import org.sleuthkit.datamodel.TskCoreException
import org.sleuthkit.datamodel.TskData.DbType
import org.sleuthkit.datamodel.SleuthkitJNI
import org.sleuthkit.autopsy.core.UserPreferences

import scala.collection.JavaConverters._
import collection.JavaConversions._

/**
 * Class to run files through Autopsy's ingest pipeline.
 *  
 * @author ttran
 */
class PipelineStreamProcessor(workerArgs: FileWorkerArgs) {

    var job: IngestJob = null;

    def initialize() {
        // Set the PlatformUtil paths manually
        PlatformUtil.setInstallPath(workerArgs.strInstallPath)
        PlatformUtil.setUserDirectory(workerArgs.strUserDirectory)
        
        // Set solr folder manually
        Server.setDefaultSolrFolder(workerArgs.strSolrPath)

        // set database info
        val dbInfo = new CaseDbConnectionInfo(
                workerArgs.strDatabase,
                workerArgs.strDatabasePort,
                workerArgs.strDatabaseUser,
                workerArgs.strDatabasePasswd,
                DbType.POSTGRESQL)
        UserPreferences.setDatabaseConnectionInfo(dbInfo)

        // set messaging service info
        val msgInfo = new MessageServiceConnectionInfo(
                workerArgs.strActiveMQHost, 
                workerArgs.strActiveMQPort.toInt,
                workerArgs.strActiveMQUser, 
                workerArgs.strActiveMQPasswd)
        UserPreferences.setMessageServiceConnectionInfo(msgInfo);

        // tell the file ingest pipeline that we will be processing files manually
        FileIngestPipeline.setRunningCluster(true);


        // set solr info
        UserPreferences.setIndexingServerHost(workerArgs.strSolrHost);
        UserPreferences.setIndexingServerPort(Integer.parseInt(workerArgs.strSolrPort));

        // set multi-user mode enabled
        UserPreferences.setIsMultiUserModeEnabled(true);
        
        println("IngestManager with GUI: " + RuntimeProperties.runningWithGUI())

        //set the zookeeper server
        CoordinationService.setZooKeeperServerPort(2181);
        CoordinationService.setZooKeeperServerHost("kafka-service");

        // open the case
        Case.openAsCurrentCase(workerArgs.strCaseFilePath);

        // open the keyword search core
        val strCoreName = Case.getCurrentCase().getTextIndexName();
        //KeywordSearch.createServer(workerArgs.strSolrPath, "java")
        //KeywordSearch.openCore(workerArgs.strSolrHost, workerArgs.strSolrPort, workerArgs.strCaseFilePath, strCoreName);
    }

    /** 
     * Load up the modules and start the ingest job to get ready for file processing.
     */
    def startIngestJob() {
        var newList = new java.util.ArrayList[Content]()
        val contents = Collections.synchronizedList(newList);
        contents.addAll(Case.getCurrentCase().getSleuthkitCase().getImages());   
        val settings = new IngestJobSettings("org.sleuthkit.autopsy.casemodule.AddImageWizardIngestConfigPanel");
        settings.load();
        this.job = new IngestJob(contents, settings, false);

        // startUp the modules
        val jobs = this.job.dataSourceJobs
        for ((id, dataSourceJob) <- jobs) {
            for (fip <- dataSourceJob.getFileIngestPipelines()) {
                fip.startUp();
            }
        }

        // allow partial loading of file
        SleuthkitJNI.setPartialOk(true)

        IngestManager.getInstance().startIngestJob(this.job);
    }
    
    /** Process a file by its ID with the currently opened ClusterCase */
    def processFile(longFileID: Long): String = {

        val openedCase = Case.getCurrentCase()
        val abstractFile = openedCase.getSleuthkitCase().getAbstractFileById(longFileID)

        var ret = "Processing file: " + abstractFile.toString() + "\n";

        // let's try running all the selected ingest modules
        val jobs = this.job.dataSourceJobs
        for ((id, dataSourceJob) <- jobs) {
            // TODO: equivelent to calling:
            // dataSourceJob.process(fit);

            // TODO: Should be one filepipeline
            val fip = dataSourceJob.getFileIngestPipelines().get(0);
            
            // For all modules defined in config settings
            for (module <- fip.getModules()) {
                module.process(abstractFile);
                ret = new String(ret + "    Processed module: " + module.getDisplayName() + "...\n")
            }
        }

        return ret;
    }
}

