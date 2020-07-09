
package org.desh.worker

import org.desh.utils.options.Option
import org.desh.utils.options.Options
import java.io.File
import org.desh.utils.CaseUtil

class FileWorkerArgs {

    // case
    var strCaseName: String = null
    var strCaseDir: String = null
    var strCaseFilePath: String = null
    var strCaseDB: String = null

    // optional
    var sleep: Integer = 0
    var logDirectory: String = null
    var fileId: Long = -1
    
    // postgres
    var strDatabase: String = null
    var strDatabasePort: String = null
    var strDatabaseUser: String = null
    var strDatabasePasswd: String = null
    
    // solr
    var strSolrPort: String = null
    var strSolrHost: String = null
    var strSolrDataDir: String = null
      
    // file paths
    var strInstallPath: String = null
    var strUserDirectory: String = null
    var strSolrPath: String = null
    
    // activemq
    // TODO(ttran): Parameterize
    var strActiveMQHost: String = null
    var strActiveMQPort: String = null
    var strActiveMQUser: String = null
    var strActiveMQPasswd: String = null

    /** setup options to parse */
    def setupArgumentParser(args: Array[String]): Options = {
        
        var options: Options = new Options()

        // config
        options.addOption("case-name", "the name of the case", true)
        options.addOption("case-dir", "the root directory of the case files", true)
        options.addOption("build-dir", "the build directory of autopsy", true)

        // postgres
        options.addOption("postgres-host", "host of the PostgreSQL server", true)
        options.addOption("postgres-port", "port number of the PostgreSQL server", true)
        options.addOption("postgres-user", "username for the PostgreSQL server", true)
        options.addOption("postgres-pass", "password for the PostgreSQL server", true)

        // solr
        options.addOption("solr-host", "host of the solr server", true)
        options.addOption("solr-port", "port number of the solr server", true)

        // activemq
        options.addOption("activemq-host", "host of the active-mq server", true)
        options.addOption("activemq-port", "port of the active-mq server", true)
        options.addOption("activemq-user", "ActiveMQ server username", true)
        options.addOption("activemq-pass", "ActiveMQ server password", true)

        // optional
        options.addOption("sleep", "(optional) time to sleep before starting worker", false)
        options.addOption("log-directory", "(optional) directory to redirect logs", false)
        options.addOption("file-id", "(optional) id of file to process. Default to process all available files", false)

        // parse arguments, fail and print error if parsing fails
        options.parseArgsExitOnFailure(args)
      
        // print arguments
        options.printOptions()
        return options
    }

    def parse(args: Array[String]) {

        // parse argument string
        val options = this.setupArgumentParser(args)

        // parse case name and topic
        this.strCaseName = options.getOptionValue("case-name")

        this.strCaseDir = options.getOptionValue("case-dir") + File.separator + this.strCaseName
        this.strCaseFilePath = this.strCaseDir + File.separator + this.strCaseName + ".aut"
        this.strCaseDB = CaseUtil.getDatabaseName(this.strCaseFilePath)

        // postgres server host and port
        this.strDatabase = options.getOptionValue("postgres-host")
        this.strDatabasePort = options.getOptionValue("postgres-port")
        this.strDatabaseUser = options.getOptionValue("postgres-user")
        this.strDatabasePasswd = options.getOptionValue("postgres-pass")

        // solr server host and port
        this.strSolrHost = options.getOptionValue("solr-host")
        this.strSolrPort = options.getOptionValue("solr-port")

        // activemq
        this.strActiveMQHost = options.getOptionValue("activemq-host")
        this.strActiveMQPort = options.getOptionValue("activemq-port")
        this.strActiveMQUser = options.getOptionValue("activemq-user")
        this.strActiveMQPasswd = options.getOptionValue("activemq-pass")

        // file paths
        this.strInstallPath = options.getOptionValue("build-dir")
        this.strUserDirectory = this.strInstallPath + File.separator + "testuserdir"
        this.strSolrPath = this.strInstallPath + File.separator + "cluster/solr";

        // solr data directory
        this.strSolrDataDir = this.strCaseDir + File.separator + "deshcluster.rand.org/ModuleOutput/keywordsearch/data/"

        // optional sleep seconds
        val sleepValue = options.getOptionValue("sleep")
        if (sleepValue != null) this.sleep = sleepValue.toInt

        // optional log dir
        this.logDirectory = options.getOptionValue("log-directory")

        // file to process
        val fileIdValue = options.getOptionValue("file-id")
        if (fileIdValue != null) this.fileId = fileIdValue.toLong
    }   
    
    override def toString(): String = {

        val retString = s"strCaseName = $strCaseName\n" +
                        s"strCaseDir = $strCaseDir\n" +
                        s"strCaseFilePath = $strCaseFilePath\n" +
                        s"strCaseDB = $strCaseDB\n" +
                        s"sleep = $sleep\n" +
                        s"logDirectory = $logDirectory\n" +
                        s"strDatabase = $strDatabase\n" +
                        s"strDatabasePort = $strDatabasePort\n" +
                        s"strDatabaseUser = $strDatabaseUser\n" +
                        s"strDatabasePasswd = $strDatabasePasswd\n" +
                        s"strSolrPort = $strSolrPort\n" +
                        s"strSolrHost = $strSolrHost\n" +
                        s"strInstallPath = $strInstallPath\n" +
                        s"strUserDirectory = $strUserDirectory\n" +
                        s"strSolrDataDir = $strSolrDataDir\n" + 
                        s"strSolrPath = $strSolrPath\n" +
                        s"strActiveMQHost = $strActiveMQHost\n" +
                        s"strActiveMQPort = strActiveMQPort\n" +
                        s"strActiveMQUser = $strActiveMQUser\n" +
                        s"strActiveMQPasswd = $strActiveMQPasswd\n"
                       
       return retString
    }
}
