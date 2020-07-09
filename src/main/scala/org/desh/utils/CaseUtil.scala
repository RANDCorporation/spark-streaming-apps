
package org.desh.utils

import org.sleuthkit.autopsy.casemodule.CaseMetadata
import java.nio.file.Path
import java.nio.file.FileSystems

// CaseUtil to get relevant information about a case from its case file
object CaseUtil extends Serializable {
    def getDatabaseName(strCaseFilePath: String): String = 
    {
    	val path = FileSystems.getDefault().getPath(strCaseFilePath)
        val caseMetadata = new CaseMetadata(path)
        val database = caseMetadata.getCaseDatabaseName()
        return database
    }
}