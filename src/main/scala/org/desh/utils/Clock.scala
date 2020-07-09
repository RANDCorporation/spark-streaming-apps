
package org.desh.utils

import scala.sys.process._
import scala.collection.JavaConversions._

import org.sleuthkit.autopsy.coreutils.Logger

// java imports
import java.util.{ArrayList,List,Properties,SortedSet,Date}
import java.io.{File,FileOutputStream}
import java.sql.{DriverManager,Connection,PreparedStatement,Timestamp}

// scala imports
import scala.collection.mutable.ListBuffer

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
import java.util.logging.Level

object Clock {

   def clock(tag: String, timestamps: ArrayList[java.util.Date], tags: ArrayList[String], aggregateGroups: ArrayList[String],
             aggregateGroup: String = null, date: java.util.Date = null) 
   {
        if (date == null) {
          timestamps.add(new java.util.Date())
        } else {
          timestamps.add(date)
        }
        tags.add(tags.size() + " " + tag)
        aggregateGroups.add(aggregateGroup)
   }


    def showClock( timestamps: ArrayList[java.util.Date], tags: ArrayList[String], aggregateGroups: ArrayList[String] )
    {   
        println( "\n\n\nclock\n\n" )

        var aggregateTime = new java.util.HashMap[String,Long]
        var prevTime = -1L 

        for( index <- 0 until timestamps.size() ){
            val tag = tags.get(index)
            val time = timestamps.get(index)
            val aggregateGroup = aggregateGroups.get( index )

            if ( prevTime == -1L ) {
                println( "\telapsed: 0\t" + tag + "\t" + time )
            }   
            else {
                val elapsed = time.getTime() - prevTime

                if( aggregateGroup != null ) {
                    var newTime = elapsed

                    if( aggregateTime.containsKey( aggregateGroup) ){
                        newTime += aggregateTime.get( aggregateGroup )
                    }   
                    aggregateTime.put( aggregateGroup, newTime )
                } else {
                    println( "\telapsed: " + elapsed + "\t" + tag + "\t" + time ) 
                }   
            }   
            prevTime = time.getTime()
        }   

        val itGroups = aggregateTime.keySet().iterator()
        println( "\ngroups\n" )

        while( itGroups.hasNext() ){
            val group = itGroups.next()
                    val time = aggregateTime.get( group )
                    println( "\t" + time + "\t" + group )
        }

        println( "\n\n\nclock done\n\n" )
    }
}
