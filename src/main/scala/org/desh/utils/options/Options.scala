
package org.desh.utils.options

import scala.collection.mutable.LinkedHashMap

/** 
 *  Command-Line Interface Options to be parsed
 */
class Options {
    
    // array of options
    val optionsMap: LinkedHashMap[String, Option] = LinkedHashMap()
    
    /** add option */
    def addOption(name: String, help: String, isRequired: Boolean) {
        val option = new Option(name, help, isRequired)
        // this.options += option
        this.optionsMap += (name -> option)
    }

    /** parse argument string checking defined options */
    protected def parseArgs(args: Array[String]) {
        
        // parse the arguments
        val arglist = args.toList
        for (i <- 0 until arglist.length by 2) {
            val arg = arglist(i)

            // only allow options in the form: -flag value
            // TODO(ttran): Allow flags that don't need values
            if (!arg.startsWith("-"))  throw new IllegalArgumentException("Illegal argument " + arg) 

            // check available options
            var optionValid = false
            for ((name, option) <- this.optionsMap) {
                if (option.matches(arg)) {
                    
                    // not enough arguments
                    if (i + 1 > arglist.length - 1) throw new IllegalArgumentException(option.getCLIName() + " flag requires value")

                    val value = arglist(i + 1)
                    option.setValue(value)
                    optionValid = true
                }
            }

            // no options for arg
            if (!optionValid) throw new IllegalArgumentException("Illegal Option: " + arg)

        }
    }
    
    /** parse arguments, exit if parsing fails and print usage */
    def parseArgsExitOnFailure(args: Array[String]) {
        // Parse arguments
        try { 
            this.parseArgs(args)
            this.validate()

        // Invalid arguments
        } catch {
            case exception: Exception =>
                println(exception.getMessage)
                this.printUsage()
                sys.exit(1)
        }
    }
    
    /** validate options */
    def validate() {
      for ((name, option) <- this.optionsMap) {
           option.validate()
       }
    }
    
    /** print option values */
    def printOptions() {
        for ((name, option) <- this.optionsMap) {
          println(option)
        }
    }
    
    /** get option value */
    def getOptionValue(optionName: String): String = {
        val option = this.optionsMap(optionName)
        return option.getValue()
    }
    
    /** usage of options */
    protected def printUsage() {
        println(this.usage())
    }

    /** usage string */
    protected def usage(): String = {
        var ret = "Options:\n"
        for ((name, option) <- this.optionsMap) {
            ret += "\t" + option.usage() + "\n"
        }
        return ret
    }

}
