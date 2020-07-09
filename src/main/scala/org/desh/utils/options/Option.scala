
package org.desh.utils.options

/** 
 *  Command-Line Interface Option to be parsed
 */
class Option(name: String, help: String, isRequired: Boolean) {
  
    /** the value of the option */
    private var value: String = null

    /** whether the option matches */
    def matches(cliArg: String): Boolean = {
        return (cliArg.startsWith("-")) && (cliArg contains this.name)
    }
    
    /** set the value of the option */
    def setValue(value: String) {
        this.value = value
    }
    
    /** usage string */
    def usage(): String = {
        return String.format("%-18s%s", this.getCLIName(), this.help)
    }
    
    /** cli name with prefix */
    def getCLIName(): String = {
        return s"-$name"
    }

    /** name of the option */
    def getName(): String = {
        return this.name 
    }

    /** the value of the option */
    def getValue(): String = {
        return this.value
    }
    
    /** validate the option's value */
    def validate() {
       if (this.isRequired && this.getValue() == null) {
           throw new IllegalArgumentException(String.format("Missing required option: %s", this.getCLIName()))
       }
    }
    
    /** string representation */
    override def toString(): String = {
       return String.format("%-18s%s", this.name, this.value)
    }
}
