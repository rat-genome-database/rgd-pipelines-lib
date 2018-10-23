package edu.mcw.rgd.datamodel;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: Apr 13, 2012
 */

/**
 * A Dumpable object supports dumping its state in a human readable format for diagnostic/debug/export purposes
 */
public interface Dumpable {

    /**
     * dump state of an object in separator-delimited format to a string;
     * fields are separated by a supplied delimiter;
     * every field has format: <field-name>:<field-value>, where <field-name> is usually the name of column in database table
     * @param delimiter delimiting character(s), like ' ', '\t', '|' etc
     * @return string
     */
    public String dump(String delimiter);

}
