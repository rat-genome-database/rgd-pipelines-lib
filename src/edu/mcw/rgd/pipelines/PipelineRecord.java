package edu.mcw.rgd.pipelines;

import java.util.HashSet;
import java.util.Set;

/**
 * @author mtutaj
 * Date: Nov 30, 2010
 * Time: 1:29:46 PM
 * Concept: pipeline is reading the input converting it effectively into a stream
 *  of PipelineRecords, which in turn are being quality checked against database,
 *  and then loaded into database
 */
public class PipelineRecord {

    // unique id assigned to every pipeline record processed;
    // must be set during creation of pipeline record
    private int recNo;

    // a set of flags the given record is flagged with
    private Set<String> flags = new HashSet<>();

    // persistence: save the record state as XML string
    public String toXML() {
        return null;
    }

    // persistence: restore the record state from XML string
    public void fromXML(String xml) {
    }


    public int getRecNo() {
        return recNo;
    }

    public void setRecNo(int recNo) {
        this.recNo = recNo;
    }

    /**
     * set a flag for this record
     * @param flag flag name
     * @return true if this set did not already contain the specified flag; false otherwise
     */
    public boolean setFlag(String flag) {
        return flags.add(flag);
    }

    /**
     * unset a flag for this record
     * @param flag flag name
     * @return true if the flag was set before this operation; false otherwise
     */
    public boolean unsetFlag(String flag) {
        return flags.remove(flag);
    }

    /**
     * is given flag present for this record
     * @param flag flag name
     * @return true if this flag is set; false otherwise
     */
    public boolean isFlagSet(String flag) {
        return flags.contains(flag);
    }

    /**
     * return all flags associated with given pipeline record
     * @return
     */
    public Set<String> getFlags() {
        return flags;
    }
}
