package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.datamodel.Dumpable;
import org.apache.logging.log4j.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author mtutaj
 * Date: 4/13/12
 * Time: 3:49 PM
 * Collects incoming data, runs QC of ths data against RGD database, and then synchronizes incoming
 * data with RGD database. Supports optional logging to log4j file.
 * Incoming objects must implement edu.mcw.rgd.datamodel.Dumpable interfaces.
 * So they must implement state dumping to a human readable string
 * */
abstract public class RgdObjectSyncer {

    private Logger logger;
    private Object dao;

    private List incomingList = new ArrayList();
    private List inRgdList = new ArrayList();
    private List matchingList = new ArrayList();
    private List forInsertList = new ArrayList();
    private List forUpdateList = new ArrayList();
    private List forDeleteList = new ArrayList();

    public static final int CONTEXT_INSERT = 1;
    public static final int CONTEXT_UPDATE = 2;
    public static final int CONTEXT_DELETE = 3;

    /**
     * add incoming object to list of incmoing
     * @param obj RGD object implementing dumpable interface
     * @return true if object has been added to incoming objects collection; false otherwise
     */
    public boolean addIncomingObject(Object obj) {

        // incoming object must be non null
        if( obj==null )
            return false;

        // incoming object must not be a duplicate
        for( Object dump: getIncomingList() ) {
            if( equalsByContents(dump, obj) ) {
                // already on incomingList
                return false;
            }
        }

        // add to incoming list
        return getIncomingList().add(obj);
    }

    /**
     * QC of incoming data against RGD; populate arrays 'inRgdList', 'matchingList' and 'forInsertList';
     * if object is updatable, also populate 'forUpdateList';
     * if object is deletable, also populate 'forDeleteList'
     * @param rgdId object rgd id
     * @throws Exception
     */
    public void qc(int rgdId) throws Exception {

        if( rgdId!=0 ) {
            getInRgdList().clear();
            getInRgdList().addAll( getDataInRgd(rgdId) );
        }

        List inRgdListCopy = new ArrayList(getInRgdList());

        for( Object obj: getIncomingList() ) {

            boolean matchAgainstRgd = false;
            Iterator it = inRgdListCopy.iterator();
            while( it.hasNext() ) {
                Object obj2 = it.next();
                if( equalsByUniqueKey(obj, obj2) ) {
                    matchAgainstRgd = true;

                    // if object is updatable, match by contents to determine if updates are needed
                    if( isUpdatable() ) {
                        if( equalsByContents(obj, obj2) ) {
                            // objects do match by contents
                            getMatchingList().add(obj);
                        } else {
                            // objects do not match by contents
                            copyObjectUniqueKey(obj, obj2);
                            getForUpdateList().add(obj);
                        }
                    }
                    else {
                        // object not updatable: we are happy if it matches by unique key against RGD
                        getMatchingList().add(obj);
                    }
                    it.remove();
                    break;
                }
            }

            if( !matchAgainstRgd ) {
                getForInsertList().add(obj);
            }
        }

        // in deletable mode, whatever is left in 'inRgdListCopy' does not match incoming data
        // so it should be deleted, if deletions are enabled
        if( isDeletable() && !inRgdListCopy.isEmpty() ) {
            getForDeleteList().addAll(inRgdListCopy);
        }

        optimizeInDels();
    }

    /**
     * synchronize incoming data with rgd
     * @param rgdId rgdId of genomic element
     * @throws Exception
     */
    public void sync(int rgdId, Object userData) throws Exception {

        if( !getForInsertList().isEmpty() ) {
            for( Object obj: getForInsertList() ) {
                prepare(obj, rgdId, userData, CONTEXT_INSERT);

                if( getLog()!=null )
                    getLog().info("INSERT "+((Dumpable)obj).dump("|"));
            }

            insertDataIntoRgd(getForInsertList());
        }

        if( !getForUpdateList().isEmpty() ) {
            for( Object obj: getForUpdateList() ) {
                prepare(obj, rgdId, userData, CONTEXT_UPDATE);

                if( getLog()!=null )
                    getLog().info("UPDATE NEW "+((Dumpable)obj).dump("|"));
            }

            updateDataInRgd(getForUpdateList());
        }

        if( !getForDeleteList().isEmpty() ) {
            for( Object obj: getForDeleteList() ) {
                prepare(obj, rgdId, userData, CONTEXT_DELETE);

                if( getLog()!=null )
                    getLog().info("DELETE "+((Dumpable)obj).dump("|"));
            }

            deleteDataFromRgd(getForDeleteList());
        }
    }

    public void incrementCounters(PipelineSession session, String counterPrefix) {

        if( !matchingList.isEmpty() )
            session.incrementCounter(counterPrefix+"MATCHED", matchingList.size());
        if( !forInsertList.isEmpty() )
            session.incrementCounter(counterPrefix+"INSERTED", forInsertList.size());
        if( !forUpdateList.isEmpty() )
            session.incrementCounter(counterPrefix+"UPDATED", forUpdateList.size());
        if( !forDeleteList.isEmpty() )
            session.incrementCounter(counterPrefix+"DELETED", forDeleteList.size());
    }

    protected void optimizeInDels() {

        // so far our logic determined:
        // 1. matching positions, that landed in matchingList array
        // 2. to be inserted positions, that landed in forInsertList array
        // 3. to be deleted positions, that are found in forDeleteList array
        //
        // to minimize inserts/delete operations, we can replace one insert/delete pair with an update operation
        while( !forInsertList.isEmpty() && !forDeleteList.isEmpty() ) {
            Object objInsert = forInsertList.get(0);
            Object objDelete = forDeleteList.get(0);

            if( logger!=null )
                logger.info("UPDATE OLD " + ((Dumpable)objDelete).dump("|"));

            copyObjectUniqueKey(objInsert, objDelete);

            forUpdateList.add(objInsert);

            forInsertList.remove(0);
            forDeleteList.remove(0);
        }
    }

    /**
     * compares two rgd objects for equality of their unique keys;
     * used to determine object presence in RGD database
     * @param obj1 rgd object 1
     * @param obj2 rgd object 2
     * @return true if objects are considered equal
     */
    abstract protected boolean equalsByUniqueKey(Object obj1, Object obj2);

    /**
     * compares contents two rgd objects for equality;
     * used to determine whether object contents changed against second object
     * @param obj1 rgd object 1
     * @param obj2 rgd object 2
     * @return true if objects are considered equal
     */
    abstract protected boolean equalsByContents(Object obj1, Object obj2);

    /**
     * override this method to get list of rgd object being already in RGD database
     * given object rgd id
     * @param rgdId rgd id
     * @return list of objects in rgd
     * @throws Exception
     */
    abstract protected List getDataInRgd(int rgdId) throws Exception;

    /**
     * insert a list of objects into RGD database
     * @return count of rows affected
     * @throws Exception
     */
    abstract protected int insertDataIntoRgd(List list) throws Exception;

    /**
     * update a list of objects in RGD database
     * @return count of rows affected
     * @throws Exception
     */
    abstract protected int updateDataInRgd(List list) throws Exception;

    /**
     * delete a list of objects from RGD database
     * @return count of rows affected
     * @throws Exception
     */
    abstract protected int deleteDataFromRgd(List list) throws Exception;

    /**
     * copy object unique key from one object to another
     * @param toObj to object
     * @param fromObj from object
     */
    abstract protected void copyObjectUniqueKey(Object toObj, Object fromObj);

    /**
     * preapre object for database operation, like update, insert or delete
     * @param obj Dumpable object
     * @param rgdId rgdId
     * @param userData custom user data
     */
    abstract protected void prepare(Object obj, int rgdId, Object userData, int context);


    public Logger getLog() {
        return logger;
    }

    public void setLog(Logger logger) {
        this.logger = logger;
    }

    public Object getDao() {
        return dao;
    }

    /** set class that will perform operations against database
     *
     * @param dao dao class
     */
    public void setDao(Object dao) {
        this.dao = dao;
    }

    abstract public boolean isUpdatable();

    abstract public boolean isDeletable();

    public List getIncomingList() {
        return incomingList;
    }

    public List getInRgdList() {
        return inRgdList;
    }

    public List getMatchingList() {
        return matchingList;
    }

    public List getForInsertList() {
        return forInsertList;
    }

    public List getForUpdateList() {
        return forUpdateList;
    }

    public List getForDeleteList() {
        return forDeleteList;
    }
}
