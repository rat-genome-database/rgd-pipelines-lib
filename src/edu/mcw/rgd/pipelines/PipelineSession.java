package edu.mcw.rgd.pipelines;

import org.apache.logging.log4j.*;

import java.io.PrintStream;
import java.util.*;

/**
 * @author mtutaj
 * Date: Nov 30, 2010
 * Time: 1:47:43 PM
 * data shared between multiple threads -- all access must be synchronized
 */
public class PipelineSession {

    private List<PipelineWorkgroup> workgroups = new ArrayList<>(); // pools of threads
    private List<PipelineRecordQueue> queues = new ArrayList<>(); // record queues; the last queue is a put-back queue
    private List<Integer> recordsProcessed = new ArrayList<>(); // count of records processed by given workgroup

    private Map<String, Integer> counters = new TreeMap<>();
    private Map<String, Object> attributes = new HashMap<>(); // custom attributes stored in pipeline session

    private int endOfInputCount = 0; // incremented by every input pipeline;
        // once all of the input pipelines signal end of input, it will signal the end of input in general

    // number of allowed exceptions per thread workgroup;
    // if exception is encountered, and the current exception count for the thread group
    // is not exceeded, the exception is reported and the record processed is passed to put-back queue,
    // so it could be run through the pipeline when the first queue is empty
    private int allowedExceptions = 0;

    // list of exceptions user do not want to log; they are expected to happen by the user;
    // put-back queue mechanism is used to handle these exceptions silently;
    // array String[] contains substrings that must occur in the exception stacktrace to uniquely
    // identify the user exception
    private List<String[]> registeredUserExceptions = new ArrayList<>();

    // internal record counter: increments with every record that we attempt to get while put-back queue is not empty;
    // every 100 records, a record is pulled from put-back queue if possible
    private int recordCounter = 0;

    // if an exception that cannot be handled was thrown in a thread, all threads
    // must be aborted
    private boolean abortRequested = false;

    Logger log = LogManager.getLogger(getClass());

    /**
     * determine whether the processing of all data is complete; worker threads are calling this method
     * often when there is no data in their input queue trying to find out if they should terminate
     * @return true if the processing for this thread group is complete
     */
    synchronized public boolean isProcessingComplete() {
        if( abortRequested ) {
            log.debug("PipelineSession isProcessingComplete - TRUE - abort requested");
            return true;
        }
        // there must be end of input signaled
        if( endOfInputCount < workgroups.get(0).getThreadCount() ) {
            log.debug("PipelineSession isProcessingComplete - FALSE - no end of input");
            return false;
        }

        // all pipeline workgroups must signal the same number of records processed
        Integer processed0 = recordsProcessed.get(0);
        for( int i=1; i<recordsProcessed.size(); i++ ) {
            Integer processedi = recordsProcessed.get(i);
            if( !processedi.equals(processed0) ) {
                log.debug("PipelineSession isProcessingComplete - FALSE - workgroup"+i+"="+processedi+" <> workgroup0="+processed0);
                return false;
            }
        }
        log.debug("PipelineSession isProcessingComplete - TRUE!");
        return true;
    }

    /** get a new record from the head of the queue;
     * does not return until it gets a record from queue
     *
     * @param queueIndex queue index
     * @return PipelineRecord object or null if there are no more objects to process
     */
    public PipelineRecord getRecordFromQueue(int queueIndex) throws InterruptedException {
        final int sleepInSeconds = 2;
        PipelineRecordQueue queue = queues.get(queueIndex);
        PipelineRecordQueue putBackQueue = queues.get(queues.size()-1);

        while( !isProcessingComplete() ) {

            if( queueIndex==0 && putBackQueue.getSize()>0 ) {
                ++recordCounter;
            }

            // process a record from pullback queue every 100 records
            PipelineRecord rec;
            if( recordCounter%100==0 && queueIndex==0 && putBackQueue.getSize()>0 ) {
                rec = putBackQueue.pullRecord();
                if( rec!=null ) {
                    log.debug("processing record recno="+rec.getRecNo()+" from put-back queue; put-back records left: "+putBackQueue.getSize());
                    return rec;
                }
            }

            // try to get a new record from regular queue
            rec = queue.pullRecord();
            if( rec!=null )
                return rec;

            // no regular record -- try put-back queue
            if( queueIndex==0 ) {
                rec = putBackQueue.pullRecord();
                if( rec!=null ) {
                    log.debug("processing record recno="+rec.getRecNo()+" from put-back queue; put-back records left: "+putBackQueue.getSize());
                    return rec;
                }
                else {
                    log.debug("no records in put-back queue; waiting...");
                }
            }

            // sleep a bit
            Thread.sleep(1000 * sleepInSeconds);
        }

        // we got here because processing is complete
        return null;
    }

    public void putRecordToQueue(int queueIndex, PipelineRecord rec) throws InterruptedException {
        assert(rec!=null);

        PipelineRecordQueue queue = queues.get(queueIndex);
        queue.putRecord(rec);
    }

    /**
     * increment number of records processed by given workgroup
     * @param workgroupIndex workgroup index
     * @param inc value of increment (usually 1 to increment, -1 to decrement)
     */
    synchronized public void incrementRecordsProcessed(int workgroupIndex, int inc) {
        recordsProcessed.set(workgroupIndex, recordsProcessed.get(workgroupIndex) + inc);
    }

    /**
     * get number of records processed within given workgroup
     *
     */
    synchronized public int getRecordsProcessed(int workgroupIndex) {
        return recordsProcessed.get(workgroupIndex);
    }

    /**
     * pipeline without input queue should call this method to insert every record created
     * to the queue for further processing by other pipelines
     * @param rec PipelineRecord object; null signals end of input
     * @throws InterruptedException
     */
    public void putRecordToFirstQueue(PipelineRecord rec) throws InterruptedException, PipelineException {
        if( abortRequested ) {
            throw new PipelineException("putRecordToFirstQueue() failed; general abort requested");
        }

        if( rec==null ) {
            synchronized(this) {
                // signal end of input for one of input-only pipelines
                endOfInputCount++;
            }
        }
        else {
            // regular record -- insert it into the first queue
            putRecordToQueue(0, rec);

            // and increment number of records processed
            incrementRecordsProcessed(0, 1);
        }
    }

    public int getAllowedExceptions() {
        return allowedExceptions;
    }

    public void setAllowedExceptions(int allowedExceptions) {
        this.allowedExceptions = allowedExceptions;
    }

    /**
     * get size of a queue identified by an index
     * @param queueIndex queue index
     * @return number of objects in a given queue
     */
    public int getQueueSize(int queueIndex) {
        PipelineRecordQueue queue = queues.get(queueIndex);
        if( queue==null )
            return -1;
        return queue.getSize();
    }

    /******************
     * SETUP and INTERNAL
     */
    public void addWorkgroup(PipelineWorkgroup workgroup) {
        workgroups.add(workgroup);
        recordsProcessed.add(0);
    }

    public int getWorkgroupCount() {
        return workgroups.size();
    }

    public PipelineWorkgroup getWorkgroup(int index) {
        return workgroups.get(index);
    }

    public void addQueue(int queueSize) {
        assert(queueSize>=0);

        PipelineRecordQueue queue = new PipelineRecordQueue();
        queues.add(queue);
        queue.setMaxSize(queueSize);
        queue.create();
    }

    /**
     * increment counter by given delta value
     * @param counterName name of the counter -- if it does not exist, it is created
     * @param delta increment delta, usually 1
     * @return new value of the counter
     */
    synchronized public int incrementCounter(String counterName, int delta ) {
        Integer val = counters.get(counterName);
        if( val==null )
            val = delta;
        else
            val += delta;
        counters.put(counterName, val);
        return val;
    }

    /**
     * get value of specific counter
     * @param counterName counter name
     * @return value of the counter
     */
    synchronized public int getCounterValue(String counterName) {
        Integer val = counters.get(counterName);
        if( val==null )
            return 0;
        else
            return val;
    }

    /**
     * get list of all counters
     * @return set of counter names
     */
    synchronized public Set<String> getCounters() {
        return counters.keySet();
    }

    /**
     * sets a named session attribute to the new value
     * @param name name of session attribute
     * @param attr the value of the attribute
     */
    synchronized public void setAttribute(String name, Object attr) {
        this.attributes.put(name, attr);
    }

    /**
     * gets a named session attribute
     * @param name name of the attribute
     * @return value of the attribute
     */
    synchronized public Object getAttribute(String name) {
        return this.attributes.get(name);
    }

    /**
     * register user exception
     * @param exceptionInfo exception info
     */
    public void registerUserException(String[] exceptionInfo) {
        this.registeredUserExceptions.add(exceptionInfo);
    }

    /**
     * get list of registered user exceptions
     */
    public List<String[]> getRegisteredUserExceptions() {
        return this.registeredUserExceptions;
    }

    public boolean isStackTraceARegisteredUserException(String stackTrace) {

        for( String[] infos: this.registeredUserExceptions ) {
            boolean allInfoMatch = true;
            for( String info: infos ) {
                if( !stackTrace.contains(info) ) {
                    allInfoMatch = false;
                    break;
                }
            }
            if( allInfoMatch )
                return true;
        }
        return false;
    }

    /**
     * dump session counters to STDOUT
     */
    public void dumpCounters() {
        PrintStream stream = null;
        dumpCounters(stream);
    }

    /**
     * dump session counters to the supplied stream;
     * @param stream stream where session counter statistics will be written to; if null, System.out will be used
     */
    public void dumpCounters(PrintStream stream) {

        if( stream==null )
            stream = System.out;

        for( String counter: getCounters() ) {
            int count = getCounterValue(counter);
            if( count>0 ) {
                stream.println(counter+": "+count);
            }
        }
    }

    /**
     * dump session counters to the supplied logger; INFO level will be used
     * @param log log where session counter statistics will be written to
     */
    public void dumpCounters(Logger log) {

        for( String counter: getCounters() ) {
            int count = getCounterValue(counter);
            if( count>0 ) {
                log.info(counter+": "+count);
            }
        }
    }

    public void setAbortAllFlag() {
        abortRequested = true;
    }

    public boolean isAbortRequested() {
        return abortRequested;
    }
}
