package edu.mcw.rgd.pipelines;

import org.apache.logging.log4j.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mtutaj
 * @since Nov 30, 2010
 * manages a pool of pipeline workgroups
 */
public class PipelineManager {

    Logger log = LogManager.getLogger(getClass());

    // pipeline session should be always available!
    private PipelineSession session = new PipelineSession();

    /**
     * add a pipeline group to the pipeline manager
     * @param processor class responsible for processing of the data records
     * @param name name for the pipeline group
     * @param threadCount count of parallel worker threads created on behalf of the pipeline workgroup
     * @param outputQueueSize max size of records kept in the output queue; 0 - any size
     */
    public void addPipelineWorkgroup(RecordProcessorBase processor, String name, int threadCount, int outputQueueSize) {

        PipelineWorkgroup workgroup = new PipelineWorkgroup();
        workgroup.setName(name);
        workgroup.setRecordProcessor(processor);
        workgroup.setThreadCount(threadCount);
        processor.setWorkgroup(workgroup);
        session.addWorkgroup(workgroup);

        workgroup.setSession(session);
        processor.setSession(session);
        
        session.addQueue(outputQueueSize);
    }

    /**
     * runs all of the pipeline workgroups
     * <blockquote>
     *     Note: This method does not return until all workgroups finished processing.
     * </blockquote>
     */
    public void run() throws Exception {
        log.info("Starting pipeline framework - v.1.0.7, built on Oct 2, 2017");

        // at least two pipeline workgroups must be present
        int wcount = session.getWorkgroupCount();
        if( wcount<2 ) {
            throw new PipelineManagerException("At least two pipeline workgroups must be defined!");
        }

        // create put-back queue with unlimited size
        session.addQueue(0);

        // setup the pipeline record queues
        List<Thread> workerThreads = new ArrayList<>();

        for( int w=0; w<wcount; w++ ) {
            PipelineWorkgroup workgroup = session.getWorkgroup(w);
            if( w==0 ) {
                // first pipeline workgroup is always a input pipeline putting all records into output queue
                workgroup.setOutputQueueIndex(0);
            }
            else if( w==wcount-1 ) {
                // last pipeline workgroup is always a output pipeline only taking records from input queue
                // processing them and disregarding them
                workgroup.setInputQueueIndex(w-1);
            }
            else {
                // the middle pipeline reads records from input queue
                // processes them and inserts into output queue
                workgroup.setInputQueueIndex(w-1);
                workgroup.setOutputQueueIndex(w);
            }

            // create a pool of worker threads
            for( int i=0; i<workgroup.getThreadCount(); i++ ) {
                workerThreads.add(new Thread(new PipelineThread(workgroup), workgroup.getName()+(i+1)));
            }
        }

        // record time stamp before starting the threads
        long time1 = System.currentTimeMillis();

        // start all the threads
        for( Thread thread: workerThreads ) {
            thread.start();
        }

        // wait until all threads will stop running
        for( Thread thread: workerThreads ) {
            thread.join();
        }

        // record time stamp after the threads finished running
        long time2 = System.currentTimeMillis();
        // show elapsed time
        log.info("Processing elapsed time: "+PipelineUtils.formatElapsedTime(time1, time2));
        // show number of records processed
        log.info("Records processed: "+ session.getRecordsProcessed(0));

        if( session.isAbortRequested() ) {
            throw new PipelineManagerException("ERROR: pipeline framework processing aborted due to unhandled exception");
        }
    }

    /**
     * get session object
     * @return PipelineSession object
     */
    public PipelineSession getSession() {
        return session;
    }

    /**
     * set new session object
     * <p>
     *     Note: all objects held in old session objects will be gone!
     * </p>
     * @param session PipelineSession object
     */
    public void setSession(PipelineSession session) {
        this.session = session;
    }

    /**
     * dump session counters to STDOUT
     */
    public void dumpCounters() {
        getSession().dumpCounters();
    }

    /**
     * dump session counters to the supplied log; INFO level will be used
     * @param log log where session counter statistics will be written to
     */
    public void dumpCounters(Logger log) {
        getSession().dumpCounters(log);
    }

    /**
     * to distinguish our exceptions from other exceptions
     */
    class PipelineManagerException extends Exception {
        public PipelineManagerException(String message) {
            super(message);
        }
    }
}
