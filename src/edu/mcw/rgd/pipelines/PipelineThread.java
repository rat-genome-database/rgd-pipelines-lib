package edu.mcw.rgd.pipelines;

import org.apache.logging.log4j.*;

import java.io.*;

/**
 * @author mtutaj
 * Date: Dec 1, 2010
 * Time: 2:38:46 PM
 * worker thread run on behalf of PipelineWorkgroup;
 * single pipeline workgroup can run multiple worker threads, all interacting
 * with same record input and output queues
 */
public class PipelineThread implements Runnable {

    private PipelineWorkgroup workgroup;

    Logger log = LogManager.getLogger(getClass());

    public PipelineThread(PipelineWorkgroup workgroup) {
        this.workgroup = workgroup;
    }

    public void run() {
        log.debug(Thread.currentThread().getName()+" thread started");

        final PipelineSession session = workgroup.getSession();
        final int inputQueueIndex = workgroup.getInputQueueIndex();
        final int outputQueueIndex = workgroup.getOutputQueueIndex();

        try {
            if( inputQueueIndex<0 ) {
                // pipeline without an input queue is supposed to process its data source,
                // break it into a stream of PipelineRecord objects
                // and insert these records into a supplied output queue
                RecordPreprocessor preprocessor = ((RecordPreprocessor) workgroup.getRecordProcessor());
                preprocessor.process();
                // signal end of data
                preprocessor.signalEndOfData();
            }
            else {

                // initialize the workgroup
                workgroup.getRecordProcessor().onInit();

                // get next record from queue waiting if necessary
                PipelineRecord rec;
                while( (rec = session.getRecordFromQueue(inputQueueIndex)) != null ) {

                    // process the record
                    try {
                        ((RecordProcessor)workgroup.getRecordProcessor()).process(rec);

                        // handle postponed records
                        if( rec.isFlagSet("$SYSFLAG_POSTPONE$") ) {
                            rec.unsetFlag("$SYSFLAG_POSTPONE$");
                            session.putRecordToQueue(inputQueueIndex, rec);
                            log.debug(Thread.currentThread().getName()+" postpone processing for recno="+rec.getRecNo() );
                        }
                        else {
                            // increment the number of records processed
                            session.incrementRecordsProcessed(inputQueueIndex+1, 1);

                            // put the record to the output queue for further processing, if applicable
                            if( outputQueueIndex>=0 )
                                session.putRecordToQueue(outputQueueIndex, rec);
                        }
                    }
                    catch(Exception e) {
                        // print stack trace to a string
                        final Writer result = new StringWriter();
                        final PrintWriter printWriter = new PrintWriter(result);
                        e.printStackTrace(printWriter);
                        String stackTrace = result.toString();

                        boolean isRegisteredException = session.isStackTraceARegisteredUserException(stackTrace);

                        String msg = Thread.currentThread().getName()+" exception intercepted for recno="+rec.getRecNo();
                        if( !isRegisteredException )
                            log.info(msg);
                        else
                            log.debug(msg);

                        session.incrementCounter("HANDLED_EXCEPTION_COUNT", 1);

                        // registered user exceptions do NOT dump stack trace
                        if( !isRegisteredException )
                            log.warn(stackTrace);
                        else
                            session.incrementCounter("REGISTERED_USER_EXCEPTION_COUNT", 1);

                        if( workgroup.getExceptionsOccurred() >= session.getAllowedExceptions() ) {

                            log.info(Thread.currentThread().getName()+" aborting: number of exceptions that occurred in this thread group exceeds the number allowed "+session.getAllowedExceptions() );

                            // number of exception that occurred in this thread group exceeds the number allowed
                            // abort processing
                            throw e;
                        }
                        else {
                            // registered user exceptions are silent
                            if( !isRegisteredException )
                                log.info(Thread.currentThread().getName()+" exception occurred: "+e.getMessage() );

                            // increment nr of exceptions that occurred in this thread group
                            workgroup.incExceptionsOccurred();

                            // decrement nr of records processed in all preceding queues
                            for( int queueIndex=1; queueIndex<=inputQueueIndex; queueIndex++ ) {
                                session.incrementRecordsProcessed(queueIndex, -1);
                            }

                            // put the record to the put-back queue
                            int putBackQueueIndex = session.getWorkgroupCount();
                            log.debug(Thread.currentThread().getName()+" putting record with recno="+rec.getRecNo()+" to put-back queue" );
                            session.putRecordToQueue(putBackQueueIndex, rec);
                            log.debug(Thread.currentThread().getName()+" there are "+ session.getQueueSize(putBackQueueIndex)+" put-back records in the queue" );
                        }
                    }
                }

                // finalize the workgroup
                workgroup.getRecordProcessor().onExit();
            }
        }
        catch(Exception e) {
            abort(e, session);
        }

        log.debug(Thread.currentThread().getName()+" thread finished!");
    }

    /** helper function - aborts the application with given status code
     * @param e Exception
     */
    public void abort(Exception e, PipelineSession session) {
        log.error(workgroup.getName()+" exception: "+e.getMessage());

        // print stack trace to error stream
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        e.printStackTrace(new PrintStream(bs));
        log.error(bs.toString());

        // force abort
        String msg = workgroup.getName()+" aborted due to exception -- forced pipeline framework abort ---";
        log.error(msg);
        session.setAbortAllFlag();
        throw new PipelineException(msg, e);
    }
 }
