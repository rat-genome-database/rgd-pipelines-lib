package edu.mcw.rgd.pipelines;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: Nov 30, 2010
 * Time: 1:49:44 PM
 * it creates a number of threads that will run in parallel;
 * threads take records from input queue, and put records into output queue
 */
public class PipelineWorkgroup {

    // shared data object
    private PipelineSession session;

    // custom record processor
    private RecordProcessorBase processor;

    // unique name of the pipeline group
    private String name;

    // number of threads in this workgroup -- default is one
    private int threadCount = 1;

    // index of input queue
    private int inputQueueIndex = -1;
    // output of input queue
    private int outputQueueIndex = -1;

    // count of exceptions that occurred during running of all threads in the group
    private int exceptionsOccurred = 0;

    /**
     * pipeline is of input only type: no input queue, putting records into output queue
     * @return
     */
    public boolean isInputOnly() {
        return inputQueueIndex < 0 && outputQueueIndex>=0;
    }

    /**
     * pipeline is of output only type: only getting records from input queue and disregarding them upon processing
     * @return
     */
    public boolean isOutputOnly() {
        return inputQueueIndex >= 0 && outputQueueIndex<0;
    }

    /**
     * pipeline is of input-output type: getting records from input queue, processing them
     * and putting the records in output queue
     * @return
     */
    public boolean isInputOutput() {
        return inputQueueIndex >= 0 && outputQueueIndex>=0;
    }



    public PipelineSession getSession() {
        return session;
    }

    // will be set automatically by PipelineManager
    public void setSession(PipelineSession session) {
        this.session = session;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public int getInputQueueIndex() {
        return inputQueueIndex;
    }

    public void setInputQueueIndex(int inputQueueIndex) {
        this.inputQueueIndex = inputQueueIndex;
    }

    public int getOutputQueueIndex() {
        return outputQueueIndex;
    }

    public void setOutputQueueIndex(int outputQueueIndex) {
        this.outputQueueIndex = outputQueueIndex;
    }

    public RecordProcessorBase getRecordProcessor() {
        return processor;
    }

    public void setRecordProcessor(RecordProcessorBase processor) {
        this.processor = processor;
    }

    public synchronized int getExceptionsOccurred() {
        return exceptionsOccurred;
    }

    public synchronized void incExceptionsOccurred() {
        this.exceptionsOccurred++;
    }

    /**
     * get count of records processed by this thread workgroup
     * @return count of records processed by this thread workgroup
     */
    public int getRecordsProcessed() {
        return session.getRecordsProcessed(inputQueueIndex+1);
    }
}
