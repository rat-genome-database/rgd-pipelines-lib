package edu.mcw.rgd.pipelines;

/**
 * Created by IntelliJ IDEA. </br>
 * User: mtutaj </br>
 * Date: Apr 28, 2011 </br>
 * Time: 4:12:54 PM </br>
 * provide a stream of records to the pipeline framework
 */
public abstract class RecordPreprocessor extends RecordProcessorBase {

    /** implement to transform the input data into a stream of PipelineRecord-s;
     * once the customized PipelineRecord 'rec' is complete, put it into the framework
     * by calling 'session.putRecordToFirstQueue(rec)';
     * the framework calls automatically 'session.signalEndOfData()' to signal the end of data
     * after process() method finishes running
     * (in rgd-pipelines-1.0.3.jar, you had to make a call to that method in the code)
     */
    public abstract void process() throws Exception;

    /**
     * when no more data is available, call this method so the framework will be able
     * to finalize the processing
     */
    public void signalEndOfData() throws InterruptedException {
        this.getSession().putRecordToFirstQueue(null);
    }
}
