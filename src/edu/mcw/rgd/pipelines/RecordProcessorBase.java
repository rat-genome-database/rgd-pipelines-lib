package edu.mcw.rgd.pipelines;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: Apr 28, 2011
 * Time: 4:15:30 PM
 *
 * provides common functionality for RecordProcessor and RecordPreprocessor
 */
public class RecordProcessorBase {

    // set automatically by PipelineManager
    PipelineSession session;

    // pipeline workgroup
    PipelineWorkgroup workgroup;

    // override to customize the maximum queue size;
    // default is 0 meaning the queue size is unlimited
    int maxQueueSize = 0;

    public PipelineSession getSession() {
        return session;
    }

    public void setSession(PipelineSession session) {
        this.session = session;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public PipelineWorkgroup getWorkgroup() {
        return workgroup;
    }

    public void setWorkgroup(PipelineWorkgroup workgroup) {
        this.workgroup = workgroup;
    }

    /**
     * called BEFORE the record processing, to perform necessary initialisations;
     * by default, this method does nothing
     * @throws Exception
     */
    public void onInit() throws Exception {

    }

    /**
     * called AFTER all the records have been processed, to perform necessary finalisation;
     * by default, this method does nothing
     * @throws Exception
     */
    public void onExit() throws Exception {

    }
}
