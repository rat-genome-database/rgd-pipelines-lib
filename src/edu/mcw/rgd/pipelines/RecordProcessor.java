package edu.mcw.rgd.pipelines;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: Dec 1, 2010
 * Time: 2:58:36 PM
 * extend to provide custom processing of your records
 */
public abstract class RecordProcessor extends RecordProcessorBase {

    /**
     * process a record by this processor
     * @param rec PipelineRecord object
     * @throws Exception
     */
    public abstract void process(PipelineRecord rec) throws Exception;

    /**
     * given record 'rec' should be processed later -- record 'rec' is moved to the end
     * of the current processing queue
     * @param rec PipelineRecord object
     */
    public void postponeRecordProcessing(PipelineRecord rec) {
        rec.setFlag("$SYSFLAG_POSTPONE$");
    }
}
