package edu.mcw.rgd.pipelines;

/**
 * @author mtutaj
 * RuntimeException allowing for controlled handling of the exception causing the crash
 */
public class PipelineException extends RuntimeException {

    public PipelineException(String msg) {
        super(msg);
    }

    public PipelineException(String msg, Throwable e) {
        super(msg, e);
    }
}
