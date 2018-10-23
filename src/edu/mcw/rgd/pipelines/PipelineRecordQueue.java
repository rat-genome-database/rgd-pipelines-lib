package edu.mcw.rgd.pipelines;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author mtutaj
 * Date: Nov 30, 2010
 * Time: 1:37:32 PM
 * 
 */
public class PipelineRecordQueue {

    // unique name of record queue; usually composed as concatenation of pipelines interacting with the queue
    private String name;
    // maximum queue size
    private int maxSize;
    // internal blocking queue
    private BlockingQueue<PipelineRecord> queue;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    /**
     * get size of the queue
     * @return number of objects in the queue
     */
    public int getSize() {
        return queue.size();
    }

    // creates an empty queue
    public void create() {
        if( maxSize <= 0 ) {
            // create a blocking queue without size limit
            queue = new LinkedBlockingQueue<PipelineRecord>();
        }
        else {
            // create a blocking queue with size limit
            queue = new LinkedBlockingQueue<PipelineRecord>(maxSize);
        }
    }

    // pulls a record from head of the queue without waiting -- null possible
    public PipelineRecord pullRecord() throws InterruptedException {
        return queue.poll();
    }

    // pulls a record from head of the queue waiting up to specified nr of seconds
    public PipelineRecord pullRecord(int timeoutInSeconds) throws InterruptedException {
        return queue.poll(timeoutInSeconds, TimeUnit.SECONDS);
    }

    // push a record to the end of the queue waiting indefinitely
    public void putRecord(PipelineRecord rec) throws InterruptedException {
        queue.put(rec);
    }
}
