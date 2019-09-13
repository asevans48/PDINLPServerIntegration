package com.si.utilities;

import com.si.PDINLPServerIntegrationData;
import com.si.PDINLPServerIntegrationMeta;
import com.si.celery.Celery;
import com.si.celery.conf.Config;
import com.si.celery.enums.ThreadPoolType;
import com.si.celery.task.result.AsyncResult;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Utilities {
    private PDINLPServerIntegrationData data;
    private PDINLPServerIntegrationMeta meta;

    public void setData(PDINLPServerIntegrationData data) {
        this.data = data;
    }

    public void setMeta(PDINLPServerIntegrationMeta meta) {
        this.meta = meta;
    }

    /**
     * Ensures that objects are not null
     *
     * @throws NullPointerException if data, meta, infield, or outfield is null
     */
    public void checkObjects() throws NullPointerException{
        if(this.data == null){
            throw new NullPointerException("Data is null for NLP Server Integration");
        }
        if(this.meta == null){
            throw new NullPointerException("Meta is null for NLP Server Integration");
        }
        if(this.meta.getInField() == null){
            throw new NullPointerException("Input Field is null for NLP Server Integration");
        }
        if(this.meta.getOutField() == null){
            throw new NullPointerException("Output Field is null for NLP Server Integration");
        }
    }

    /**
     * Create the arguments for the message
     * @param inRows  The input rows
     * @param idx   Index of the argument field
     * @return  The output arguments
     * @throws NullPointerException thrown check objects fails
     */
    public Object[] createArgs(Object[][] inRows, int idx) throws NullPointerException{
        Object[][] outArgs = null;
        if(this.meta.getEntities() != null && this.meta.getEntities().trim().length() > 0){
            outArgs = new Object[2][];
            outArgs[1] = this.meta.getEntities().split(";");
        }else{
            outArgs = new Object[1][];
        }
        this.checkObjects();
        Object[] args = new Object[inRows.length];
        if(idx > -1){
            for(int i = 0; i < inRows.length; i++){
                Object[] orow = inRows[i];
                if(idx < orow.length) {
                    Object arg = orow[idx];
                    args[i] = arg;
                }
            }
        }else{
            throw new IllegalArgumentException("Index for Output Field Not found in NLP Server Integration");
        }
        outArgs[0] = args;
        return outArgs;
    }

    /**
     * Send the task to celery and obtain the result
     * @param taskName  The task name
     * @param args  arguments for the task
     * @param kwargs  kwargs for the task
     * @return  awaitable future with the arguments
     * @throws CloneNotSupportedException on failure to clone the config
     */
    public Future<AsyncResult> sendTask(String taskName, Object[] args, Map<String, Object> kwargs) throws CloneNotSupportedException {
        if(this.data.getCelery() == null){
            this.setCelery();
        }
        Future<AsyncResult> fut = this.data.getCelery().sendTask(taskName, args, kwargs);
        return fut;
    }

    /**
     * Returns the results packaged into the new rows
     * @param inrows  The input rows
     * @param result The result from Celery which should contain a batch of results
     * @return  A packaged double array of results
     * @throws ParseException  thrown when the celery request fails
     * @throws AssertionError  thrown when result size does not equal batch size
     */
    public Object[][] packageResult(Object[][] inrows, AsyncResult result) throws ParseException, AssertionError {
        Object[][] nrows = new Object[inrows.length][];
        result.parsePayloadJson();
        if(result.isSuccess()) {
            JSONArray resultBatch = (JSONArray) result.getResult();
            if (resultBatch.size() == inrows.length) {
                for (int i = 0; i < resultBatch.size(); i++) {
                    String resultStr = (String) resultBatch.get(i);
                    Object[] r = inrows[i].clone();
                    int idx = this.data.outputRowMeta.indexOfValue(this.meta.getOutField());
                    r[idx] = resultStr;
                    nrows[i] = r;
                }
            } else {
                throw new AssertionError("Result from Celery Must be Same Length as Sent Batch");
            }
        }
        return nrows;
    }

    /**
     * Create the celery configuration
     */
    public void setCelery(){
        //create celery as needed
        Config cfg = Config
                .builder()
                .defaultRoutingKey(this.meta.getDefaultRoutingKey())
                .broker(this.meta.getBrokerURI())
                .acceptContent("application/json")
                .resultBackend(this.meta.getBackendURI())
                .defaultQueue(this.meta.getDefaultQueue())
                .defaultRoutingKey(this.meta.getDefaultRoutingKey())
                .threadpoolExecutor(ThreadPoolType.THREAD_POOL_EXECUTOR)
                .numThreads(1)
                .build();
        this.data.setConfig(cfg);
        Celery celery = Celery
                .builder()
                .conf(cfg)
                .brokerURI(this.meta.getBrokerURI())
                .backendURI(this.meta.getBackendURI())
                .app_name("pdi_nlp_server_app")
                .build();
        long hlimit = this.meta.getHardTimeLimit();
        long slimit = this.meta.getTimeLimit();
        long delay = this.meta.getDelay();
        celery.setHardTimeLimit(hlimit);
        celery.setTimeLimit(slimit);
        celery.setDelay(delay);
        this.data.setCelery(celery);
    }

    /**
     * Get results from celery
     *
     * @param batch The batch for the task to execute
     * @return  Batch results from celery
     * @throws CloneNotSupportedException When cloning the config fails
     * @throws InterruptedException When the future fails
     * @throws ExecutionException When the future fails
     * @throws TimeoutException When the future fails
     * @throws ParseException Thrown when parsing the results fails
     */
    public Object[][] getResults(Object[][] batch) throws CloneNotSupportedException, InterruptedException, ExecutionException, TimeoutException, ParseException {
        //create celery if necessary
        if(this.data.getCelery() == null){
            this.setCelery();
        }

        //create args
        int idx = this.data.outputRowMeta.indexOfValue(this.meta.getInField());
        Object[] args = this.createArgs(batch, idx);

        //send task and get the future
        String taskName = this.meta.getNerTask();
        Future<AsyncResult> fut = this.sendTask(taskName, args, null);

        //process the return value
        AsyncResult rval = null;
        int attempt = 0;
        do {
            if (this.meta.getHardTimeLimit() > 0) {
                rval = fut.get(this.meta.getHardTimeLimit(), TimeUnit.MILLISECONDS);
            } else {
                rval = fut.get();
            }
            attempt += 1;
        }while((rval == null || rval.isSuccess() == false) && attempt < this.meta.getAttempts());
        Object[][] packagedRows = this.packageResult(batch, rval);
        return packagedRows;
    }

    /**
     * Properly close celery
     */
    public void close(){
        this.data.getCelery().close();
    }
}
