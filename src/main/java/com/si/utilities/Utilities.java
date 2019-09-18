package com.si.utilities;

import com.si.PDINLPServerIntegrationData;
import com.si.PDINLPServerIntegrationMeta;
import com.si.celery.Celery;
import com.si.celery.conf.Config;
import com.si.celery.enums.ThreadPoolType;
import com.si.celery.task.result.AsyncResult;
import com.si.rows.RowSet;
import org.eclipse.jetty.util.ajax.JSON;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
     * Get the NER Results
     *
     * @param outRows    Output Row Set
     * @param objArr    The object array
     * @param inrow     The input row
     * @return  RowSet containing the object arrays
     */
    private RowSet getNERResults(RowSet outRows, JSONArray objArr, Object[] inrow){
        if(objArr != null && objArr.size() > 0) {
            String outFieldName = this.meta.getOutField();
            int idx = this.data.outputRowMeta.indexOfValue(outFieldName);
            if(idx < inrow.length){
                for(int i = 0; i < objArr.size(); i++){
                    Object[] cloneRow = inrow.clone();
                    Object val = objArr.get(i);
                    if(val != null){
                        cloneRow[idx] = val;
                    }
                    outRows.add(cloneRow);
                }
            }else{
                throw new IndexOutOfBoundsException("Field Index Out of Bounds");
            }
        }
        return outRows;
    }

    /**
     * Package a specfic named entity
     * @param inRows    The input rows
     * @param entityType    The entity type
     * @param resultObj     The result object
     * @param inRow     The input row
     * @return  The updated row set
     */
    private RowSet getNamedEntities(RowSet inRows, String entityType, JSONObject resultObj, Object[] inRow){
        Object results = resultObj.get(entityType);
        if(results != null) {
            JSONArray resultsArr = (JSONArray) results;
            return this.getNERResults(inRows, resultsArr, inRow);
        }else{
            inRows.add(inRow);
        }
        return inRows;
    }

    /**
     * Get the entities
     *
     * @param inRows        The input rows
     * @param resultObj     The result batch for the row
     * @param inRow     The input row from the previous step
     * @return  The updated RowSet
     */
    private RowSet getNamedEntityResults(RowSet inRows, JSONObject resultObj, Object[] inRow){
        RowSet outRowSet = inRows;
        String[] entities = meta.getEntities().split(";");
        List<String> entityList = Arrays.asList(entities);
        Boolean parsed = false;
        if(entityList.contains("PERSON")){
            outRowSet = this.getNamedEntities(inRows, "PERSON", resultObj, inRow);
            parsed = true;
        }

        if(entityList.contains("ORGANIZATION")){
            outRowSet = this.getNamedEntities(inRows, "ORGANIZATION", resultObj, inRow);
            parsed = true;
        }

        if(entityList.contains("LOCATION")){
            outRowSet = this.getNamedEntities(inRows, "LOCATION", resultObj, inRow);
            parsed = true;
        }

        if(entityList.contains("FACILITY")){
            outRowSet = this.getNamedEntities(inRows, "FACILITY", resultObj, inRow);
            parsed = true;
        }

        if(entityList.contains("LANGUAGE")){
            outRowSet = this.getNamedEntities(inRows, "LANGUAGE", resultObj, inRow);
            parsed = true;
        }

        if(entityList.contains("DATE")){
            outRowSet = this.getNamedEntities(inRows, "DATE", resultObj, inRow);
            parsed = true;
        }

        if(entityList.contains("EVENT")){
            outRowSet = this.getNamedEntities(inRows, "EVENT", resultObj, inRow);
            parsed = true;
        }

        if(entityList.contains("PRODUCT")){
            outRowSet = this.getNamedEntities(inRows, "PRODUCT", resultObj, inRow);
            parsed = true;
        }
        if(parsed == false){
            throw new IllegalArgumentException("Entity Not Found");
        }
        return outRowSet;
    }

    /**
     * For relevant tasks below. Append a row to the row set.
     *
     * @param inRows    The input row set
     * @param output    The output string
     * @param inRow     The input row object
     * @return  The updated row set
     */
    private RowSet appendStringToSet(RowSet inRows, String output, Object[] inRow){
        String outField = this.meta.getOutField();
        int idx = this.data.outputRowMeta.indexOfValue(outField);
        if(idx < inRow.length){
            inRow[idx] = output;
        }
        return inRows;
    }

    /**
     * Obtain text rows from the result set
     *
     * @param inRows    The input row set
     * @param resultArr The result array
     * @param inRow     The object array
     * @return  The updated row set
     */
    private RowSet getTextRows(RowSet inRows, JSONArray resultArr, Object[] inRow){
        RowSet outRows = inRows;
        if(resultArr != null && resultArr.size() > 0){
            for(int i = 0; i < resultArr.size(); i++){
                String result = (String) resultArr.get(i);
                outRows = this.appendStringToSet(inRows, result, inRow);
            }
        }else{
            outRows.add(inRow);
        }
        return outRows;
    }

    /**
     * Process Result Rows
     * @param jarrObj   The object from the results
     * @param task      The task
     * @param inrows    The input rows
     * @param inRowSet  The input row set
     * @return  The updated row set
     */
    private RowSet processResultRows(Object jarrObj, String task, Object[][] inrows, RowSet inRowSet){
        RowSet outRows = inRowSet;
        if(jarrObj != null) {
            JSONArray jarr = (JSONArray) jarrObj;
            for(Object[] row: inrows) {
                if(task.equals("nertask")) {
                    for(int i = 0; i < jarr.size(); i++) {
                        JSONObject jobj = (JSONObject) jarr.get(i);
                        outRows = this.getNamedEntityResults(inRowSet, jobj, row);
                    }
                }else if(task.equals("sentencetokenizer") || task.equals("texttilingtokenizer")){
                    outRows = this.getTextRows(inRowSet, jarr, row);
                }
            }
        }else{
            for(Object[] row: inrows) {
                outRows.add(row);
            }
        }
        return outRows;
    }

    /**
     * Returns the results packaged into the new rows
     * @param inrows  The input rows
     * @param result The result from Celery which should contain a batch of results
     * @return  A RowSet
     * @throws ParseException  thrown when the celery request fails
     * @throws AssertionError  thrown when result size does not equal batch size
     * @throws IllegalArgumentException thrown when task or another critical variable is null
     */
    public RowSet packageResult(Object[][] inrows, AsyncResult result) throws ParseException, AssertionError, IllegalArgumentException {
        RowSet outRows = new RowSet();
        result.parsePayloadJson();
        if(result.isSuccess()) {
            JSONObject jobj= (JSONObject) result.getPayload();
            if (jobj != null && ((Boolean) jobj.get("err")) == false) {
                String task = this.meta.getNerTask().toLowerCase().trim();
                if(task != null) {
                    Object jarrObj = null;
                    if (task.equals("nertask")) {
                        jarrObj = jobj.get("entities");
                    }else if(task.equals("sentencetokenizer")){
                        jarrObj = jobj.get("sentences");
                    }else if(task.equals("texttilingtokenizer")){
                        jarrObj = jobj.get("topics");
                    }
                    if(jarrObj != null){
                        outRows = this.processResultRows(jarrObj, task, inrows, outRows);
                    }else{
                        throw new IllegalArgumentException("Task not Found");
                    }
                }else{
                    throw new IllegalArgumentException("NER Task Not Specified");
                }
            } else {
                throw new AssertionError("Result from Celery Must be Same Length as Sent Batch");
            }
        }
        return outRows;
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
     * @throws IllegalArgumentException Thrown when an option is missing
     */
    public RowSet getResults(Object[][] batch) throws CloneNotSupportedException, IllegalArgumentException, InterruptedException, ExecutionException, TimeoutException, ParseException {
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
        RowSet packagedRows = this.packageResult(batch, rval);
        return packagedRows;
    }

    /**
     * Properly close celery
     */
    public void close(){
        this.data.getCelery().close();
    }
}
