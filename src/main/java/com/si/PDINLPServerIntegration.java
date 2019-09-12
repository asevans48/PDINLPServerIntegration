/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.si;

import com.si.celery.Celery;
import com.si.celery.conf.Config;
import com.si.celery.enums.ThreadPoolType;
import com.si.celery.task.result.AsyncResult;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Describe your step plugin.
 * 
 */
public class PDINLPServerIntegration extends BaseStep implements StepInterface {

  private ArrayList<Object[]> batch;
  private int currBatch;
  private PDINLPServerIntegrationData data;
  private PDINLPServerIntegrationMeta meta;

  private static Class<?> PKG = PDINLPServerIntegrationMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  /**
   * The constructor
   * @param stepMeta  Step meta
   * @param stepDataInterface Step data
   * @param copyNr  Copy number or thread number
   * @param transMeta transformation meat
   * @param trans transformation object
   */
  public PDINLPServerIntegration( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }
  
  /**
     * Initialize and do work where other steps need to wait for...
     *
     * @param stepMetaInterface
     *          The metadata to work with
     * @param stepDataInterface
     *          The data to initialize
     */
    public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
      return super.init( stepMetaInterface, stepDataInterface );
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
   * @return  The output arguments
   * @throws NullPointerException thrown check objects fails
   */
    public Object[] createArgs(Object[][] inRows) throws NullPointerException{
      this.checkObjects();
      Object[] args = new Object[inRows.length];
      int idx = this.data.outputRowMeta.indexOfValue(this.meta.getInField());
      if(idx > -1){
        for(int i = 0; i < inRows.length; i++){
          Object[] orow = inRows[i];
          if(idx >= orow.length) {
            Object arg = orow[idx];
            args[i] = arg;
          }else if(isBasic()){
            logBasic("Argument not Present for Row in NLP Server");
          }
        }
      }else{
        throw new IllegalArgumentException("Index for Output Field Not found in NLP Server Integration");
      }
      return args;
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
      Future<AsyncResult> fut = null;
      if(this.data.getCelery() != null){
        fut = this.data.getCelery().sendTask(taskName, args, kwargs);
      }
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
      }else if(isBasic()){
        logBasic("Failed to parser results in Celery for NLP Server");
      }
      return nrows;
    }

    public void setCelery(){
      //create celery as needed
      Config cfg = Config
              .builder()
              .broker(this.meta.getBrokerURI())
              .acceptContent("application/json")
              .resultBackend(this.meta.getBackendURI())
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
      long hlimit = this.meta.getHardTimeLimit() / 1000;
      long slimit = this.meta.getTimeLimit() / 1000;
      long delay = this.meta.getDelay() / 1000;
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
      Object[] args = this.createArgs(batch);

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
   * Setup the processor
   * @throws KettleException  Thrown if there is an error in kettle
   */
  public void setupProcessor() throws KettleException{
      RowMetaInterface inMeta = getInputRowMeta().clone();
      data.outputRowMeta = inMeta;
      meta.getFields(data.outputRowMeta, getStepname(), null, null, this, null, null);
      first = false;
    }

  /**
   * Push rows to output
   * @param rows  The rows
   * @throws KettleStepException  thrown by kettle
   */
  public void pushRows(Object[][] rows) throws KettleStepException {
    for(Object[] row: rows) {
      putRow(getInputRowMeta(), row);
    }
   }

  /**
   * Process a row
   * @param smi step meta interface for the row
   * @param sdi step data interface for the row
   * @return  whether the processing succeeded
   * @throws KettleException  Thrown if there is an error in kettle
   */
    public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
      Object[] r = getRow(); // get row, set busy!
      if ( r == null ) {
        // no more input to be expected
        if(this.data.getCelery() != null) {
          try{
            this.data.getCelery().close();
          }finally{
            setOutputDone();
            return false;
          }
        }else {
          setOutputDone();
          return false;
        }
      }

      if(first){
        this.data = (PDINLPServerIntegrationData) sdi;
        this.meta = (PDINLPServerIntegrationMeta) meta;
        this.setupProcessor();
        int bsize = meta.getBatchSize().intValue();
        this.batch = new ArrayList<Object[]>();
      }

      Object[] nrow = null;
      if(r != null) {
        nrow = r.clone();
        nrow = RowDataUtil.resizeArray(nrow, this.data.outputRowMeta.size());
        this.batch.add(nrow);
        this.currBatch += 1;
      }

      if((this.currBatch == this.meta.getBatchSize().intValue() || nrow == null) && this.batch.size() > 0){
        Object[][] obatch = new Object[this.batch.size()][];
        for(int i = 0; i < this.batch.size(); i++){
          Object[] or = this.batch.get(i);
          obatch[i] = or;
        }
        Object[][] results = obatch;
        try {
          results = this.getResults(obatch);
        } catch (CloneNotSupportedException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        } catch (TimeoutException e) {
          e.printStackTrace();
        } catch (ParseException e) {
          e.printStackTrace();
        }
        this.pushRows(results);
        this.batch = new ArrayList<Object[]>();
      }

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isBasic() )
          logBasic( BaseMessages.getString( PKG, "PDINLPServerIntegration.Log.LineNumber" ) + getLinesRead() );
      }

      return true;
    }
}