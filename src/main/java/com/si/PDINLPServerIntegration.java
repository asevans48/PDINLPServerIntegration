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

import com.si.celery.task.result.AsyncResult;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * Describe your step plugin.
 * 
 */
public class PDINLPServerIntegration extends BaseStep implements StepInterface {

  private Object[][] batch;
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
      JSONArray resultBatch = (JSONArray) result.getResult();
      if(resultBatch.size() == inrows.length){
        for(int i = 0; i < resultBatch.size(); i++){
          String resultStr = (String) resultBatch.get(i);
          Object[] r = inrows[i].clone();
          int idx = this.data.outputRowMeta.indexOfValue(this.meta.getOutField());
          r[idx] = resultStr;
          nrows[i] = r;
        }
      }else{
        throw new AssertionError("Result from Celery Must be Same Length as Sent Batch");
      }
      return nrows;
    }

  /**
   * Obtain the results through celery
   *
   * @param batch The batch
   * @return  Processed batch results
   */
    public Object[][] getResults(Object[][] batch){
      if(this.data.getCelery() == null){
        //create celery as needed
    }

      //create args

      //send task and get the future

      //process the return value
      return null;
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
   * Process a row
   * @param smi step meta interface for the row
   * @param sdi step data interface for the row
   * @return  whether the processing succeeded
   * @throws KettleException  Thrown if there is an error in kettle
   */
    public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
      Object[] r = getRow(); // get row, set busy!
      if ( r == null ) {
        // no more input to be expected...
        setOutputDone();
        return false;
      }

      if(first){
        this.data = (PDINLPServerIntegrationData) sdi;
        this.meta = (PDINLPServerIntegrationMeta) meta;
        this.setupProcessor();
        int bsize = meta.getBatchSize().intValue();
        this.batch = new Object[bsize][];
      }

      Object[] nrow = r.clone();
      this.batch[this.currBatch] = nrow;
      this.currBatch += 1;
      if(this.currBatch == this.meta.getBatchSize().intValue()) {
        Object[][] results = this.getResults(this.batch);
        for(int i = 0; i < results.length; i++) {
          Object[] outRow = results[i].clone();
          putRow(getInputRowMeta(), outRow);
        }
        this.batch = new Object[this.meta.getBatchSize().intValue()][];
      }

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isBasic() )
          logBasic( BaseMessages.getString( PKG, "PDINLPServerIntegration.Log.LineNumber" ) + getLinesRead() );
      }

      return true;
    }
}