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

import com.si.utilities.Utilities;
import org.json.simple.parser.ParseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Describe your step plugin.
 * 
 */
public class PDINLPServerIntegration extends BaseStep implements StepInterface {
  private Utilities utilities;
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
        this.utilities = new Utilities();
        this.data = (PDINLPServerIntegrationData) sdi;
        this.meta = (PDINLPServerIntegrationMeta) meta;
        this.utilities.setData(data);
        this.utilities.setMeta(meta);
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
          results = this.utilities.getResults(obatch);
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