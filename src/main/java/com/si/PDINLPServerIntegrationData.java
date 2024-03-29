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
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;


public class PDINLPServerIntegrationData extends BaseStepData implements StepDataInterface {
  // Add any execution-specific data here
  private Celery celery;
  private Config config;
  public RowMetaInterface outputRowMeta;

  /**
   * Constructor
   */
  public PDINLPServerIntegrationData() {
    super();
  }

  /**
   * Set the celery object
   *
   * @param celery  The celery object
   */
  public void setCelery(Celery celery){
    this.celery = celery;
  }

  /**
   * Obtain the celery object
   *
   * @return  The celery object
   */
  public Celery getCelery(){
    return this.celery;
  }

  /**
   * Get the configuration for celery
   * @return  the configuration
   */
  public Config getConfig() {
    return config;
  }

  /**
   * Set the configuration for celery
   * @param config  The configuration for celery
   */
  public void setConfig(Config config) {
    this.config = config;
  }
}