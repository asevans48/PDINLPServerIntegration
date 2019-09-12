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

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.List;


/**
 * Skeleton for PDI Step plugin.
 */
@Step( id = "PDINLPServerIntegration", image = "PDINLPServerIntegration.svg", name = "NLP Server Integration",
    description = "Calls my NLP Server to process text.", categoryDescription = "Transform" )
public class PDINLPServerIntegrationMeta extends BaseStepMeta implements StepMetaInterface {
  private String inField;
  private String outField;
  private Long batchSize;
  private String defaultQueue;
  private String brokerURI;
  private String backendURI;
  private String nerTask;
  private Long hardTimeLimit = 180000L;
  private Long timeLimit = 180000L;
  private Long delay = 0L;
  private Long attempts = 0L;
  private static Class<?> PKG = PDINLPServerIntegration.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  public PDINLPServerIntegrationMeta() {
    super(); // allocate BaseStepMeta
  }

  public String getInField() {
    return inField;
  }

  public void setInField(String inField) {
    this.inField = inField;
  }

  public String getOutField() {
    return outField;
  }

  public void setOutField(String outField) {
    this.outField = outField;
  }

  public Long getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(Long batchSize) {
    this.batchSize = batchSize;
  }

  public String getBrokerURI() {
    return brokerURI;
  }

  public void setBrokerURI(String brokerURI) {
    this.brokerURI = brokerURI;
  }

  public String getBackendURI() {
    return backendURI;
  }

  public void setBackendURI(String backendURI) {
    this.backendURI = backendURI;
  }

  public Long getHardTimeLimit() {
    return hardTimeLimit;
  }

  public void setHardTimeLimit(Long hardTimeLimit) {
    this.hardTimeLimit = hardTimeLimit;
  }

  public Long getTimeLimit() {
    return timeLimit;
  }

  public void setTimeLimit(Long timeLimit) {
    this.timeLimit = timeLimit;
  }

  public Long getDelay() {
    return delay;
  }

  public void setDelay(Long delay) {
    this.delay = delay;
  }

  public String getNerTask() {
    return nerTask;
  }

  public void setNerTask(String nerTask) {
    this.nerTask = nerTask;
  }

  public String getDefaultQueue() {
    return defaultQueue;
  }

  public void setDefaultQueue(String defaultQueue) {
    this.defaultQueue = defaultQueue;
  }

  public Long getAttempts() {
    return attempts;
  }

  public void setAttempts(Long attempts) {
    this.attempts = attempts;
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode );
  }

  public String getXML() throws KettleValueException{
    StringBuilder xml = new StringBuilder();
    if(this.batchSize == null){
      this.batchSize = 1L;
    }
    xml.append(XMLHandler.addTagValue("inField", this.inField));
    xml.append(XMLHandler.addTagValue("outField", this.outField));
    xml.append(XMLHandler.addTagValue("backendURI", this.backendURI));
    xml.append(XMLHandler.addTagValue("brokerURI", this.brokerURI));
    xml.append(XMLHandler.addTagValue("batchSize", this.batchSize));
    xml.append(XMLHandler.addTagValue("nerTask", this.nerTask));
    xml.append(XMLHandler.addTagValue("hardTimeLimit", this.hardTimeLimit));
    xml.append(XMLHandler.addTagValue("timeLimit", this.timeLimit));
    xml.append(XMLHandler.addTagValue("delay", this.delay));
    xml.append(XMLHandler.addTagValue("defaultQueue", this.defaultQueue));
    xml.append(XMLHandler.addTagValue("attempts", this.attempts));
    return xml.toString();
  }
  
  private void readData( Node stepnode ) throws KettleXMLException {
    // Parse the XML (starting with the given stepnode) to extract the step metadata (into member variables, for example)
    try{
      setInField(Const.NVL(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "inField")), ""));
      setOutField(Const.NVL(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "outField")), ""));
      setBackendURI(Const.NVL(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "backendURI")), ""));
      setBrokerURI(Const.NVL(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "brokerURI")), ""));
      setBatchSize(Long.parseLong(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "batchSize"))));
      setNerTask(Const.NVL(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "nerTask")), ""));
      setHardTimeLimit(Long.parseLong(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "hardTimeLimit"))));
      setTimeLimit(Long.parseLong(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "timeLimit"))));
      setDelay(Long.parseLong(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "delay"))));
      setAttempts(Long.parseLong(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "attempts"))));
      setDefaultQueue(Const.NVL(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "defaultQueue")), ""));
    }catch(Exception e){
      throw new KettleXMLException("NLP Server Plugin unable to read step info from XML node", e);
    }
  }

  public void setDefault() {
    this.inField = "";
    this.outField = "";
    this.brokerURI = "";
    this.backendURI = "";
    this.nerTask = "";
    this.batchSize = 1L;
    this.hardTimeLimit = 180000L;
    this.timeLimit = 180000L;
    this.delay = 0L;
    this.defaultQueue = "celery";
    this.attempts = 1L;
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
    this.inField = rep.getStepAttributeString(id_step, "inField");
    this.outField = rep.getStepAttributeString(id_step, "outField");
    this.brokerURI = rep.getStepAttributeString(id_step, "brokerURI");
    this.backendURI = rep.getStepAttributeString(id_step, "backendURI");
    this.batchSize = rep.getStepAttributeInteger(id_step, "batchSize");
    this.nerTask = rep.getStepAttributeString(id_step, "nerTask");
    this.hardTimeLimit = rep.getStepAttributeInteger(id_step, "hardTimeLimit");
    this.timeLimit = rep.getStepAttributeInteger(id_step, "timeLimit");
    this.delay = rep.getStepAttributeInteger(id_step, "delay");
    this.defaultQueue = rep.getStepAttributeString(id_step, "defaultQueue");
    this.attempts = rep.getJobEntryAttributeInteger(id_step, "attempts");
  }
  
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    rep.saveStepAttribute(id_transformation, id_step, "inField", this.inField);
    rep.saveStepAttribute(id_transformation, id_step, "outField", this.outField);
    rep.saveStepAttribute(id_transformation, id_step, "backendURI", this.backendURI);
    rep.saveStepAttribute(id_transformation, id_step, "brokerURI", this.brokerURI);
    rep.saveStepAttribute(id_transformation, id_step, "batchSize", this.batchSize);
    rep.saveStepAttribute(id_transformation, id_step, "nerTask", this.nerTask);
    rep.saveStepAttribute(id_transformation, id_step, "hardTimeLimit", this.hardTimeLimit);
    rep.saveStepAttribute(id_transformation, id_step, "timeLimit", this.timeLimit);
    rep.saveStepAttribute(id_transformation, id_step, "delay", this.delay);
    rep.saveStepAttribute(id_transformation, id_step, "defaultQueue", this.defaultQueue);
    rep.saveStepAttribute(id_transformation, id_step, "attempts", this.attempts);
  }
  
  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep, 
    VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
    // Default: nothing changes to rowMeta
    if(!Arrays.asList(rowMeta.getFieldNames()).contains(this.outField)){
      ValueMetaInterface v0 = new ValueMetaString(this.outField);
      v0.setTrimType(ValueMetaInterface.TRIM_TYPE_BOTH);
      v0.setOrigin(this.outField);
      rowMeta.addValueMeta(v0);
    }
  }
  
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, 
    StepMeta stepMeta, RowMetaInterface prev, String input[], String output[],
    RowMetaInterface info, VariableSpace space, Repository repository, 
    IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString( PKG, "PDINLPServerIntegrationMeta.CheckResult.NotReceivingFields" ), stepMeta ); 
      remarks.add( cr );
    }
    else {
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG, "PDINLPServerIntegrationMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta );  
      remarks.add( cr );
    }
    
    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG, "PDINLPServerIntegrationMeta.CheckResult.StepRecevingData2" ), stepMeta ); 
      remarks.add( cr );
    }
    else {
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG, "PDINLPServerIntegrationMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta ); 
      remarks.add( cr );
    }
  }
  
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr, Trans trans ) {
    return new PDINLPServerIntegration( stepMeta, stepDataInterface, cnr, tr, trans );
  }
  
  public StepDataInterface getStepData() {
    return new PDINLPServerIntegrationData();
  }

  public String getDialogClassName() {
    return "com.si.PDINLPServerIntegrationDialog";
  }
}
