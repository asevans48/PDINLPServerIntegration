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

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.FormDataBuilder;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class PDINLPServerIntegrationDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = PDINLPServerIntegrationMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private static final int MARGIN_SIZE = 15;
  private static final int LABEL_SPACING = 5;
  private static final int ELEMENT_SPACING = 10;

  private static final int LARGE_FIELD = 350;
  private static final int MEDIUM_FIELD = 250;
  private static final int SMALL_FIELD = 75;

  private PDINLPServerIntegrationMeta meta;

  private Label wlStepname;
  private Text wStepname;
  private FormData fdStepname, fdlStepname;

  private Label lfname;
  private CCombo wInFieldCombo;
  private FormData fdlFname, fdStep;

  private Label wOutFieldName;
  private TextVar wOutField;
  private FormData fdlOutFieldName, fdlOutField;

  private Label brokerURI;
  private TextVar wBrokerURI;
  private FormData fdlBrokerURI, fdlBrokerURIField;

  private Label backendURI;
  private TextVar wBackendURI;
  private FormData fdlBackendURI, fdlBackendURIField;

  private Label hardTimeLimit;
  private TextVar wHardTimeLimit;
  private FormData fdlHardTimeLimit, fdlHardTimeLimitField;

  private Label timeLimit;
  private TextVar wTimeLimit;
  private FormData fdlTimeLimit, fdlTimeLimitField;

  private Label delay;
  private TextVar wDelay;
  private FormData fdlDelay, fdlDelayField;

  private Label nerTask;
  private TextVar wNerTask;
  private FormData fdlNerTask, fdlNerTaskField;

  private Label defaultQueue;
  private TextVar wDefaultQueue;
  private FormData fdlDefaultQueue, fdlDefaultQueueField;

  private Label batchSize;
  private TextVar wBatchSize;
  private FormData fdlBatchSize, fdlBatchSizeField;

  private Label attempts;
  private TextVar wAttempts;
  private FormData fdlAttempts, fdlAttemptsField;

  private Label entities;
  private TextVar wEntities;
  private FormData fdlEntities, fdlEntitiesField;

  private Label routingKey;
  private TextVar wRoutingKey;
  private FormData fdlRoutingKey, fdlRoutingKeyField;


  private Button wCancel;
  private Button wOK;
  private ModifyListener lsMod;
  private Listener lsCancel;
  private Listener lsOK;
  private SelectionAdapter lsDef;
  private boolean changed;

  public PDINLPServerIntegrationDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    meta = (PDINLPServerIntegrationMeta) in;
  }

  public String open() {
    // store some convenient SWT variables
    Shell parent = getParent();
    Display display = parent.getDisplay();

    // SWT code for preparing the dialog
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, meta);

    // Save the value of the changed flag on the meta object. If the user cancels
    // the dialog, it will be restored to this saved value.
    // The "changed" variable is inherited from BaseStepDialog
    changed = meta.hasChanged();

    // The ModifyListener used on all controls. It will update the meta object to
    // indicate that changes are being made.
    ModifyListener lsMod = new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        meta.setChanged();
      }
    };

    // ------------------------------------------------------- //
    // SWT code for building the actual settings dialog        //
    // ------------------------------------------------------- //
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Shell.Title"));
    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label(shell, SWT.RIGHT);
    wlStepname.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Stepname.Label"));
    props.setLook(wlStepname);
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment(0, 0);
    fdlStepname.right = new FormAttachment(middle, -margin);
    fdlStepname.top = new FormAttachment(0, margin);
    wlStepname.setLayoutData(fdlStepname);

    wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wStepname.setText(stepname);
    props.setLook(wStepname);
    wStepname.addModifyListener(lsMod);
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment(middle, 0);
    fdStepname.top = new FormAttachment(0, margin);
    fdStepname.right = new FormAttachment(100, 0);
    wStepname.setLayoutData(fdStepname);

    //output field name
    lfname = new Label( shell, SWT.RIGHT );
    lfname.setText( BaseMessages.getString( PKG, "NominatimPDIPluginDialog.Fields.InField" ) );
    props.setLook( lfname );
    fdlFname = new FormData();
    fdlFname.left = new FormAttachment( 0, 0 );
    fdlFname.right = new FormAttachment( middle, -margin );
    fdlFname.top = new FormAttachment( wStepname, 15 );
    lfname.setLayoutData( fdlFname );

    wInFieldCombo = new CCombo( shell, SWT.BORDER );
    props.setLook( wInFieldCombo );
    StepMeta stepinfo = transMeta.findStep( stepname );
    if ( stepinfo != null ) {
      try {
        String[] fields = transMeta.getStepFields(stepname).getFieldNames();
        for (int i = 0; i < fields.length; i++) {
          wInFieldCombo.add(fields[i]);
        }
      }catch(KettleException e){
        if ( log.isBasic())
          logBasic("Failed to Get Step Fields");
      }
    }

    wInFieldCombo.addModifyListener( lsMod );
    fdStep = new FormData();
    fdStep.left = new FormAttachment( middle, 0 );
    fdStep.top = new FormAttachment( wStepname, 15 );
    fdStep.right = new FormAttachment( 100, 0 );
    wInFieldCombo.setLayoutData( fdStep );

    //outfield
    wOutFieldName = new Label(shell, SWT.RIGHT);
    wOutFieldName.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Fields.OutField"));
    props.setLook(wOutFieldName);
    fdlOutFieldName = new FormData();
    fdlOutFieldName.left = new FormAttachment(0, 0);
    fdlOutFieldName.top = new FormAttachment(lfname, 15);
    fdlOutFieldName.right = new FormAttachment(middle, -margin);
    wOutFieldName.setLayoutData(fdlOutFieldName);
    wOutField = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wOutField.setText("");
    wOutField.addModifyListener(lsMod);
    props.setLook(wOutField);
    fdlOutField = new FormData();
    fdlOutField.left = new FormAttachment(middle, 0);
    fdlOutField.top = new FormAttachment(lfname, 15);
    fdlOutField.right = new FormAttachment(100, 0);
    wOutField.setLayoutData(fdlOutField);

    //entities
    entities = new Label(shell, SWT.RIGHT);
    entities.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Fields.Entities"));
    props.setLook( entities );
    fdlEntities = new FormData();
    fdlEntities.left = new FormAttachment( 0, 0 );
    fdlEntities.right = new FormAttachment( middle, -margin );
    fdlEntities.top = new FormAttachment( wOutField, 15 );
    entities.setLayoutData( fdlEntities);

    wEntities = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wEntities.setText("");
    wEntities.addModifyListener(lsMod);
    props.setLook(wEntities);
    fdlEntitiesField = new FormData();
    fdlEntitiesField.left = new FormAttachment(middle, 0);
    fdlEntitiesField.top = new FormAttachment(wOutField, 15);
    fdlEntitiesField.right = new FormAttachment(100, 0);
    wEntities.setLayoutData(fdlEntitiesField);

    //set the broker  URI (optional if environment var set)
    brokerURI = new Label(shell, SWT.RIGHT);
    brokerURI.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Fields.Broker"));
    props.setLook( brokerURI );
    fdlBrokerURI = new FormData();
    fdlBrokerURI.left = new FormAttachment( 0, 0 );
    fdlBrokerURI.right = new FormAttachment( middle, -margin );
    fdlBrokerURI.top = new FormAttachment( wEntities, 15 );
    brokerURI.setLayoutData( fdlBrokerURI);

    wBrokerURI = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wBrokerURI.setText("");
    wBrokerURI.addModifyListener(lsMod);
    props.setLook(wBrokerURI);
    fdlBrokerURIField = new FormData();
    fdlBrokerURIField.left = new FormAttachment(middle, 0);
    fdlBrokerURIField.top = new FormAttachment(wEntities, 15);
    fdlBrokerURIField.right = new FormAttachment(100, 0);
    wBrokerURI.setLayoutData(fdlBrokerURIField);

    //set the backend URI (optional if environment var set)
    backendURI = new Label(shell, SWT.RIGHT);
    backendURI.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Fields.Backend"));
    props.setLook( backendURI );
    fdlBackendURI = new FormData();
    fdlBackendURI.left = new FormAttachment( 0, 0 );
    fdlBackendURI.right = new FormAttachment( middle, -margin );
    fdlBackendURI.top = new FormAttachment( wBrokerURI, 15 );
    backendURI.setLayoutData( fdlBackendURI);

    wBackendURI = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wBackendURI.setText("");
    wBackendURI.addModifyListener(lsMod);
    props.setLook(wBackendURI);
    fdlBackendURIField = new FormData();
    fdlBackendURIField.left = new FormAttachment(middle, 0);
    fdlBackendURIField.top = new FormAttachment(wBrokerURI, 15);
    fdlBackendURIField.right = new FormAttachment(100, 0);
    wBackendURI.setLayoutData(fdlBackendURIField);

    //set the task name
    nerTask = new Label(shell, SWT.RIGHT);
    nerTask.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Fields.NERTask"));
    props.setLook( nerTask );
    fdlNerTask = new FormData();
    fdlNerTask.left = new FormAttachment( 0, 0 );
    fdlNerTask.right = new FormAttachment( middle, -margin );
    fdlNerTask.top = new FormAttachment( wBackendURI, 15 );
    nerTask.setLayoutData( fdlNerTask);

    wNerTask = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wNerTask.setText("");
    wNerTask.addModifyListener(lsMod);
    props.setLook(wNerTask);
    fdlNerTaskField = new FormData();
    fdlNerTaskField.left = new FormAttachment(middle, 0);
    fdlNerTaskField.top = new FormAttachment(wBackendURI, 15);
    fdlNerTaskField.right = new FormAttachment(100, 0);
    wNerTask.setLayoutData(fdlNerTaskField);

    //hard timeLimit (default 180000)
    hardTimeLimit = new Label(shell, SWT.RIGHT);
    hardTimeLimit.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Fields.HardTimeLimit"));
    props.setLook( hardTimeLimit );
    fdlHardTimeLimit = new FormData();
    fdlHardTimeLimit.left = new FormAttachment( 0, 0 );
    fdlHardTimeLimit.right = new FormAttachment( middle, -margin );
    fdlHardTimeLimit.top = new FormAttachment( wNerTask, 15 );
    hardTimeLimit.setLayoutData( fdlHardTimeLimit);

    wHardTimeLimit = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wHardTimeLimit.setText("");
    wHardTimeLimit.addModifyListener(lsMod);
    props.setLook(wHardTimeLimit);
    fdlHardTimeLimitField = new FormData();
    fdlHardTimeLimitField.left = new FormAttachment(middle, 0);
    fdlHardTimeLimitField.top = new FormAttachment(wNerTask, 15);
    fdlHardTimeLimitField.right = new FormAttachment(100, 0);
    wHardTimeLimit.setLayoutData(fdlHardTimeLimitField);

    //time limit (default 180000)
    timeLimit = new Label(shell, SWT.RIGHT);
    timeLimit.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Fields.TimeLimit"));
    props.setLook( timeLimit );
    fdlTimeLimit = new FormData();
    fdlTimeLimit.left = new FormAttachment( 0, 0 );
    fdlTimeLimit.right = new FormAttachment( middle, -margin );
    fdlTimeLimit.top = new FormAttachment( hardTimeLimit, 15 );
    timeLimit.setLayoutData( fdlTimeLimit);

    wTimeLimit = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTimeLimit.setText("");
    wTimeLimit.addModifyListener(lsMod);
    props.setLook(wTimeLimit);
    fdlTimeLimitField = new FormData();
    fdlTimeLimitField.left = new FormAttachment(middle, 0);
    fdlTimeLimitField.top = new FormAttachment(hardTimeLimit, 15);
    fdlTimeLimitField.right = new FormAttachment(100, 0);
    wBatchSize.setLayoutData(fdlBatchSize);


    //delay (default 180000)
    delay = new Label(shell, SWT.RIGHT);
    delay.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Fields.Delay"));
    props.setLook( delay );
    fdlDelay = new FormData();
    fdlDelay.left = new FormAttachment( 0, 0 );
    fdlDelay.right = new FormAttachment( middle, -margin );
    fdlDelay.top = new FormAttachment( timeLimit, 15 );
    delay.setLayoutData( fdlBatchSize);

    wDelay = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wDelay.setText("");
    wDelay.addModifyListener(lsMod);
    props.setLook(wDelay);
    fdlDelayField = new FormData();
    fdlDelayField.left = new FormAttachment(middle, 0);
    fdlDelayField.top = new FormAttachment(timeLimit, 15);
    fdlDelayField.right = new FormAttachment(100, 0);
    wDelay.setLayoutData(fdlDelayField);

    //set the batch size (optional will default to 1)
    batchSize = new Label(shell, SWT.RIGHT);
    batchSize.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Fields.BatchSize"));
    props.setLook( batchSize );
    fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment( 0, 0 );
    fdlBatchSize.right = new FormAttachment( middle, -margin );
    fdlBatchSize.top = new FormAttachment( wDelay, 15 );
    batchSize.setLayoutData( fdlBatchSize);

    wBatchSize = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wBatchSize.setText("");
    wBatchSize.addModifyListener(lsMod);
    props.setLook(wBatchSize);
    fdlBatchSizeField = new FormData();
    fdlBatchSizeField.left = new FormAttachment(middle, 0);
    fdlBatchSizeField.top = new FormAttachment(wDelay, 15);
    fdlBatchSizeField.right = new FormAttachment(100, 0);
    wBatchSize.setLayoutData(fdlBatchSize);

    //set the default queue
    defaultQueue = new Label(shell, SWT.RIGHT);
    defaultQueue.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Fields.NERTask"));
    props.setLook( defaultQueue );
    fdlDefaultQueue = new FormData();
    fdlDefaultQueue.left = new FormAttachment( 0, 0 );
    fdlDefaultQueue.right = new FormAttachment( middle, -margin );
    fdlDefaultQueue.top = new FormAttachment( wBatchSize, 15 );
    defaultQueue.setLayoutData( fdlDefaultQueue);

    wDefaultQueue = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wDefaultQueue.setText("");
    wDefaultQueue.addModifyListener(lsMod);
    props.setLook(wDefaultQueue);
    fdlDefaultQueueField = new FormData();
    fdlDefaultQueueField.left = new FormAttachment(middle, 0);
    fdlDefaultQueueField.top = new FormAttachment(wBatchSize, 15);
    fdlDefaultQueueField.right = new FormAttachment(100, 0);
    wDefaultQueue.setLayoutData(fdlDefaultQueueField);

    //routing key (default to celery)
    routingKey = new Label(shell, SWT.RIGHT);
    routingKey.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Fields.RoutingKey"));
    props.setLook( routingKey );
    fdlRoutingKey = new FormData();
    fdlRoutingKey.left = new FormAttachment( 0, 0 );
    fdlRoutingKey.right = new FormAttachment( middle, -margin );
    fdlRoutingKey.top = new FormAttachment( wDefaultQueue, 15 );
    routingKey.setLayoutData( fdlRoutingKey);

    wRoutingKey = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wRoutingKey.setText("");
    wRoutingKey.addModifyListener(lsMod);
    props.setLook(wRoutingKey);
    fdlRoutingKeyField = new FormData();
    fdlRoutingKeyField.left = new FormAttachment(middle, 0);
    fdlRoutingKeyField.top = new FormAttachment(wDefaultQueue, 15);
    fdlRoutingKeyField.right = new FormAttachment(100, 0);
    wRoutingKey.setLayoutData(fdlRoutingKeyField);

    //number of attempts
    attempts = new Label(shell, SWT.RIGHT);
    attempts.setText(BaseMessages.getString(PKG, "NominatimPDIPluginDialog.Fields.Attempts"));
    props.setLook( attempts );
    fdlAttempts = new FormData();
    fdlAttempts.left = new FormAttachment( 0, 0 );
    fdlAttempts.right = new FormAttachment( middle, -margin );
    fdlAttempts.top = new FormAttachment( wRoutingKey, 15 );
    attempts.setLayoutData( fdlAttempts);

    wAttempts = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wAttempts.setText("");
    wAttempts.addModifyListener(lsMod);
    props.setLook(wDefaultQueue);
    fdlAttemptsField = new FormData();
    fdlAttemptsField.left = new FormAttachment(middle, 0);
    fdlAttemptsField.top = new FormAttachment(wRoutingKey, 15);
    fdlAttemptsField.right = new FormAttachment(100, 0);
    wAttempts.setLayoutData(fdlAttemptsField);

    // OK and cancel buttons
    wOK = new Button(shell, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    setButtonPositions(new Button[]{wOK, wCancel}, margin, attempts);

    // Add listeners for cancel and OK
    lsCancel = new Listener() {
      public void handleEvent(Event e) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent(Event e) {
        ok();
      }
    };
    wCancel.addListener(SWT.Selection, lsCancel);
    wOK.addListener(SWT.Selection, lsOK);

    // default listener (for hitting "enter")
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };
    wStepname.addSelectionListener(lsDef);
    wInFieldCombo.addSelectionListener(lsDef);
    wOutField.addSelectionListener(lsDef);
    wBrokerURI.addSelectionListener(lsDef);
    wBackendURI.addSelectionListener(lsDef);
    wBatchSize.addSelectionListener(lsDef);
    wNerTask.addSelectionListener(lsDef);
    wHardTimeLimit.addSelectionListener(lsDef);
    wTimeLimit.addSelectionListener(lsDef);
    wDelay.addSelectionListener(lsDef);
    wDefaultQueue.addSelectionListener(lsDef);
    wAttempts.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window and cancel the dialog properly
    shell.addShellListener(new ShellAdapter() {
      public void shellClosed(ShellEvent e) {
        cancel();
      }
    });

    // Set/Restore the dialog size based on last position on screen
    // The setSize() method is inherited from BaseStepDialog
    setSize();

    // populate the dialog with the values from the meta object
    getData();

    // restore the changed flag to original value, as the modify listeners fire during dialog population
    meta.setChanged(changed);

    // open dialog and enter event loop
    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    // at this point the dialog has closed, so either ok() or cancel() have been executed
    // The "stepname" variable is inherited from BaseStepDialog
    return stepname;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wStepname.selectAll();
    wInFieldCombo.setText(Const.NVL(meta.getInField(), ""));
    wOutField.setText(Const.NVL(meta.getOutField(), ""));
    wBrokerURI.setText(Const.NVL(meta.getBrokerURI(), ""));
    wBackendURI.setText(Const.NVL(meta.getBackendURI(),""));
    wBatchSize.setText(String.valueOf(meta.getBatchSize()));
    wNerTask.setText(Const.NVL(meta.getNerTask(), ""));
    wHardTimeLimit.setText(String.valueOf(meta.getHardTimeLimit()));
    wTimeLimit.setText(String.valueOf(meta.getTimeLimit()));
    wDelay.setText(String.valueOf(meta.getDelay()));
    wDefaultQueue.setText(String.valueOf(meta.getDefaultQueue()));
    wAttempts.setText(String.valueOf(meta.getAttempts()));
    wEntities.setText(Const.NVL(meta.getEntities(), ""));
    wRoutingKey.setText(Const.NVL(meta.getEntities(), ""));
    wStepname.setFocus();
  }

  private Image getImage() {
    PluginInterface plugin =
        PluginRegistry.getInstance().getPlugin( StepPluginType.class, stepMeta.getStepMetaInterface() );
    String id = plugin.getIds()[0];
    if ( id != null ) {
      return GUIResource.getInstance().getImagesSteps().get( id ).getAsBitmapForSize( shell.getDisplay(),
          ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );
    }
    return null;
  }

  private void cancel() {
    dispose();
  }

  private void ok() {
    stepname = wStepname.getText();
    String inField = wInFieldCombo.getText();
    String outField = wOutField.getText();
    String brokerURI = wBrokerURI.getText();
    String backendURI = wBackendURI.getText();
    String batchSize = wBatchSize.getText();
    String nerTask = wNerTask.getText();
    String hardTimeLimit = wHardTimeLimit.getText();
    String timeLimit = wTimeLimit.getText();
    String delay = wDelay.getText();
    String defaultQueue = wDefaultQueue.getText();
    String attempts = wAttempts.getText();
    String entities = wEntities.getText();
    String routingKey = wRoutingKey.getText();
    if(batchSize == null){
      batchSize = "1";
    }
    if(attempts == null){
      attempts = "1";
    }
    meta.setInField(inField);
    meta.setOutField(outField);
    meta.setBrokerURI(brokerURI);
    meta.setBackendURI(backendURI);
    meta.setBatchSize(Long.parseLong(batchSize));
    meta.setNerTask(nerTask);
    meta.setHardTimeLimit(Long.parseLong(hardTimeLimit));
    meta.setTimeLimit(Long.parseLong(timeLimit));
    meta.setDelay(Long.parseLong(delay));
    meta.setDefaultQueue(defaultQueue);
    meta.setAttempts(Long.parseLong(attempts));
    meta.setEntities(entities);
    meta.setDefaultRoutingKey(routingKey);
    dispose();
  }
}