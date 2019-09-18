package com.si;
/**
 * For testing transformation
 *https://programtalk.com/vs/pentaho-kettle/test/org/pentaho/di/trans/steps/javafilter/JavaFilterTest.java/
 * @author Andrew Evans
 */


import com.si.celery.conf.Config;
import com.si.celery.enums.BackendType;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.trans.TransMeta;

import java.util.ArrayList;
import java.util.List;


/**
 * Test transformation
 */
public class TestTransformation {

    /**
     * Get a backend URI
     *
     * @return  backend uri
     */
    private String getBackendURI(){
        return System.getenv().get("CELERY_BACKEND_URI");
    }

    /**
     * Get the broker uri
     *
     * @return  the broker uri
     */
    private String getBrokerURI(){
        return System.getenv().get("CELERY_BROKER_URI");
    }

    private static final String fieldName = "testString";

    @BeforeClass
    public static void before() throws KettleException {
        KettleEnvironment.init();
    }

    private static List<RowMetaAndData> getInputData() {
        List<RowMetaAndData> input = new ArrayList<RowMetaAndData>();
        RowMeta rm = new RowMeta();
        rm.addValueMeta( new ValueMetaString( fieldName ) );
        input.add( new RowMetaAndData( rm, new Object[]{ "My name is Slim Shady" } ) );
        input.add( new RowMetaAndData( rm, new Object[]{ "My name is Drew" } ) );
        return input;
    }

    private PDINLPServerIntegrationMeta getTestMeta(){
        PDINLPServerIntegrationMeta meta= new PDINLPServerIntegrationMeta();
        meta.setTimeLimit(180000L);
        meta.setHardTimeLimit(180000L);
        meta.setBrokerURI(this.getBrokerURI());
        meta.setBackendURI(this.getBackendURI());
        meta.setDefaultRoutingKey("celery");
        meta.setDefaultQueue("celery");
        meta.setBatchSize(1L);
        meta.setInField("testString");
        meta.setOutField("testOut");
        return meta;
    }

    @Test
    public void testShouldProcessNERTASK() throws KettleException {
        PDINLPServerIntegrationMeta meta= this.getTestMeta();
        meta.setNerTask("NERTask");
        meta.setEntities("PERSON");
        TransMeta transMeta = TransTestFactory.generateTestTransformation(null, meta, "testStep");
        List<RowMetaAndData> input = getInputData();
        List<RowMetaAndData> result = TransTestFactory.executeTestTransformation( transMeta,
                TransTestFactory.INJECTOR_STEPNAME, "testStep", TransTestFactory.DUMMY_STEPNAME, input );
    }

    @Test
    public void testShouldProcessSentences() throws KettleException {
        PDINLPServerIntegrationMeta meta= this.getTestMeta();
        meta.setNerTask("SentenceTokenizer");
        TransMeta transMeta = TransTestFactory.generateTestTransformation(null, meta, "testStep");
        List<RowMetaAndData> input = getInputData();
        List<RowMetaAndData> result = TransTestFactory.executeTestTransformation( transMeta,
                TransTestFactory.INJECTOR_STEPNAME, "testStep", TransTestFactory.DUMMY_STEPNAME, input );
    }

    @Test
    public void testShouldProcessTopics() throws KettleException {
        PDINLPServerIntegrationMeta meta = this.getTestMeta();
        meta.setNerTask("TextTilingTokenizer");
        TransMeta transMeta = TransTestFactory.generateTestTransformation(null, meta, "testStep");
        List<RowMetaAndData> input = getInputData();
        List<RowMetaAndData> result = TransTestFactory.executeTestTransformation( transMeta,
                TransTestFactory.INJECTOR_STEPNAME, "testStep", TransTestFactory.DUMMY_STEPNAME, input );
    }

    @Test
    public void testShouldProcessRows(){

    }
}
