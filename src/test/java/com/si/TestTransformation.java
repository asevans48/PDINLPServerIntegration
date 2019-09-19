package com.si;
/**
 * For testing transformation
 *https://programtalk.com/vs/pentaho-kettle/test/org/pentaho/di/trans/steps/javafilter/JavaFilterTest.java/
 * @author Andrew Evans
 */


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

    private static List<RowMetaAndData> getNERInputData() {
        List<RowMetaAndData> input = new ArrayList<RowMetaAndData>();
        RowMeta rm = new RowMeta();
        rm.addValueMeta( new ValueMetaString( fieldName ) );
        input.add( new RowMetaAndData( rm, new Object[]{ "Jackson Pollock was a famous artist." } ) );
        input.add( new RowMetaAndData( rm, new Object[]{ "My name is Drew." } ) );
        return input;
    }

    private static List<RowMetaAndData> getMultiNERInputData(){
        List<RowMetaAndData> input = new ArrayList<RowMetaAndData>();
        RowMeta rm = new RowMeta();
        rm.addValueMeta( new ValueMetaString( fieldName ) );
        for(int i = 0; i < 7500; i++) {
            input.add(new RowMetaAndData(rm, new Object[]{"Jackson Pollock was a famous artist."}));
            input.add(new RowMetaAndData(rm, new Object[]{"My name is Drew."}));
        }
        return input;
    }

    private static List<RowMetaAndData> getSentInputData() {
        List<RowMetaAndData> input = new ArrayList<RowMetaAndData>();
        RowMeta rm = new RowMeta();
        rm.addValueMeta( new ValueMetaString( fieldName ) );
        input.add( new RowMetaAndData( rm, new Object[]{ "Jackson Pollock was a famous artist." } ) );
        input.add( new RowMetaAndData( rm, new Object[]{ "My name is Drew. This is another sentence." } ) );
        return input;
    }

    private static List<RowMetaAndData> getMultiSentInputData() {
        List<RowMetaAndData> input = new ArrayList<RowMetaAndData>();
        RowMeta rm = new RowMeta();
        rm.addValueMeta( new ValueMetaString( fieldName ) );
        for(int i = 0; i < 7500; i++) {
            input.add(new RowMetaAndData(rm, new Object[]{"Jackson Pollock was a famous artist."}));
            input.add(new RowMetaAndData(rm, new Object[]{"My name is Drew. This is another sentence."}));
        }
        return input;
    }

    private static List<RowMetaAndData> getTopicInputData() {
        List<RowMetaAndData> input = new ArrayList<RowMetaAndData>();
        RowMeta rm = new RowMeta();
        rm.addValueMeta( new ValueMetaString( fieldName ) );
        String topicText= "Every task must have a unique name.\n" +
                "\n" +
                "    If no explicit name is provided the task decorator will generate one\n" +
                "    for you, and this name will be based on 1) the module the task is \n" +
                "    defined in, and 2) the name of the task function.\n" +
                "    +\n" +
                "    Example setting explicit name.\n" +
                "    \n" +
                "    The eager mode enabled by the task_always_eager setting is by definition\n" +
                "    not suitable for unit tests.\n" +
                "\n" +
                "    When testing with eager mode you are only testing an emulation of what\n" +
                "    happens in a worker, and there are many discrepancies between the emulation\n" +
                "    and what happens in reality.";
        input.add( new RowMetaAndData( rm, new Object[]{ topicText } ) );
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
        meta.setBatchSize(2L);
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
        List<RowMetaAndData> input = getNERInputData();
        List<RowMetaAndData> result = TransTestFactory.executeTestTransformation( transMeta,
                TransTestFactory.INJECTOR_STEPNAME, "testStep", TransTestFactory.DUMMY_STEPNAME, input );
        assert(result.size() == 2);
    }

    @Test
    public void testShouldProcessSentences() throws KettleException {
        PDINLPServerIntegrationMeta meta= this.getTestMeta();
        meta.setNerTask("SentenceTokenizer");
        TransMeta transMeta = TransTestFactory.generateTestTransformation(null, meta, "testStep");
        List<RowMetaAndData> input = getSentInputData();
        List<RowMetaAndData> result = TransTestFactory.executeTestTransformation( transMeta,
                TransTestFactory.INJECTOR_STEPNAME, "testStep", TransTestFactory.DUMMY_STEPNAME, input );
        assert(result.size() == 3);
    }

    @Test
    public void testShouldProcessTopics() throws KettleException {
        PDINLPServerIntegrationMeta meta = this.getTestMeta();
        meta.setNerTask("TextTilingTokenizer");
        TransMeta transMeta = TransTestFactory.generateTestTransformation(null, meta, "testStep");
        List<RowMetaAndData> input = getTopicInputData();
        List<RowMetaAndData> result = TransTestFactory.executeTestTransformation( transMeta,
                TransTestFactory.INJECTOR_STEPNAME, "testStep", TransTestFactory.DUMMY_STEPNAME, input );
        assert(result.size() >= 1);
    }

    @Test
    public void testShouldNotFailIfTopicsAreTooShort() throws KettleException {
        PDINLPServerIntegrationMeta meta = this.getTestMeta();
        meta.setNerTask("TextTilingTokenizer");
        TransMeta transMeta = TransTestFactory.generateTestTransformation(null, meta, "testStep");
        List<RowMetaAndData> input = getNERInputData();
        List<RowMetaAndData> result = TransTestFactory.executeTestTransformation( transMeta,
                TransTestFactory.INJECTOR_STEPNAME, "testStep", TransTestFactory.DUMMY_STEPNAME, input );
        assert(result.size() == 2);
    }

    @Test
    public void testShouldSentTokenizeManyRows() throws KettleException {
        PDINLPServerIntegrationMeta meta= this.getTestMeta();
        meta.setNerTask("SentenceTokenizer");
        meta.setBatchSize(100L);
        TransMeta transMeta = TransTestFactory.generateTestTransformation(null, meta, "testStep");
         List<RowMetaAndData> input = getMultiSentInputData();
         List<RowMetaAndData> result = TransTestFactory.executeTestTransformation( transMeta,
                 TransTestFactory.INJECTOR_STEPNAME, "testStep", TransTestFactory.DUMMY_STEPNAME, input );
         assert(result.size() >= 15000);
    }

    @Test
    public void testShouldNERManyRows() throws KettleException {
        PDINLPServerIntegrationMeta meta= this.getTestMeta();
        meta.setNerTask("NERTask");
        meta.setBatchSize(100L);
        meta.setEntities("PERSON");
        TransMeta transMeta = TransTestFactory.generateTestTransformation(null, meta, "testStep");
        List<RowMetaAndData> input = getMultiNERInputData();
        List<RowMetaAndData> result = TransTestFactory.executeTestTransformation( transMeta,
                TransTestFactory.INJECTOR_STEPNAME, "testStep", TransTestFactory.DUMMY_STEPNAME, input );
        assert(result.size() >= 15000);
    }
}
