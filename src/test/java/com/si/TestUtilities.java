/**
 * Tests integration with celery-java. Specifict tests are on celery-java.
 *
 * Must have a running instance of NLP Server configured for tests to work. Intentionally not
 * working here.
 *
 */
package com.si;

import com.si.celery.task.result.AsyncResult;
import com.si.utilities.Utilities;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.junit.Test;

import javax.rmi.CORBA.Util;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TestUtilities {

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

    /**
     * Obtain a setup version of the utilities
     * @return  The setup utilities
     */
    private Utilities getNonDefaultUtilities(){
        Utilities utilities = new Utilities();
        String entities = "PERSON;ORGANIZATION";
        PDINLPServerIntegrationMeta meta = new PDINLPServerIntegrationMeta();
        meta.setDefault();
        meta.setDefaultRoutingKey("celery");
        meta.setDefaultQueue("celerytest");
        meta.setHardTimeLimit(180000L);
        meta.setTimeLimit(180000L);
        meta.setEntities(entities);
        meta.setBackendURI(this.getBackendURI());
        meta.setBrokerURI(this.getBrokerURI());
        PDINLPServerIntegrationData data = new PDINLPServerIntegrationData();
        utilities.setMeta(meta);
        utilities.setData(data);
        return utilities;
    }

    /**
     * Obtain a setup version of the utilities
     * @return  The setup utilities
     */
    private Utilities getUtilities(){
        Utilities utilities = new Utilities();
        String entities = "PERSON;ORGANIZATION";
        PDINLPServerIntegrationMeta meta = new PDINLPServerIntegrationMeta();
        meta.setDefault();
        meta.setDefaultRoutingKey("celery");
        meta.setDefaultQueue("celery");
        meta.setHardTimeLimit(180000L);
        meta.setTimeLimit(180000L);
        meta.setEntities(entities);
        meta.setBackendURI(this.getBackendURI());
        meta.setBrokerURI(this.getBrokerURI());
        PDINLPServerIntegrationData data = new PDINLPServerIntegrationData();
        utilities.setMeta(meta);
        utilities.setData(data);
        return utilities;
    }

    @Test
    public void testShouldCreateAConfiguration(){
        Utilities utilities = this.getUtilities();
        utilities.setCelery();
    }

    @Test
    public void testShouldCreateArgs(){
        Utilities utilities = this.getUtilities();
        utilities.setCelery();
        String[][] testStrings = new String[][]{ new String[]{"Hello my dear friend"}, new String[]{"Goodbye to anger"}, new String[]{"Testing a third one"}};
        Object[] args = utilities.createArgs(testStrings, 0);
        assert(args.length == 2);
        Object[] argsArray = (Object[]) args[0];
        assert(testStrings.length == argsArray.length);
        for(int i = 0; i < argsArray.length; i++){
            assert(((String)argsArray[i]).equals(testStrings[i][0]));
        }
    }

    @Test
    public void testShouldSendTask() throws CloneNotSupportedException, ExecutionException, InterruptedException {
        Utilities utilities = this.getUtilities();
        utilities.setCelery();
        Object[] args = new Object[]{"My name is slim Shady and all you other slim shadys can.", new String[]{"PERSON"}};
        Future<AsyncResult> fut = utilities.sendTask("NERTask", args, null);
        fut.get();

    }

    @Test
    public void testShouldPackageResult() throws ExecutionException, InterruptedException, CloneNotSupportedException, ParseException {
        Utilities utilities = this.getUtilities();
        utilities.setCelery();
        String[][] testStrings = new String[][]{ new String[]{"My name is slim Shady and all you other slim shadys can."}, new String[]{"My name is slim Shady and all you other slim shadys can."}, new String[]{"My name is slim Shady and all you other slim shadys can."}};
        Object[] args = utilities.createArgs(testStrings, 0);
        Future<AsyncResult> fut = utilities.sendTask("NERTask", args, null);
        AsyncResult result = fut.get();
        utilities.close();
        result.parsePayloadJson();
        assert(result.isSuccess());
        Object results = result.getResult();
        JSONArray resArray = (JSONArray) results;
        for(int i = 0; i < resArray.size(); i++){
            JSONObject job = (JSONObject) resArray.get(i);
            JSONArray jarr = (JSONArray) job.get("people");
            assert(jarr.size() == 1);
            jarr = (JSONArray) job.get("organizations");
            assert(jarr.size() == 0);
        }
    }

    @Test
    public void shouldUseANonDefaultQueue() throws ParseException, CloneNotSupportedException, ExecutionException, InterruptedException {
        Utilities utilities = this.getNonDefaultUtilities();
        utilities.setCelery();
        String[][] testStrings = new String[][]{ new String[]{"My name is slim Shady and all you other slim shadys can."}, new String[]{"My name is slim Shady and all you other slim shadys can."}, new String[]{"My name is slim Shady and all you other slim shadys can."}};
        Object[] args = utilities.createArgs(testStrings, 0);
        Future<AsyncResult> fut = utilities.sendTask("NERTask", args, null);
        AsyncResult result = fut.get();
        utilities.close();
        result.parsePayloadJson();
        assert(result.isSuccess());
        Object results = result.getResult();
        JSONArray resArray = (JSONArray) results;
        for(int i = 0; i < resArray.size(); i++){
            JSONObject job = (JSONObject) resArray.get(i);
            JSONArray jarr = (JSONArray) job.get("people");
            assert(jarr.size() == 1);
            jarr = (JSONArray) job.get("organizations");
            assert(jarr.size() == 0);
        }
    }
}
