package com.tune.shoveler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * This class launch Shoveler using property file created from configuration injection
 * Shoveler consumes messages from SQS to feed into Kafka
 *
 */
public class ShovelerDriver {
    
    Shoveler shoveler;
    
    private static void runThroughCI() {
        JSONParser parser = new JSONParser();
        try {
            Object obj =parser.parse(new FileReader(new File("/var/has/mat-df/shoveler/shoveler_conf.json")));  // fixed location
            JSONObject jsonObj = (JSONObject) obj;
            
            Object awsPropObj = jsonObj.get("aws");
            String access_key =((JSONObject)awsPropObj).get("aws_access_key_id").toString();
            String secret_key =((JSONObject)awsPropObj).get("aws_secret_access_key").toString();
            
            String no_consumer = jsonObj.get("no_consumer").toString();
            String no_producer = jsonObj.get("no_producer").toString();
            String pool_size = jsonObj.get("pool_size").toString();
            String sqs_name = jsonObj.get("sqs_name").toString();
            
            Shoveler shoveler = new Shoveler(Integer.parseInt(no_consumer), Integer.parseInt(no_producer), sqs_name, 
                    Integer.parseInt(pool_size), access_key, secret_key);
            shoveler.run();
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
            
    }
   
    public static void main(String[] args) {        
        runThroughCI();
    }

}
