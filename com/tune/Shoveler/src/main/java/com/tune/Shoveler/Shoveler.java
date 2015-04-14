package com.tune.Shoveler;

import com.amazonaws.ClientConfiguration;

import com.amazonaws.auth.BasicAWSCredentials;

import com.amazonaws.internal.StaticCredentialsProvider;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

//Json parser
import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.*;
//Jackson JSON parser
import org.codehaus.jackson.*;
import org.codehaus.jackson.map.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.json.JSONObject;

public class Shoveler {
    private BasicAWSCredentials credentials;
    private AmazonSQSClient sqs;
    private String BlockingQueueName = "prod_measured_raw"; //TODO: Do not hard code
    private Producer producer;
    private static BlockingQueue<String> queue;

    public Shoveler() {
        try {
            Properties properties = new Properties();
            //modify the configuration of the s3 client
            ClientConfiguration config = new ClientConfiguration();
            config.setSocketTimeout(100000); // default is 50000
            config.setConnectionTimeout(100000);  //default is 50000
            config.setMaxConnections(100); //default is 50

            properties.load( new FileInputStream("/home/peterb/Shoveler/aws.properties") ); //TODO: Do not hard code
            this.credentials = new   BasicAWSCredentials(properties.getProperty("aws_access_key_id"),
                                                         properties.getProperty("aws_secret_access_key"));

            StaticCredentialsProvider credentialsProvider = new StaticCredentialsProvider(this.credentials);
            this.sqs = new AmazonSQSClient(credentialsProvider, config);
            this.sqs.setEndpoint("https://sqs.us-east-1.amazonaws.com");

            // Setup Kafka Producer
            Properties props = new Properties();
            props.put("zk.connect", "p-kafka01.use01.plat.priv:2181");
            props.put("metadata.broker.list", "p-kafka01.use01.plat.priv:9092,p-kafka02.use01.plat.priv:9092,p-kafka03.use01.plat.priv:9092");
            props.put("producer.type", "async");
            props.put("compression.type", "gzip");
            //props.put("serializer.class", "kafka.serializer.StringEncoder");
            //props.put("requests.required.acks", 1);
            this.producer = new Producer<Integer, String>(new ProducerConfig(props));
        } catch(Exception e){
            System.out.println("Exception while creating SQSUtility : " + e);
        }
    }

    public void run() {
        this.queue = new ArrayBlockingQueue(4096); // TODO: Hard coded
    
        // TODO: Remove hard coded pool size, consumers, and queue names
        ExecutorService executor = Executors.newFixedThreadPool(50);
        for (int i = 0; i < 9; ++i ){
            Runnable worker = new SQSConsumer(this.queue, this.sqs, "prod_measured_raw");
            executor.execute(worker);
        }

        for (int i = 0; i < 1; ++i ) {
            Runnable producer = new KafkaProducer(this.queue, this.producer, "prod_measured_raw");
            executor.execute(producer);
        }

        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        
    }

    private static class SQSConsumer implements Runnable {

        protected BlockingQueue<String> queue = null;
        protected AmazonSQSClient sqsQueue = null;
        protected String queueName;

        public SQSConsumer(BlockingQueue<String> queue, AmazonSQSClient sqs, String queueName) {
            this.queue = queue;
            this.sqsQueue = sqs;
            this.queueName = queueName;
        }

        public void run() {
            // Gets the queue object from the name
            String queueUrl = this.sqsQueue.getQueueUrl(this.queueName).getQueueUrl();

            List<Message> sqsMessages = new ArrayList<Message>();
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
            while ( true ) {
                // Get new messages and append to shared queue
                try {
                    sqsMessages.addAll( this.sqsQueue.receiveMessage(receiveMessageRequest).getMessages() );
                    for ( Message msg : sqsMessages ) {
                        this.queue.put(msg.getBody());
                        this.sqsQueue.deleteMessage(new DeleteMessageRequest(queueUrl, msg.getReceiptHandle()));
                        // @bug: Messages can be lost if the process dies    -- Need to delete on Kafka push or write to error log
                    }
                    sqsMessages.clear();
                } catch (Exception e) {
                    System.out.println("ERROR: " + e);
                }
            }
        }
    }

    private static class KafkaProducer implements Runnable {
        protected BlockingQueue<String> queue = null;
        protected Producer producer;
        protected String queueName;

        private class Payload { 
            public byte[] value;
        }

        public KafkaProducer(BlockingQueue<String> queue, Producer producer, String queueName) {
            this.queue = queue;
            this.producer = producer;
            this.queueName = queueName;
        }
        
        public void run() {
            List<KeyedMessage<String, byte[]>> batchedMessages = new ArrayList<KeyedMessage<String, byte[]>>();
            long JsonCount = 0;
            long JsonTime = 0;
            long newCount = 0;
            long newTime = 0;

            long startNew = System.currentTimeMillis();
            while( true ) {
                try {
                    String json_msg = "";
                    if(  batchedMessages.size() == 0 ) {
                        startNew = System.currentTimeMillis();
                    }

                    json_msg = this.queue.take().toString();
                    
                    // Jackson Method
                    JsonFactory f = new JsonFactory();
                    JsonParser jp = f.createJsonParser(json_msg);
                    jp.nextToken();

                    while(jp.nextToken() == JsonToken.START_OBJECT) {
                        ObjectMapper mapper = new ObjectMapper();
                        Payload payload = mapper.readValue(jp, Payload.class);
                        batchedMessages.add( new KeyedMessage<String, byte[]>(this.queueName, payload.value));

                        if (batchedMessages.size() >= 250) {
                            newTime += System.currentTimeMillis() - startNew;
                            newCount += 1;
                            System.out.println("(" + batchedMessages.size() + ") - Test: " + newTime/newCount + " - SQSSize: " + this.queue.size());
                            this.producer.send(batchedMessages);

                            newCount = 0;
                            newTime = 0;
                            batchedMessages.clear();
                        }
                    }

                    // Normal Method
                    /*
                    JSONObject obj = new JSONObject(json_msg);
                    Iterator keys = obj.keys();
                    while( keys.hasNext() ) {
                        String key = (String)keys.next();
                        batchedMessages.add( new KeyedMessage<String, byte[]>(this.queueName, obj.get(key).toString().getBytes("utf-8")));

                        if (batchedMessages.size() >= 250) {
                            newTime += System.currentTimeMillis() - startNew;
                            newCount += 1;
                            System.out.println("(" + batchedMessages.size() + ") - Test: " + newTime/newCount + " - SQSSize: " + this.queue.size());
                            this.producer.send(batchedMessages);

                            newCount = 0;
                            newTime = 0;
                            batchedMessages.clear();
                        }
                    }*/
                } catch (Exception e) {
                    System.out.println("Oh hot damm... \n" + e);
                }
            }
        }
    }
}
