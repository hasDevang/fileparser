package Shoveler;

import com.amazonaws.ClientConfiguration;

import com.amazonaws.auth.BasicAWSCredentials;

import com.amazonaws.internal.StaticCredentialsProvider;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.json.JSONObject;

public class Shoveler {
    private BasicAWSCredentials credentials;
    private AmazonSQSClient sqs;
    private String QueueName = "prod_measured_raw"; //TODO: Do not hard code
    private Producer producer;
    private static BlockingQueue<String> queue;

    public Shoveler() {
        try {
            Properties properties = new Properties();
            //modify the configuration of the s3 client
            ClientConfiguration config = new ClientConfiguration();
            config.setSocketTimeout(100000); // default is 50000
            config.setConnectionTimeout(100000);  //default is 50000
            config.setMaxConnections(1000); //default is 50

            properties.load( new FileInputStream("/home/peterb/MATDF/Shoveler/aws.properties") ); //TODO: Do not hard code
            this.credentials = new   BasicAWSCredentials(properties.getProperty("aws_access_key_id"),
                                                         properties.getProperty("aws_secret_access_key"));

            StaticCredentialsProvider credentialsProvider = new StaticCredentialsProvider(this.credentials);
            this.sqs = new AmazonSQSClient(credentialsProvider, config);
            this.sqs.setEndpoint("https://sqs.us-east-1.amazonaws.com");

            // Setup Kafka Producer
            Properties props = new Properties();
            props.put("zk.connect", "p-kafka01.use01.plat.priv:2181");
            props.put("metadata.broker.list", "p-kafka01.use01.plat.priv:9092, p-kafka92.use01.plat.priv:9092, p-kafka03.use01.plat.priv:9092");
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("producer.type", "async");
            props.put("compression.codec", "1");
            this.producer = new Producer<Integer, String>(new ProducerConfig(props));
        } catch(Exception e){
            System.out.println("Exception while createing SQSUtility : " + e);
        }
    }

    public void run() {
        this.queue = new ArrayBlockingQueue(5120);   //TODO: Hard coded to 10 full message sets + slack
        
        SQSConsumer consumer = new SQSConsumer(this.queue, this.sqs, "prod_measured_raw");
        KafkaProducer producer = new KafkaProducer(this.queue, this.producer, "prod_measured_raw");

        new Thread(consumer).start();
        new Thread(producer).start();
        
        try {
            Thread.sleep(20);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Done!");
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
                    }
                } catch (Exception e) {
                    System.out.println("Son of a nutcracker...\n" + e);
                }
            }
        }
    }

    private static class KafkaProducer implements Runnable {
        protected BlockingQueue<String> queue = null;
        protected Producer producer;
        protected String queueName;

        public KafkaProducer(BlockingQueue<String> queue, Producer producer, String queueName) {
            this.queue = queue;
            this.producer = producer;
            this.queueName = queueName;
        }
        
        public void run() {
            System.out.println("Starting Producer!");
            List<KeyedMessage<String, String>> batchedMessages = new ArrayList<KeyedMessage<String, String>>();

            // TODO: Change this to not stop the second the queue is empty
            while( true ) {
                try {
                    String json_msg = "";
                    while ( batchedMessages.size() < 500 ) {
                        json_msg = this.queue.take().toString();

                        JSONObject obj = new JSONObject(json_msg);
                        Iterator keys = obj.keys();
                        while( keys.hasNext() ) {
                            String key = (String)keys.next();
                            batchedMessages.add( new KeyedMessage<String, String>(this.queueName,obj.get(key).toString()));

                            if (batchedMessages.size() >= 500) {
                                this.producer.send(batchedMessages);
                                System.out.println("Wrote " + batchedMessages.size() + " to " + this.queueName);
                                batchedMessages.clear();
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Oh hot damm... \n" + e);
                }
            }
        }
    }
}
