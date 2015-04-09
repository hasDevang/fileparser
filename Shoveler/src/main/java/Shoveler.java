package Shoveler;

import com.amazonaws.ClientConfiguration;

import com.amazonaws.auth.BasicAWSCredentials;

import com.amazonaws.internal.StaticCredentialsProvider;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

import java.io.FileInputStream;
import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.json.JSONObject;

public class Shoveler {
    private BasicAWSCredentials credentials;
    private AmazonSQS sqs;
    private String QueueName = "prod_measured_raw"; //TODO: Do not hard code

    public Shoveler() {
        try {
            Properties properties = new Properties();
            //modify the configuration of the s3 client
            ClientConfiguration config = new ClientConfiguration();
            config.setSocketTimeout(100000); // default is 50000
            config.setConnectionTimeout(100000);  //default is 50000
            config.setMaxConnections(100); //default is 50

            properties.load( new FileInputStream("/home/peterb/MATDF/Shoveler/aws.properties") ); //TODO: Do not hard code
            this.credentials = new   BasicAWSCredentials(properties.getProperty("aws_access_key_id"),
                                                         properties.getProperty("aws_secret_access_key"));

            StaticCredentialsProvider credentialsProvider = new StaticCredentialsProvider(this.credentials);
            this.sqs = new AmazonSQSClient(credentialsProvider, config);
            this.sqs.setEndpoint("https://sqs.us-east-1.amazonaws.com");
        } catch(Exception e){
            System.out.println("Exception while createing SQSUtility : " + e);
        }
    }

    // TODO: Comments
    private List<Message> getMessagesFromSQS(String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        return messages;
    }

    public void run() {
        // Setup Kafka Producer
        Properties props = new Properties();
        props.put("zk.connect", "p-kafka01.use01.plat.priv:2181");
        props.put("metadata.broker.list", "p-kafka01.use01.plat.priv:9092, p-kafka92.use01.plat.priv:9092, p-kafka03.use01.plat.priv:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "async");
        props.put("compression.codec", "1");

        List<KeyedMessage<String, String>> batchedMessages = new ArrayList<KeyedMessage<String, String>>();
        // Gets the queue object from the name
        String queueUrl = this.sqs.getQueueUrl(this.QueueName).getQueueUrl();

        Producer producer = new Producer<Integer, String>(new ProducerConfig(props));

        while( true ) {
            List<Message> sqsMessages = new ArrayList<Message>();
            
            while (sqsMessages.size() < 100) {
                System.out.println("Appended " + sqsMessages.size() + " messages");
                sqsMessages.addAll( this.getMessagesFromSQS(queueUrl) );
            }

            for( Message msg : sqsMessages ) {
                try {
                    JSONObject obj = new JSONObject(msg.getBody());
                    Iterator keys = obj.keys();
                    while( keys.hasNext() ) {
                        String key = (String)keys.next();
                        batchedMessages.add( new KeyedMessage<String, String>(this.QueueName,obj.get(key).toString()));

                        if (batchedMessages.size() > 500) {
                            producer.send(batchedMessages);
                            System.out.println("Wrote " + batchedMessages.size() + " to " + this.QueueName + " hopefully...");
                            batchedMessages.clear();
                        }
                    }

                } catch (Exception e) {
                    System.out.println("Oh hot damm... \n" + e);
                }
            }
        }
    }
}
