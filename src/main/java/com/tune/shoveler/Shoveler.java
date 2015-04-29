package com.tune.shoveler;


import java.io.File;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;

public class Shoveler {
    private BasicAWSCredentials credentials;
    private AmazonSQSClient sqs;
    private String BlockingQueueName = "prod_measured_raw"; //TODO: Do not hard code
    private Producer producer;
    private static BlockingQueue<String> queue;
    private int noProducer;
    private int noConsumer;

    public Shoveler(int noConsumer, int noProducer, File fileProperty) {
    	this.noConsumer = noConsumer;
    	this.noProducer = noProducer;
        try {
            Properties properties = new Properties();
            //modify the configuration of the s3 client
            ClientConfiguration config = new ClientConfiguration();
            config.setSocketTimeout(100000); // default is 50000
            config.setConnectionTimeout(100000);  //default is 50000
            config.setMaxConnections(100); //default is 50

            properties.load( new FileReader(fileProperty) ); //TODO: Do not hard code
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
        ExecutorService executor = Executors.newFixedThreadPool(30);
        
        for (int i = 0; i < noConsumer; ++i ){
            Runnable worker = new SQSConsumer(this.queue, this.sqs, "prod_measured_raw");
            executor.execute(worker);
        }

        for (int i = 0; i < noProducer; ++i ) {
            Runnable producer = new KafkaProducer(this.queue, this.producer, "prod_measured_raw");
            executor.execute(producer);
        }

        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        
    }

}
