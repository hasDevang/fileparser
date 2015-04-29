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

/**
 * This class provides shoveling messages from SQS to feed into Kafka
 *
 */
public class Shoveler {
    private BasicAWSCredentials credentials;
    private AmazonSQSClient sqs;
    private String blockingQueueName;	// SQS queue name
    private Producer producer;
    private static BlockingQueue<String> queue;
    private int noProducer;	// number of Kafka producer
    private int noConsumer;	// number of SQS consumer
    private int poolSize;	// number of thread in the pool

    /**
     * Shoveler constructor
     * 
     * @param noConsumer	Number of SQS consumer
     * @param noProducer	Number of Kafka producer
     * @param fileProperty	AWS properties file containing key pairs
     * @param queueName	SQS queue name
     * @param poolSize	Number of thread in the pool
     */
    public Shoveler(int noConsumer, int noProducer, File fileProperty, String queueName, int poolSize) {
    	this.noConsumer = noConsumer;
    	this.noProducer = noProducer;
    	this.blockingQueueName = queueName;
    	this.poolSize = poolSize;
    	
        try {
            Properties properties = new Properties();
            // modify the configuration of the s3 client
            ClientConfiguration config = new ClientConfiguration();
            config.setSocketTimeout(100000); // default is 50000
            config.setConnectionTimeout(100000);  //default is 50000
            config.setMaxConnections(100); //default is 50

            // AWS credential configuration
            properties.load( new FileReader(fileProperty) ); //TODO: security property file using as parameter may not be secure way
            this.credentials = new   BasicAWSCredentials(properties.getProperty("aws_access_key_id"),
                                                         properties.getProperty("aws_secret_access_key"));

            StaticCredentialsProvider credentialsProvider = new StaticCredentialsProvider(this.credentials);
            this.sqs = new AmazonSQSClient(credentialsProvider, config);
            this.sqs.setEndpoint("https://sqs.us-east-1.amazonaws.com");	// set SQS entry point

            // Setup Kafka Producer
            // TODO: should not hard code
            Properties props = new Properties();
            props.put("zk.connect", "p-kafka01.use01.plat.priv:2181");
            props.put("metadata.broker.list", "p-kafka01.use01.plat.priv:9092,p-kafka02.use01.plat.priv:9092,p-kafka03.use01.plat.priv:9092");
            props.put("producer.type", "async");	// use asynchronous mode since message comes in unordered
            props.put("compression.type", "gzip");	// use GZip compressed format
            //props.put("serializer.class", "kafka.serializer.StringEncoder");
            //props.put("requests.required.acks", 1);
            this.producer = new Producer<Integer, String>(new ProducerConfig(props));
        } catch(Exception e){
            System.out.println("Exception while creating SQSUtility : " + e);
        }
    }

    /**
     * Run SQS consumer threads and Kafka producer threads
     * 
     */
    public void run() {
        this.queue = new ArrayBlockingQueue(4096); // TODO: Hard coded. Do we need this as parameter also?
    
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);
        
        // Run SQS consumers
        for (int i = 0; i < noConsumer; ++i ){
            Runnable worker = new SQSConsumer(this.queue, this.sqs, blockingQueueName);
            executor.execute(worker);
        }

        // Run Kafka producers
        for (int i = 0; i < noProducer; ++i ) {
            Runnable producer = new KafkaProducer(this.queue, this.producer, blockingQueueName);
            executor.execute(producer);
        }

        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        
    }

}
