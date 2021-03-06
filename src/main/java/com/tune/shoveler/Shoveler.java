package com.tune.shoveler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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
	private String blockingQueueName; // SQS queue name
	private Producer producer;
	private static BlockingQueue<String> queue;
	private int noProducer; // number of Kafka producer
	private int noConsumer; // number of SQS consumer
	private int poolSize; // number of thread in the pool

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
		Properties properties = new Properties();
		
		 // AWS credential configuration
        try {
            properties.load(new FileReader(fileProperty));  // TODO: security property file using as parameter may not be secure way
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } 
            
        this.credentials = new BasicAWSCredentials( properties.getProperty("aws_access_key_id"), properties.getProperty("aws_secret_access_key"));
        init();
	}
	
	/**
     * Shoveler constructor
     * 
     * @param noConsumer    Number of SQS consumer
     * @param noProducer    Number of Kafka producer
     * @param queueName SQS queue name
     * @param poolSize  Number of thread in the pool
     * @param access_key  aws access key
     * @param secret_key  aws secret key
     */
    public Shoveler(int noConsumer, int noProducer, String queueName, int poolSize, String access_key, String secret_key) {
        this.noConsumer = noConsumer;
        this.noProducer = noProducer;
        this.blockingQueueName = queueName;
        this.poolSize = poolSize;        
        
         // AWS credential configuration
        this.credentials = new BasicAWSCredentials( access_key, secret_key);
        init();
    }
	
	
	
	private void init(){
	    
        // modify the configuration of the s3 client
        ClientConfiguration config = new ClientConfiguration();
        config.setSocketTimeout(100000); // default is 50000
        config.setConnectionTimeout(100000); // default is 50000
        config.setMaxConnections(100); // default is 50       

        StaticCredentialsProvider credentialsProvider = new StaticCredentialsProvider( this.credentials );
        this.sqs = new AmazonSQSClient(credentialsProvider, config);
        this.sqs.setEndpoint("https://sqs.us-east-1.amazonaws.com"); // set SQS entry point

        // Setup Kafka Producer
        // TODO: should not hard code
        Properties props = new Properties();
        props.put("metadata.broker.list", "p-kafka01.use01.plat.priv:9092,p-kafka02.use01.plat.priv:9092,p-kafka03.use01.plat.priv:9092");
        props.put("producer.type", "async"); // use asynchronous mode since message comes in unordered
        props.put("compression.codec", "gzip"); // use GZip compressed format
        props.put("request.required.acks","0" );    // explicitly set as default value. producer never waits for an acknowledgement from the broke
        props.put("serializer.class", "kafka.serializer.DefaultEncoder" ); // explicitly set as default value. this takes a byte[] and returns the same byte[]
    
        this.producer = new Producer<Integer, String>(new ProducerConfig(props));	    
	}
	

	/**
	 * Run SQS consumer threads and Kafka producer threads
	 * 
	 */
	public void run() {
		this.queue = new ArrayBlockingQueue(4096); // TODO: Hard coded. Do we need this as parameter also?

		ExecutorService executor = Executors.newFixedThreadPool(poolSize);

		// Run SQS consumers
		for (int i = 0; i < noConsumer; ++i) {
			Runnable worker = new SQSConsumer(this.queue, this.sqs, blockingQueueName);
			executor.execute(worker);
		}

		// Run Kafka producers
		for (int i = 0; i < noProducer; ++i) {
			Runnable producer = new KafkaProducer(this.queue, this.producer, blockingQueueName);
			executor.execute(producer);
		}

		executor.shutdown();
		while (!executor.isTerminated()) {
		}

	}

}
