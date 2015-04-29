package com.tune.shoveler;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class KafkaProducer implements Runnable {
    protected BlockingQueue<String> queue = null;
    protected Producer producer;
    protected String queueName;

    private class Payload { 
        public byte[] value;
    }

    /**
     * Constructor for Kafka Producer
     * 
     * @param queue	Shared blocking queue
     * @param producer
     * @param queueName	SQS queue name
     */
    public KafkaProducer(BlockingQueue<String> queue, Producer producer, String queueName) {
        this.queue = queue;
        this.producer = producer;
        this.queueName = queueName;
    }
     
    public void run() {
        List<KeyedMessage<String, byte[]>> batchedMessages = new ArrayList<KeyedMessage<String, byte[]>>();	// temporary list to store messages for Kafka
//        long JsonCount = 0;
//        long JsonTime = 0;
        long newCount = 0;
        long newTime = 0;

        long startNew = System.currentTimeMillis();
        JSONParser parser = new JSONParser();	// json parer using json simple
        
		while (true) {
			// parse json text from shared blocking queue and send 250 log messages per Kafka queue 
			try {
//				String json_msg = "";
				if (batchedMessages.size() == 0) {
					startNew = System.currentTimeMillis();
				}

				JSONObject obj = (JSONObject) parser.parse(this.queue.take().toString());

				Set<String> keys = obj.keySet();
				for (String key : keys) {
					
					batchedMessages.add(new KeyedMessage<String, byte[]>(this.queueName, obj.get(key).toString().getBytes("utf-8")));
					// Recieves 250 messages from blocking queue and feed into Kafka queue
					if (batchedMessages.size() >= 250) {
						newTime += System.currentTimeMillis() - startNew;
						newCount += 1;
						System.out.println("(" + batchedMessages.size()	+ ") - Test: " + newTime / newCount + " - SQSSize: " + this.queue.size());
						this.producer.send(batchedMessages);
						newCount = 0;
						newTime = 0;
						batchedMessages.clear();
					}
				}
			} catch (Exception e) {
				System.out.println("Oh hot damm... \n" + e);
			}
		}
    
    }
}
