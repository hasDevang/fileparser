package com.tune.shoveler;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.log4j.Logger;

/**
 * This is Kafka producer class
 *
 */
public class KafkaProducer implements Runnable {
	protected BlockingQueue<String> queue = null;
	protected Producer producer;
	protected String queueName;
	private final Logger logger = Logger.getLogger(getClass());

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
		List<KeyedMessage<String, byte[]>> batchedMessages = new ArrayList<KeyedMessage<String, byte[]>>(); // temporary list to store messages for Kafka
		long newCount = 0;
		long newTime = 0;

		long startNew = System.currentTimeMillis();
		JSONParser parser = new JSONParser(); // json parer using json-simple

		while (true) {
			// parse json text from shared blocking queue and send 250 log messages per Kafka queue

			if (batchedMessages.size() == 0) {
				startNew = System.currentTimeMillis();
			}

			JSONObject obj = null;
			Set<String> keys = null;

			try {
				obj = (JSONObject) parser.parse(this.queue.take().toString());
			} catch (ParseException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			keys = obj.keySet();

			for (String key : keys) {

				try {
					batchedMessages.add(new KeyedMessage<String, byte[]>(this.queueName, obj.get(key).toString().getBytes("utf-8")));
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}

				// Receives 250 messages from blocking queue and feed into Kafka queue
				if (batchedMessages.size() >= 250) {
					newTime += System.currentTimeMillis() - startNew;
					newCount += 1;
					logger.info("(" + batchedMessages.size() + ") - Test: "	+ newTime / newCount + " - SQSSize: " + this.queue.size());
					this.producer.send(batchedMessages);
					newCount = 0;
					newTime = 0;
					batchedMessages.clear();
				}
			}

		}

	}
}
