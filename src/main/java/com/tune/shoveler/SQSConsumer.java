package com.tune.shoveler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

/**
 * This class is SQS consumer class
 *
 */
public class SQSConsumer implements Runnable {

	protected BlockingQueue<String> queue = null;
	protected AmazonSQSClient sqsQueue = null;
	protected String queueName; // SQS queue name
	private final Logger logger = Logger.getLogger(getClass());

	/**
	 * Constructor for SQS consumer
	 * 
	 * @param queue	Shared blocking queue
	 * @param sqs
	 * @param queueName	SQS quene name
	 */
	public SQSConsumer(BlockingQueue<String> queue, AmazonSQSClient sqs, String queueName) {
		this.queue = queue;
		this.sqsQueue = sqs;
		this.queueName = queueName;
	}

	public void run() {
		// Gets the queue object from the name
		String queueUrl = this.sqsQueue.getQueueUrl(this.queueName).getQueueUrl();

		List<Message> sqsMessages = new ArrayList<Message>(); // temporary list to store messages in SQS
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
		while (true) {
			// Get new messages and append to shared queue

			sqsMessages.addAll(this.sqsQueue.receiveMessage(receiveMessageRequest).getMessages());
			for (Message msg : sqsMessages) {
				if(logger.isDebugEnabled()){
					logger.debug(msg.getBody() + " added to the blocking queue");
				}
				try {
					this.queue.put(msg.getBody());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				// Once messages are added in the shared blocking queue, that message in SQS is deleted.
				this.sqsQueue.deleteMessage(new DeleteMessageRequest(queueUrl, msg.getReceiptHandle()));
				// @bug: Messages can be lost if the process dies -- Need to delete on Kafka push or write to error log
			}
			sqsMessages.clear();

		}
	}
}
