package com.tune.shoveler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class SQSConsumer implements Runnable {

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
