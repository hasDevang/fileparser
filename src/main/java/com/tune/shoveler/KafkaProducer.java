package com.tune.shoveler;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.json.simple.JSONArray;
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

    /**
     * Constructor for Kafka Producer
     * 
     * @param queue    Shared blocking queue
     * @param producer 
     * @param queueName    SQS queue name
     */
    public KafkaProducer(BlockingQueue<String> queue, Producer producer,
            String queueName) {
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

            String input = "";

            // Incoming message is ["msg1", "msg2", "msg3"]. Use JSONArray to easily handle this format

            try {
                input = this.queue.take();
                Object inObj = parser.parse(input);
                JSONArray array = (JSONArray) inObj;

                if (logger.isDebugEnabled()) {
                    logger.debug("array size : " + array.size());
                }

                for (int i = 0; i < array.size(); i++) {
                    batchedMessages.add(new KeyedMessage<String, byte[]>(this.queueName, array.get(i).toString().getBytes("utf-8")));

                    if (batchedMessages.size() >= 250) {
                        newTime += System.currentTimeMillis() - startNew;
                        newCount += 1;
                        logger.info("(" + batchedMessages.size() + ") - Test: " + newTime / newCount + " - SQSSize: " + this.queue.size());
                        this.producer.send(batchedMessages);
                        newCount = 0;
                        newTime = 0;
                        batchedMessages.clear();
                    }
                }
            } catch (ClassCastException e) {
                System.err.println("ClassCastException when parsing " + input);
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                System.err.println("ParseException when parsing " + input);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
    }
}
