package ar.com.carloscurotto.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * <pre>
 * This is a very basic kafka example.  
 * This example requires to start a zookeeper node and a kafka server before running. To do this execute the following
 * commands in the root kafka directory:
 * 
 * 1- bin/zookeeper-server-start.sh config/zookeeper.properties (starts a zookeeper node)
 * 2- bin/kafka-server-start.sh config/server.properties (starts a kafka server)
 * </pre>
 * 
 * @author carloscurotto
 */
public class KafkaExample {

    public static void main(String[] args) throws Exception {
        
        // Quantity of messages
        final int quantityOfMessages = 10;
        
        // Creates a producer configuration
        Properties producerProperties = new Properties();
        producerProperties.put("serializer.class", "kafka.serializer.StringEncoder");
        producerProperties.put("metadata.broker.list", "localhost:9092");
        ProducerConfig producerConfiguration = new ProducerConfig(producerProperties);
        
        // Creates a producer
        Producer<String, String> producer = new Producer<String, String>(producerConfiguration);

        // Sends a message
        for (int i = 1; i <= quantityOfMessages; i++) {
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("test-topic", "test-message-" + i);
            producer.send(data);
        }
        
        // Creates a consumer configuration
        Properties consumerProperties = new Properties();
        consumerProperties.put("zookeeper.connect", "localhost:2181");
        consumerProperties.put("group.id", "group1");
        consumerProperties.put("zookeeper.session.timeout.ms", "40000");
        consumerProperties.put("zookeeper.sync.time.ms", "20000");
        consumerProperties.put("auto.commit.interval.ms", "100000");
        
        // Creates consumer connector
        ConsumerConfig consumerConfiguration = new ConsumerConfig(consumerProperties);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfiguration);
        
        // Creates consumer stream
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put("test-topic", new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                consumerConnector.createMessageStreams(topicCountMap);
        
        // Gets all the created streams
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("test-topic");
        
        // Prints messages for each stream asynchronously
        ExecutorService executor = Executors.newFixedThreadPool(streams.size());
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new Runnable() {
                public void run() {
                    for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {
                        System.out.println(new String(msgAndMetadata.message()));
                    }
                }
            });
        }        
        
        // Sleeps for a while to complete the printing
        Thread.sleep(5000);
        
        // Closes the consumer
        consumerConnector.shutdown();
        
        // Closes the executor
        executor.shutdownNow();
        
        // Closes the producer
        producer.close();

    }
}
