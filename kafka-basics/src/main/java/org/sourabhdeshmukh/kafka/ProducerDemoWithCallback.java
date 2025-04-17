package org.sourabhdeshmukh.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("Hello, I am Producer!");

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "0.0.0.0:19092");

        // Value Serializer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        // properties.setProperty("batch.size", "400");
        // Create The Producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        // Create a Producer Record
        for (int i=0; i<10; i++ ) {
            for (int j=0; j<30; j++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo_java", "Hello Kafka, I am producer no: " + j);

                // Send Data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes every time the record is sent or exception is thrown.
                        if (e == null) {
                            log.info("Received new metadata \nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}\n", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                        } else {
                            log.error("Error while Producing \n", e);
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // Flush and Close the Producer
        producer.close();

    }
}
