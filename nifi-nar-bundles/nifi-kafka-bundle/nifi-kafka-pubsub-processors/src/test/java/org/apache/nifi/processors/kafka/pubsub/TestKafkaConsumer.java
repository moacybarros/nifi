package org.apache.nifi.processors.kafka.pubsub;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class TestKafkaConsumer {

    private final Logger logger = LoggerFactory.getLogger(TestKafkaConsumer.class);

    @Test
    public void test() throws Exception {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:6667");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(Arrays.asList("nifi-test"));
        while (true) {
//            logger.info("polling...");
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                logger.info("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
        }
    }

    @Test
    public void fetchConsumerGroupInfo() throws Exception {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:6667");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
        final String topic = "nifi-test";
        consumer.partitionsFor(topic).stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .map(tp -> consumer.committed(tp))
                .forEach(p -> logger.info("partition: {}", p));
    }

    @Test
    public void clearOffsets() throws Exception {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:6667");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
//        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "NiFi");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
        final String topic = "nifi-test";
        // Hypothesis: consumer can't clear unless it's a member of the group?
        consumer.subscribe(Collections.singletonList(topic));
        consumer.poll(1);


        final Map<TopicPartition, OffsetAndMetadata> freshOffsets = consumer.partitionsFor(topic).stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .collect(Collectors.toMap(tp -> tp, tp -> new OffsetAndMetadata(-1)));
        consumer.commitSync(freshOffsets);

    }

}
