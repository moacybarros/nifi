package org.apache.nifi.atlas;

import org.apache.atlas.notification.MessageDeserializer;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.function.Consumer;

public class AtlasNotificationServerEmulator {

    // EntityPartialUpdateRequest
    // EntityCreateRequest
    // NotificationInterface

    public void consume(Consumer<HookNotification.HookNotificationMessage> c) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("ATLAS_HOOK"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                final MessageDeserializer deserializer = NotificationInterface.NotificationType.HOOK.getDeserializer();
                final HookNotification.HookNotificationMessage m
                        = (HookNotification.HookNotificationMessage) deserializer.deserialize(record.value());
                c.accept(m);
            }
        }
    }
}
