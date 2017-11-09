/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.azure.eventhub;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.CloseReason;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.IEventProcessorFactory;
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.AzureConstants;
import org.apache.nifi.util.StopWatch;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.util.StringUtils.isEmpty;

// TODO: Support Demarcater.
// TODO: Support Record writer.
@Tags({"azure", "microsoft", "cloud", "eventhub", "events", "streaming", "streams"})
@CapabilityDescription("Receives messages from a Microsoft Azure Event Hub, writing the contents of the Azure message to the content of the FlowFile.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@WritesAttributes({
        @WritesAttribute(attribute = "eventhub.enqueued.timestamp", description = "The time (in milliseconds since epoch, UTC) at which the message was enqueued in the Azure Event Hub"),
        @WritesAttribute(attribute = "eventhub.offset", description = "The offset into the partition at which the message was stored"),
        @WritesAttribute(attribute = "eventhub.sequence", description = "The Azure Sequence number associated with the message"),
        @WritesAttribute(attribute = "eventhub.name", description = "The name of the Event Hub from which the message was pulled"),
        @WritesAttribute(attribute = "eventhub.partition", description = "The name of the Azure Partition from which the message was pulled")
})
public class ConsumeAzureEventHub extends AbstractSessionFactoryProcessor {

    private volatile EventProcessorHost eventProcessorHost;
    private volatile ProcessSessionFactory processSessionFactory;
    // The namespace name can not be retrieved from a PartitionContext at EventProcessor.onEvents, so keep it here.
    private volatile String namespaceName;

    static final PropertyDescriptor NAMESPACE = new PropertyDescriptor.Builder()
            .name("event-hub-namespace")
            .displayName("Event Hub Namespace")
            .description("The Azure Namespace that the Event Hub is assigned to. This is generally equal to <Event Hub Name>-ns.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(true)
            .build();
    static final PropertyDescriptor EVENT_HUB_NAME = new PropertyDescriptor.Builder()
            .name("event-hub-name")
            .displayName("Event Hub Name")
            .description("The name of the Azure Event Hub to pull messages from.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(true)
            .build();
    // TODO: Do we need to support custom service endpoints as GetAzureEventHub does? Is it possible?
    static final PropertyDescriptor ACCESS_POLICY_NAME = new PropertyDescriptor.Builder()
            .name("event-hub-shared-access-policy-name")
            .displayName("Shared Access Policy Name")
            .description("The name of the Event Hub Shared Access Policy. This Policy must have Listen permissions.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(true)
            .build();
    static final PropertyDescriptor POLICY_PRIMARY_KEY = new PropertyDescriptor.Builder()
            .name("event-hub-shared-access-policy-primary-key")
            .displayName("Shared Access Policy Primary Key")
            .description("The primary key of the Event Hub Shared Access Policy.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .sensitive(true)
            .required(true)
            .build();
    static final PropertyDescriptor CONSUMER_GROUP = new PropertyDescriptor.Builder()
            .name("event-hub-consumer-group")
            .displayName("Event Hub Consumer Group")
            .description("The name of the Event Hub Consumer Group to use.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("$Default")
            .required(true)
            .build();
    static final PropertyDescriptor CONSUMER_HOSTNAME = new PropertyDescriptor.Builder()
            .name("event-hub-consumer-hostname")
            .displayName("Event Hub Consumer Hostname")
            .description("The hostname of this Event Hub Consumer instance." +
                    " If not specified, an unique identifier is generated in 'nifi-<UUID>' format.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(false)
            .build();
    static final AllowableValue INITIAL_OFFSET_START_OF_STREAM = new AllowableValue(
            "start-of-stream", "Start of stream", "Read from the oldest message retained in the stream.");
    static final AllowableValue INITIAL_OFFSET_END_OF_STREAM = new AllowableValue(
            "end-of-stream", "End of stream",
            "Ignore old retained messages even if exist, start reading new ones from now.");
    static final PropertyDescriptor INITIAL_OFFSET = new PropertyDescriptor.Builder()
            .name("event-hub-initial-offset")
            .displayName("Initial Offset")
            .description("Specify where to start receiving messages if offset is not stored in Azure Storage.")
            .required(true)
            .allowableValues(INITIAL_OFFSET_START_OF_STREAM, INITIAL_OFFSET_END_OF_STREAM)
            .defaultValue(INITIAL_OFFSET_END_OF_STREAM.getValue())
            .build();
    static final PropertyDescriptor PREFETCH_COUNT = new PropertyDescriptor.Builder()
            .name("event-hub-prefetch-count")
            .displayName("Prefetch Count")
            .defaultValue("The number of messages to fetch from Event Hub before processing." +
                    " This parameter affects throughput." +
                    " The more prefetch count, the better throughput in general, but consumes more resources (RAM).")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("300")
            .expressionLanguageSupported(true)
            .required(true)
            .build();
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("event-hub-batch-size")
            .displayName("Batch Size")
            .description("The number of messages to process within a NiFi session." +
                    " This parameter affects throughput and consistency." +
                    " NiFi commits its session and Event Hub checkpoint after processing this number of messages." +
                    " If NiFi session is committed, but failed to create an Event Hub checkpoint," +
                    " then it is possible that the same messages to be retrieved again." +
                    " The higher number, the higher throughput, but possibly less consistent.")
            .defaultValue("10")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(true)
            .build();
    static final PropertyDescriptor RECEIVE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("event-hub-message-receive-timeout")
            .displayName("Message Receive Timeout")
            .description("The amount of time this consumer should wait to receive the Prefetch Count before returning.")
            .defaultValue("1 min")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(true)
            .build();
    static final PropertyDescriptor STORAGE_ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .name("storage-account-name")
            .displayName("Storage Account Name")
            .description("Name of the Azure Storage account to store Event Hub Consumer Group state.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(true)
            .build();
    static final PropertyDescriptor STORAGE_ACCOUNT_KEY = new PropertyDescriptor.Builder()
            .name("storage-account-key")
            .displayName("Storage Account Key")
            .description("The Azure Storage account key to store Event Hub Consumer Group state.")
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(true)
            .build();
    static final PropertyDescriptor STORAGE_CONTAINER_NAME = new PropertyDescriptor.Builder()
            .name("storage-container-name")
            .displayName("Storage Container Name")
            .description("Name of the Azure Storage Container to store Event Hub Consumer Group state." +
                    " If not specified, Event Hub name is used.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(false)
            .build();


    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles received from Event Hub.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Collections.singleton(REL_SUCCESS);
    private static final List<PropertyDescriptor> PROPERTIES;

    static {
        PROPERTIES = Collections.unmodifiableList(Arrays.asList(
                NAMESPACE, EVENT_HUB_NAME, ACCESS_POLICY_NAME, POLICY_PRIMARY_KEY, CONSUMER_GROUP, CONSUMER_HOSTNAME,
                INITIAL_OFFSET, PREFETCH_COUNT, BATCH_SIZE, RECEIVE_TIMEOUT,
                STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, STORAGE_CONTAINER_NAME
        ));
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    public class EventProcessorFactory implements IEventProcessorFactory {
        @Override
        public IEventProcessor createEventProcessor(PartitionContext context) throws Exception {
            final EventProcessor eventProcessor = new EventProcessor();
            return eventProcessor;
        }
    }

    public class EventProcessor implements IEventProcessor {

        @Override
        public void onOpen(PartitionContext context) throws Exception {
            getLogger().info("Consumer group {} opened partition {} of {}",
                    new Object[]{context.getConsumerGroupName(), context.getPartitionId(), context.getEventHubPath()});
        }

        @Override
        public void onClose(PartitionContext context, CloseReason reason) throws Exception {
            getLogger().info("Consumer group {} closed partition {} of {}. reason={}",
                    new Object[]{context.getConsumerGroupName(), context.getPartitionId(), context.getEventHubPath(), reason});
        }

        @Override
        public void onEvents(PartitionContext context, Iterable<EventData> messages) throws Exception {
            final ProcessSession session = processSessionFactory.createSession();

            try {
                final String eventHubName = context.getEventHubPath();
                final String partitionId = context.getPartitionId();
                final String consumerGroup = context.getConsumerGroupName();

                final StopWatch stopWatch = new StopWatch(true);
                messages.forEach(eventData -> {
                    // TODO: Share logic with GetAzureEventHub.
                    final Map<String, String> attributes = new HashMap<>();
                    FlowFile flowFile = session.create();
                    final EventData.SystemProperties systemProperties = eventData.getSystemProperties();

                    if (null != systemProperties) {
                        attributes.put("eventhub.enqueued.timestamp", String.valueOf(systemProperties.getEnqueuedTime()));
                        attributes.put("eventhub.offset", systemProperties.getOffset());
                        attributes.put("eventhub.sequence", String.valueOf(systemProperties.getSequenceNumber()));
                    }

                    attributes.put("eventhub.name", eventHubName);
                    attributes.put("eventhub.partition", partitionId);


                    flowFile = session.putAllAttributes(flowFile, attributes);
                    flowFile = session.write(flowFile, out -> {
                        out.write(eventData.getBytes());
                    });

                    session.transfer(flowFile, REL_SUCCESS);
                    final String transitUri = "amqps://" + namespaceName + ".servicebus.windows.net/" + eventHubName + "/ConsumerGroups/" + consumerGroup + "/Partitions/" + partitionId;
                    session.getProvenanceReporter().receive(flowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                });

                // Commit NiFi first.
                session.commit();
                // If creating an Event Hub checkpoint failed, then the same message can be retrieved again.
                context.checkpoint();

            } catch (Exception e) {
                getLogger().error("Unable to fully process received message due to " + e, e);
                session.rollback();
            }

        }

        @Override
        public void onError(PartitionContext context, Throwable e) {
            getLogger().error("An error occurred while receiving messages from Azure Event hub {} at partition {}," +
                            " consumerGroupName={}, exception={}",
                    new Object[]{context.getEventHubPath(), context.getPartitionId(), context.getConsumerGroupName(), e}, e);
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {

        if (eventProcessorHost == null) {
            try {
                registerEventProcessor(context);
            } catch (IllegalArgumentException e) {
                // In order to show simple error message without wrapping it by another ProcessException, just throw it as it is.
                throw e;
            } catch (Exception e) {
                throw new ProcessException("Failed to register the event processor due to " + e, e);
            }
            processSessionFactory = sessionFactory;
        }

        // After a EventProcessor is registered successfully, nothing has to be done at onTrigger
        // because new sessions are created when new messages are arrived by the EventProcessor.
        context.yield();
    }

    @OnStopped
    public void unregisterEventProcessor(final ProcessContext context) {
        if (eventProcessorHost != null) {
            try {
                eventProcessorHost.unregisterEventProcessor();
                eventProcessorHost = null;
                processSessionFactory = null;
            } catch (Exception e) {
                throw new RuntimeException("Failed to unregister the event processor due to " + e, e);
            }
        }
    }

    private void registerEventProcessor(final ProcessContext context) throws Exception {
        // Validate required properties.
        final String consumerGroupName = context.getProperty(CONSUMER_GROUP).evaluateAttributeExpressions().getValue();
        validateRequiredProperty(CONSUMER_GROUP, consumerGroupName);

        namespaceName = context.getProperty(NAMESPACE).evaluateAttributeExpressions().getValue();
        validateRequiredProperty(NAMESPACE, namespaceName);

        final String eventHubName = context.getProperty(EVENT_HUB_NAME).evaluateAttributeExpressions().getValue();
        validateRequiredProperty(EVENT_HUB_NAME, eventHubName);

        final String sasName = context.getProperty(ACCESS_POLICY_NAME).evaluateAttributeExpressions().getValue();
        validateRequiredProperty(ACCESS_POLICY_NAME, sasName);

        final String sasKey = context.getProperty(POLICY_PRIMARY_KEY).evaluateAttributeExpressions().getValue();
        validateRequiredProperty(POLICY_PRIMARY_KEY, sasKey);

        final String storageAccountName = context.getProperty(STORAGE_ACCOUNT_NAME).evaluateAttributeExpressions().getValue();
        validateRequiredProperty(STORAGE_ACCOUNT_NAME, storageAccountName);

        final String storageAccountKey = context.getProperty(STORAGE_ACCOUNT_KEY).evaluateAttributeExpressions().getValue();
        validateRequiredProperty(STORAGE_ACCOUNT_KEY, storageAccountKey);


        final String consumerHostname = orDefault(context.getProperty(CONSUMER_HOSTNAME).evaluateAttributeExpressions().getValue(),
                EventProcessorHost.createHostName("nifi"));

        final String containerName = orDefault(context.getProperty(STORAGE_CONTAINER_NAME).evaluateAttributeExpressions().getValue(),
                eventHubName);


        final EventProcessorOptions options = new EventProcessorOptions();
        final String initialOffset = context.getProperty(INITIAL_OFFSET).getValue();
        if (INITIAL_OFFSET_START_OF_STREAM.getValue().equals(initialOffset)) {
            options.setInitialOffsetProvider(options.new StartOfStreamInitialOffsetProvider());
        } else if (INITIAL_OFFSET_END_OF_STREAM.getValue().equals(initialOffset)){
            options.setInitialOffsetProvider(options.new EndOfStreamInitialOffsetProvider());
        } else {
            throw new IllegalArgumentException("Initial offset " + initialOffset + " is not allowed.");
        }

        final Integer prefetchCount = context.getProperty(PREFETCH_COUNT).evaluateAttributeExpressions().asInteger();
        if (prefetchCount != null && prefetchCount > 0) {
            options.setPrefetchCount(prefetchCount);
        }

        final Integer batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        if (batchSize != null && batchSize > 0) {
            options.setMaxBatchSize(batchSize);
        }

        final Long receiveTimeoutMillis = context.getProperty(RECEIVE_TIMEOUT)
                .evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        options.setReceiveTimeOut(Duration.ofMillis(receiveTimeoutMillis));

        final String storageConnectionString = String.format(AzureConstants.FORMAT_DEFAULT_CONNECTION_STRING, storageAccountName, storageAccountKey);

        final ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder(namespaceName, eventHubName, sasName, sasKey);

        eventProcessorHost = new EventProcessorHost(consumerHostname, eventHubName, consumerGroupName, eventHubConnectionString.toString(), storageConnectionString, containerName);

        options.setExceptionNotification(e -> {
            getLogger().error("An error occurred while receiving messages from Azure Event hub {}" +
                            " at consumer group {} and partition {}, action={}, hostname={}, exception={}",
                    new Object[]{eventHubName, consumerGroupName, e.getPartitionId(), e.getAction(), e.getHostname()}, e.getException());
        });


        eventProcessorHost.registerEventProcessorFactory(new EventProcessorFactory(), options).get();
    }

    private String orDefault(String value, String defaultValue) {
        return isEmpty(value) ? defaultValue : value;
    }

    private void validateRequiredProperty(PropertyDescriptor property, String value) {
        if (isEmpty(value)) {
            throw new IllegalArgumentException(String.format("'%s' is required, but not specified.", property.getDisplayName()));
        }
    }

}
