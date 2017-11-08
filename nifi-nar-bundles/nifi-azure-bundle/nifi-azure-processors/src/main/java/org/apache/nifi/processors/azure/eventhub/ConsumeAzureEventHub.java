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
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.AzureConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.util.StringUtils.isEmpty;

// TODO: Capability description, tag ... etc
// TODO: Input not allowed.
// TODO: Trigger serially.
// TODO: Record.
public class ConsumeAzureEventHub extends AbstractSessionFactoryProcessor {

    private volatile EventProcessorHost eventProcessorHost;
    private volatile ProcessSessionFactory processSessionFactory;

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
    // TODO: Support initial offset so that only new messages can be consumed even if there're old ones.


    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles received from Event Hub.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Collections.singleton(REL_SUCCESS);
    private static final List<PropertyDescriptor> PROPERTIES;

    static {
        PROPERTIES = Collections.unmodifiableList(Arrays.asList(
                NAMESPACE, EVENT_HUB_NAME, ACCESS_POLICY_NAME, POLICY_PRIMARY_KEY, CONSUMER_GROUP, CONSUMER_HOSTNAME,
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

    public static class EventProcessor implements IEventProcessor {

        private static Logger logger = LoggerFactory.getLogger(EventProcessor.class);

        @Override
        public void onOpen(PartitionContext context) throws Exception {
            logger.info("Consumer group {} opened partition {} of {}", context.getConsumerGroupName(), context.getPartitionId(), context.getEventHubPath());
        }

        @Override
        public void onClose(PartitionContext context, CloseReason reason) throws Exception {
            logger.info("Consumer group {} closed partition {} of {}. reason={}", context.getConsumerGroupName(), context.getPartitionId(), context.getEventHubPath(), reason);
        }

        @Override
        public void onEvents(PartitionContext context, Iterable<EventData> messages) throws Exception {
            // TODO: implement consuming logic here.
            messages.forEach(m -> logger.info("Message: {}", new Object[]{new String(m.getBytes(), StandardCharsets.UTF_8)}));
        }

        @Override
        public void onError(PartitionContext context, Throwable e) {
            // TODO: what's the difference between this and Exception Notification??
            logger.error("An error occurred while receiving messages from Azure Event hub {} at partition {}," +
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
            } catch (Exception e) {
                throw new RuntimeException("Failed to unregister the event processor due to " + e, e);
            }
        }
    }

    private void registerEventProcessor(final ProcessContext context) throws Exception {
        // Validate required properties.
        final String consumerGroupName = context.getProperty(CONSUMER_GROUP).evaluateAttributeExpressions().getValue();
        validateRequiredProperty(CONSUMER_GROUP, consumerGroupName);

        final String namespaceName = context.getProperty(NAMESPACE).evaluateAttributeExpressions().getValue();
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

        final String storageConnectionString = String.format(AzureConstants.FORMAT_DEFAULT_CONNECTION_STRING, storageAccountName, storageAccountKey);

        final ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder(namespaceName, eventHubName, sasName, sasKey);

        eventProcessorHost = new EventProcessorHost(consumerHostname, eventHubName, consumerGroupName, eventHubConnectionString.toString(), storageConnectionString, containerName);

        EventProcessorOptions options = new EventProcessorOptions();
        options.setExceptionNotification(e -> {
            // TODO: error handling.
            getLogger().error("An error occurred while receiving messages from Azure Event hub {}" +
                            " at consumer group {} and partition {}, action={}, hostname={}, exception={}",
                    new Object[]{eventHubName, consumerGroupName, e.getPartitionId(), e.getAction(), e.getHostname()}, e.getException());
        });
        eventProcessorHost.registerEventProcessor(EventProcessor.class, options).get();
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
