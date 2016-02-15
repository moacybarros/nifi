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
package org.apache.nifi.processors.aws.dynamodb;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Base class for Nifi dynamo db related processors
 *
 * @see DeleteDynamoDB
 * @see PutDynamoDB
 * @see GetDynamoDB
 */
public abstract class AbstractDynamoDBProcessor extends AbstractAWSCredentialsProviderProcessor<AmazonDynamoDBClient> {

	public static final AllowableValue ALLOWABLE_VALUE_STRING = new AllowableValue("string");
	public static final AllowableValue ALLOWABLE_VALUE_NUMBER = new AllowableValue("number");

	public static final String DYNAMODB_KEY_ERROR_UNPROCESSED = "dynamodb.key.error.unprocessed";
	public static final String DYNAMODB_RANGE_KEY_VALUE_ERROR = "dynmodb.range.key.value.error";
	public static final String DYNAMODB_KEY_ERROR_NOT_FOUND = "dynamodb.key.error.not.found";
	public static final String DYNAMODB_ERROR_EXCEPTION_MESSAGE = "dynamodb.error.exception.message";
	public static final String DYNAMODB_ERROR_CODE = "dynamodb.error.code";
	public static final String DYNAMODB_ERROR_MESSAGE = "dynamodb.error.message";
	public static final String DYNAMODB_ERROR_TYPE = "dynamodb.error.type";
	public static final String DYNAMODB_ERROR_SERVICE = "dynamodb.error.service";
	public static final String DYNAMODB_ERROR_RETRYABLE = "dynamodb.error.retryable";
	public static final String DYNAMODB_ERROR_REQUEST_ID = "dynamodb.error.request.id";
	public static final String DYNAMODB_ERROR_STATUS_CODE = "dynamodb.error.status.code";

    public static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("Table Name")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The DynamoDB table name")
            .build();

    public static final PropertyDescriptor HASH_KEY_VALUE = new PropertyDescriptor.Builder()
            .name("Hash Key Value")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The hash key value of the item")
            .defaultValue("${dynamodb.item.hash.key.value}")
            .build();

    public static final PropertyDescriptor RANGE_KEY_VALUE = new PropertyDescriptor.Builder()
            .name("Range Key Value")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("${dynamodb.item.range.key.value}")
            .build();

    public static final PropertyDescriptor HASH_KEY_VALUE_TYPE = new PropertyDescriptor.Builder()
            .name("Hash Key Value Type")
            .required(true)
            .description("The hash key value type of the item")
            .defaultValue("string")
            .allowableValues(ALLOWABLE_VALUE_STRING, ALLOWABLE_VALUE_NUMBER)
            .build();

    public static final PropertyDescriptor RANGE_KEY_VALUE_TYPE = new PropertyDescriptor.Builder()
            .name("Range Key Value Type")
            .required(true)
            .description("The range key value type of the item")
            .defaultValue("string")
            .allowableValues(ALLOWABLE_VALUE_STRING, ALLOWABLE_VALUE_NUMBER)
            .build();

    public static final PropertyDescriptor HASH_KEY_NAME = new PropertyDescriptor.Builder()
            .name("Hash Key Name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The hash key name of the item")
            .build();

    public static final PropertyDescriptor RANGE_KEY_NAME = new PropertyDescriptor.Builder()
            .name("Range Key Name")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The range key name of the item")
            .build();

    public static final PropertyDescriptor JSON_DOCUMENT = new PropertyDescriptor.Builder()
            .name("Json Document attribute")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The Json document to be retrieved from the dynamodb item")
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch items for each request (between 1 and 50)")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.createLongValidator(1, 50, true))
            .defaultValue("1")
            .description("The items to be retrieved in one batch")
            .build();

    public static final PropertyDescriptor DOCUMENT_CHARSET = new PropertyDescriptor.Builder()
            .name("Character set of document")
            .description("Character set of data in the document")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .required(true)
            .defaultValue(Charset.defaultCharset().name())
            .build();

    protected DynamoDB dynamoDB;

    /**
     * Create client using credentials provider. This is the preferred way for creating clients
     */
    @Override
    protected AmazonDynamoDBClient createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration config) {
        getLogger().info("Creating client with credentials provider");

        final AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentialsProvider, config);

        return client;
    }

    /**
     * Create client using AWSCredentials
     *
     * @deprecated use {@link #createClient(ProcessContext, AWSCredentialsProvider, ClientConfiguration)} instead
     */
    @Override
    protected AmazonDynamoDBClient createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
        getLogger().info("Creating client with awd credentials");

        final AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials, config);

        return client;
    }

    protected Object getValue(ProcessContext context, PropertyDescriptor type, PropertyDescriptor value, FlowFile flowFile) {
    	if ( context.getProperty(type).getValue().equals(ALLOWABLE_VALUE_STRING.getValue())) {
    		return context.getProperty(value).evaluateAttributeExpressions(flowFile).getValue();
    	}
    	else {
    		return context.getProperty(value).evaluateAttributeExpressions(flowFile).asLong();    		
    	}
	}

	protected DynamoDB getDynamoDB() {
        if (dynamoDB == null) {
            dynamoDB = new DynamoDB(getClient());
        }
        return dynamoDB;
    }

	protected Object getValue(Map<String, AttributeValue> item, String keyName, String valueType) {
		if ( ALLOWABLE_VALUE_STRING.getValue().equals(valueType)) {
			AttributeValue val = item.get(keyName);
			if ( val == null ) return val;
			else return val.getS();
		} else {
			AttributeValue val = item.get(keyName);
			if ( val == null ) return val;
			else return val.getN();
		}
	}

	protected List<FlowFile> processException(final ProcessSession session, List<FlowFile> flowFiles, Exception exception) {
		List<FlowFile> failedFlowFiles = new ArrayList<>();
		for (FlowFile flowFile : flowFiles) {
			flowFile = session.putAttribute(flowFile, DYNAMODB_ERROR_EXCEPTION_MESSAGE, exception.getMessage() );
			failedFlowFiles.add(flowFile);
		}
		return failedFlowFiles;
	}

    protected List<FlowFile> processException(final ProcessSession session, List<FlowFile> flowFiles,
			AmazonClientException exception) {
		List<FlowFile> failedFlowFiles = new ArrayList<>();
		for (FlowFile flowFile : flowFiles) {
			flowFile = session.putAttribute(flowFile, DYNAMODB_ERROR_EXCEPTION_MESSAGE, exception.getMessage() );
			flowFile = session.putAttribute(flowFile, DYNAMODB_ERROR_RETRYABLE, Boolean.toString(exception.isRetryable()));
			failedFlowFiles.add(flowFile);
		}
		return failedFlowFiles;
	}

    protected List<FlowFile> processException(final ProcessSession session, List<FlowFile> flowFiles,
			AmazonServiceException exception) {
		List<FlowFile> failedFlowFiles = new ArrayList<>();
		for (FlowFile flowFile : flowFiles) {
			flowFile = session.putAttribute(flowFile, DYNAMODB_ERROR_EXCEPTION_MESSAGE, exception.getMessage() );
			flowFile = session.putAttribute(flowFile, DYNAMODB_ERROR_CODE, exception.getErrorCode() );
			flowFile = session.putAttribute(flowFile, DYNAMODB_ERROR_MESSAGE, exception.getErrorMessage() );
			flowFile = session.putAttribute(flowFile, DYNAMODB_ERROR_TYPE, exception.getErrorType().name() );
			flowFile = session.putAttribute(flowFile, DYNAMODB_ERROR_SERVICE, exception.getServiceName() );
			flowFile = session.putAttribute(flowFile, DYNAMODB_ERROR_RETRYABLE, Boolean.toString(exception.isRetryable()));
			flowFile = session.putAttribute(flowFile, DYNAMODB_ERROR_REQUEST_ID, exception.getRequestId() );
			flowFile = session.putAttribute(flowFile, DYNAMODB_ERROR_STATUS_CODE, Integer.toString(exception.getStatusCode()) );
			failedFlowFiles.add(flowFile);
		}
		return failedFlowFiles;
	}
    
}
