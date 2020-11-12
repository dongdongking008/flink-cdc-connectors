/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.alibaba.ververica.cdc.connectors.mongodb.internal;

import com.mongodb.MongoClient;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.mongodb.MongoDbCollectionSchema;
import io.debezium.connector.mongodb.MongoDbSchema;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;
import org.apache.kafka.connect.data.Struct;
import org.bson.Document;
import org.bson.codecs.Encoder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Emits change data based on a collection document.
 */
public class MongoDBCSChangeRecordEmitter extends AbstractChangeRecordEmitter<MongoDbCollectionSchema> {

	private final ChangeStreamDocument<Document> changeStreamEvent;
	private final Document snapshotDoc;

	@ThreadSafe
	private static final Map<OperationType, Operation> OPERATION_LITERALS;

	static {
		Map<OperationType, Operation> literals = new HashMap<>();

		literals.put(OperationType.INSERT, Operation.UPDATE);
		literals.put(OperationType.UPDATE, Operation.UPDATE);
		literals.put(OperationType.REPLACE, Operation.UPDATE);
		literals.put(OperationType.DELETE, Operation.DELETE);

		OPERATION_LITERALS = Collections.unmodifiableMap(literals);
	}

	public MongoDBCSChangeRecordEmitter(OffsetContext offsetContext, Clock clock, ChangeStreamDocument<Document> changeStreamEvent) {
		super(offsetContext, clock);
		this.changeStreamEvent = changeStreamEvent;
		this.snapshotDoc = null;
	}

	public MongoDBCSChangeRecordEmitter(OffsetContext offsetContext, Clock clock, Document snapshotDoc) {
		super(offsetContext, clock);
		this.changeStreamEvent = null;
		this.snapshotDoc = snapshotDoc;
	}

	@Override
	protected Operation getOperation() {
		if (snapshotDoc != null) {
			return Operation.READ;
		}
		Operation op = OPERATION_LITERALS.get(changeStreamEvent.getOperationType());
		if (changeStreamEvent.getFullDocument() == null) {
			if (op.equals(Operation.UPDATE)) {
				op = Operation.DELETE;
			}
		}
		return op;
	}

	@Override
	protected void emitReadRecord(Receiver receiver, MongoDbCollectionSchema schema) throws InterruptedException {
		final Object newKey = schema.keyFromDocument(snapshotDoc);
		assert newKey != null;

		final Struct value = schema.valueFromDocument(snapshotDoc, null, getOperation());
		value.put(FieldName.SOURCE, getOffset().getSourceInfo());
		value.put(FieldName.OPERATION, getOperation().code());
		value.put(FieldName.TIMESTAMP, getClock().currentTimeAsInstant().toEpochMilli());

		receiver.changeRecord(schema, getOperation(), newKey, value, getOffset(), null);
	}

	@Override
	protected void emitCreateRecord(Receiver receiver, MongoDbCollectionSchema schema) throws InterruptedException {
		createAndEmitChangeRecord(receiver, schema);
	}

	@Override
	protected void emitUpdateRecord(Receiver receiver, MongoDbCollectionSchema schema) throws InterruptedException {
		createAndEmitChangeRecord(receiver, schema);
	}

	@Override
	protected void emitDeleteRecord(Receiver receiver, MongoDbCollectionSchema schema) throws InterruptedException {
		Document keyDocument = Document.parse(changeStreamEvent.getDocumentKey().toJson());
		final Object newKey = schema.keyFromDocument(keyDocument);
		assert newKey != null;

		final Struct value = schema.valueFromDocument(keyDocument, keyDocument, getOperation());
		value.put(FieldName.SOURCE, getOffset().getSourceInfo());
		value.put(FieldName.OPERATION, getOperation().code());
		value.put(FieldName.TIMESTAMP, getClock().currentTimeAsInstant().toEpochMilli());

		receiver.changeRecord(schema, getOperation(), newKey, value, getOffset(), null);
	}

	private static String resolveValueTransformer(Document doc) {
		Encoder<Document> encoder = MongoClient.getDefaultCodecRegistry().get(Document.class);
		return doc.toJson(MongoDbSchema.COMPACT_JSON_SETTINGS, encoder);
	}

	private void createAndEmitChangeRecord(Receiver receiver, MongoDbCollectionSchema schema) throws InterruptedException {
		Document keyDocument = Document.parse(changeStreamEvent.getDocumentKey().toJson());
		final Object newKey = schema.keyFromDocument(keyDocument);
		assert newKey != null;

		Document patchObject = changeStreamEvent.getFullDocument();

		final Struct value = schema.valueFromDocument(patchObject, keyDocument, getOperation());
		value.put(FieldName.AFTER, resolveValueTransformer(patchObject));
		value.put(FieldName.SOURCE, getOffset().getSourceInfo());
		value.put(FieldName.OPERATION, getOperation().code());
		value.put(FieldName.TIMESTAMP, getClock().currentTimeAsInstant().toEpochMilli());

		receiver.changeRecord(schema, getOperation(), newKey, value, getOffset(), null);
	}

	public static boolean isValidOperation(OperationType operationType) {
		return OPERATION_LITERALS.containsKey(operationType);
	}
}
