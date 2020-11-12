/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.alibaba.ververica.cdc.connectors.mongodb.internal;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import io.debezium.connector.mongodb.CollectionId;
import io.debezium.connector.mongodb.ConnectionContext;
import io.debezium.connector.mongodb.ConnectionContext.MongoPrimary;
import io.debezium.connector.mongodb.DisconnectEvent;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbTaskContext;
import io.debezium.connector.mongodb.ReplicaSet;
import io.debezium.connector.mongodb.ReplicaSets;
import io.debezium.connector.mongodb.SourceInfo;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Threads;
import org.apache.kafka.connect.errors.ConnectException;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.changestream.OperationType.DELETE;
import static com.mongodb.client.model.changestream.OperationType.INSERT;
import static com.mongodb.client.model.changestream.OperationType.REPLACE;
import static com.mongodb.client.model.changestream.OperationType.UPDATE;

/**
 *
 */
public class MongoDBCSStreamingChangeEventSource implements StreamingChangeEventSource {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBCSStreamingChangeEventSource.class);

	private static final String AUTHORIZATION_FAILURE_MESSAGE = "Command failed with error 13";

	private static final String OPERATION_FIELD = "op";
	private static final String OBJECT_FIELD = "o";
	private static final String OPERATION_CONTROL = "c";
	private static final String TX_OPS = "applyOps";

	private final EventDispatcher<CollectionId> dispatcher;
	private final ErrorHandler errorHandler;
	private final Clock clock;
	private final MongoDBCSOffsetContext offsetContext;
	private final ConnectionContext connectionContext;
	private final ReplicaSets replicaSets;
	private final MongoDbTaskContext taskContext;

	public MongoDBCSStreamingChangeEventSource(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext,
							ReplicaSets replicaSets, MongoDBCSOffsetContext offsetContext,
							EventDispatcher<CollectionId> dispatcher, ErrorHandler errorHandler, Clock clock) {
		this.connectionContext = taskContext.getConnectionContext();
		this.dispatcher = dispatcher;
		this.errorHandler = errorHandler;
		this.clock = clock;
		this.replicaSets = replicaSets;
		this.taskContext = taskContext;
		this.offsetContext = (offsetContext != null) ? offsetContext : initializeOffsets(connectorConfig, replicaSets);
	}

	@Override
	public void execute(ChangeEventSourceContext context) throws InterruptedException {
		// Starts a thread for each replica-set and executes the streaming process
		final int threads = replicaSets.replicaSetCount();
		final ExecutorService executor = Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.serverName(), "replicator-streaming", threads);
		final CountDownLatch latch = new CountDownLatch(threads);

		LOGGER.info("Starting {} thread(s) to stream changes for replica sets: {}", threads, replicaSets);
		replicaSets.validReplicaSets().forEach(replicaSet -> {
			executor.submit(() -> {
				MongoPrimary primaryClient = null;
				try {
					primaryClient = establishConnectionToPrimary(replicaSet);
					if (primaryClient != null) {
						final AtomicReference<MongoPrimary> primaryReference = new AtomicReference<>(primaryClient);
						primaryClient.execute("read from change stream on '" + replicaSet + "'", primary -> {
							readChangeStream(primary, primaryReference.get(), replicaSet, context);
						});
					}
				}
				catch (Throwable t) {
					LOGGER.error("Streaming for replica set {} failed", replicaSet.replicaSetName(), t);
					errorHandler.setProducerThrowable(t);
				}
				finally {
					if (primaryClient != null) {
						primaryClient.stop();
					}

					latch.countDown();
				}
			});
		});

		// Wait for the executor service to terminate.
		try {
			latch.await();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

		// Shutdown the executor and cleanup connections
		try {
			executor.shutdown();
		}
		finally {
			taskContext.getConnectionContext().shutdown();
		}
	}

	private MongoPrimary establishConnectionToPrimary(ReplicaSet replicaSet) {
		return connectionContext.primaryFor(replicaSet, taskContext.filters(), (desc, error) -> {
			// propagate authorization failures
			if (error.getMessage() != null && error.getMessage().startsWith(AUTHORIZATION_FAILURE_MESSAGE)) {
				throw new ConnectException("Error while attempting to " + desc, error);
			}
			else {
				dispatcher.dispatchConnectorEvent(new DisconnectEvent());
				LOGGER.error("Error while attempting to {}: {}", desc, error.getMessage(), error);
				throw new ConnectException("Error while attempting to " + desc, error);
			}
		});
	}

	private void readChangeStream(MongoClient primary, MongoPrimary primaryClient, ReplicaSet replicaSet, ChangeEventSourceContext context) {
		final ReplicaSetCSOffsetContext rsOffsetContext = offsetContext.getReplicaSetCSOffsetContext(replicaSet);

		final BsonTimestamp oplogStart = rsOffsetContext.lastOffsetTimestamp();
		final OptionalLong txOrder = rsOffsetContext.lastOffsetTxOrder();

		final ServerAddress primaryAddress = primary.getAddress();
		LOGGER.info("Reading change stream for '{}' primary {} starting at {}", replicaSet, primaryAddress, oplogStart);

		Bson filter = Filters.in("ns", primaryClient.collections().stream().map(collectionId ->
				new BsonDocument("db", new BsonString(collectionId.dbName()))
						.append("coll", new BsonString(collectionId.name()))).collect(Collectors.toList()));

		Bson operationFilter = getSkippedOperationsFilter();
		if (operationFilter != null) {
			filter = Filters.and(filter, operationFilter);
		}

		final int batchSize = taskContext.getConnectorConfig().getSnapshotFetchSize();

		String resumeTokenJSON = (String) rsOffsetContext.getOffset().get(SourceInfo.SESSION_TXN_ID);

		ChangeStreamIterable<Document> changeStream =
				primary.watch(Collections.singletonList(Aggregates.match(filter)))
				.fullDocument(FullDocument.UPDATE_LOOKUP)
				.batchSize(batchSize);
		if (Objects.isNull(resumeTokenJSON) || resumeTokenJSON.isEmpty()) {
			changeStream.startAtOperationTime(oplogStart);
		}
		else {
			BsonDocument resumeToken = BsonDocument.parse(resumeTokenJSON);
			changeStream.resumeAfter(resumeToken);
		}

		ReplicaSetCSContext oplogContext = new ReplicaSetCSContext(rsOffsetContext, primaryClient, replicaSet);

		try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStream.cursor()) {
			// In Replicator, this used cursor.hasNext() but this is a blocking call and I observed that this can
			// delay the shutdown of the connector by up to 15 seconds or longer. By introducing a Metronome, we
			// can respond to the stop request much faster and without much overhead.
			Metronome pause = Metronome.sleeper(Duration.ofMillis(500), clock);
			while (context.isRunning()) {
				// Use tryNext which will return null if no document is yet available from the cursor.
				// In this situation if not document is available, we'll pause.
				final ChangeStreamDocument<Document> event = cursor.tryNext();
				if (event != null) {
					if (!handleChangeStreamEvent(primaryAddress, event, oplogContext, context)) {
						// Something happened and we are supposed to stop reading
						return;
					}

					try {
						dispatcher.dispatchHeartbeatEvent(oplogContext.getOffset());
					}
					catch (InterruptedException e) {
						LOGGER.info("Replicator thread is interrupted");
						Thread.currentThread().interrupt();
						return;
					}
				}
				else {
					try {
						pause.pause();
					}
					catch (InterruptedException e) {
						break;
					}
				}
			}
		}
	}

	private Bson getSkippedOperationsFilter() {
		Set<Operation> skippedOperations = taskContext.getConnectorConfig().getSkippedOps();

		if (skippedOperations.isEmpty()) {
			return null;
		}

		Stream<String> skippedOperationsFilters = skippedOperations.stream().flatMap(op -> {
			switch (op.code()) {
				case "c":
					return Stream.of(INSERT.getValue());
				case "u":
					return Stream.of(UPDATE.getValue(),
							REPLACE.getValue());
				case "d":
					return Stream.of(DELETE.getValue());
				case "r":
					break;
			}
			return Stream.empty();
		});

		if (skippedOperationsFilters.count() <= 0) {
			return null;
		}

		return Filters.nin("operationType", skippedOperationsFilters.collect(Collectors.toList()));
	}

	private boolean handleChangeStreamEvent(ServerAddress primaryAddress, ChangeStreamDocument<Document> changeStreamEvent, ReplicaSetCSContext oplogContext,
											ChangeEventSourceContext context) {

		if (!MongoDBCSChangeRecordEmitter.isValidOperation(changeStreamEvent.getOperationType())) {
			LOGGER.debug("Skipping event with \"op={}\"", changeStreamEvent.getOperationType().getValue());
			return true;
		}

		CollectionId collectionId = new CollectionId(oplogContext.getReplicaSetName(),
				changeStreamEvent.getDatabaseName(), changeStreamEvent.getNamespace().getCollectionName());

		oplogContext.getOffset().changeStreamEvent(collectionId, changeStreamEvent);
		oplogContext.getOffset().getOffset();

		try {
			return dispatcher.dispatchDataChangeEvent(
					collectionId,
					new MongoDBCSChangeRecordEmitter(
							oplogContext.getOffset(),
							clock,
							changeStreamEvent));
		}
		catch (Exception e) {
			errorHandler.setProducerThrowable(e);
			return false;
		}
	}

	protected MongoDBCSOffsetContext initializeOffsets(MongoDbConnectorConfig connectorConfig, ReplicaSets replicaSets) {
		final Map<ReplicaSet, Document> positions = new LinkedHashMap<>();
		replicaSets.onEachReplicaSet(replicaSet -> {
			LOGGER.info("Determine Snapshot Offset for replica-set {}", replicaSet.replicaSetName());
			MongoPrimary primaryClient = establishConnectionToPrimary(replicaSet);
			if (primaryClient != null) {
				try {
					primaryClient.execute("get oplog position", primary -> {
						MongoCollection<Document> oplog = primary.getDatabase("local").getCollection("oplog.rs");
						Document last = oplog.find().sort(new Document("$natural", -1)).limit(1).first(); // may be null
						positions.put(replicaSet, last);
					});
				}
				finally {
					LOGGER.info("Stopping primary client");
					primaryClient.stop();
				}
			}
		});

		return new MongoDBCSOffsetContext(new SourceInfo(connectorConfig), new TransactionContext(), positions);
	}

	/**
	 * A context associated with a given replica set oplog read operation.
	 */
	private class ReplicaSetCSContext {
		private final ReplicaSetCSOffsetContext offset;
		private final MongoPrimary primary;
		private final ReplicaSet replicaSet;

		private BsonTimestamp incompleteEventTimestamp;
		private long incompleteTxOrder = 0;

		ReplicaSetCSContext(ReplicaSetCSOffsetContext offsetContext, MongoPrimary primary, ReplicaSet replicaSet) {
			this.offset = offsetContext;
			this.primary = primary;
			this.replicaSet = replicaSet;
		}

		ReplicaSetCSOffsetContext getOffset() {
			return offset;
		}

		MongoPrimary getPrimary() {
			return primary;
		}

		String getReplicaSetName() {
			return replicaSet.replicaSetName();
		}

		BsonTimestamp getIncompleteEventTimestamp() {
			return incompleteEventTimestamp;
		}

		public void setIncompleteEventTimestamp(BsonTimestamp incompleteEventTimestamp) {
			this.incompleteEventTimestamp = incompleteEventTimestamp;
		}

		public long getIncompleteTxOrder() {
			return incompleteTxOrder;
		}

		public void setIncompleteTxOrder(long incompleteTxOrder) {
			this.incompleteTxOrder = incompleteTxOrder;
		}
	}
}
