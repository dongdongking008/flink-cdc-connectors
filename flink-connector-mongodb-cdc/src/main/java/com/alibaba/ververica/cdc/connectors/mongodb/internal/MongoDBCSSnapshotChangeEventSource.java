/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.alibaba.ververica.cdc.connectors.mongodb.internal;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.debezium.connector.mongodb.CollectionId;
import io.debezium.connector.mongodb.ConnectionContext;
import io.debezium.connector.mongodb.ConnectionContext.MongoPrimary;
import io.debezium.connector.mongodb.DisconnectEvent;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbOffsetContext;
import io.debezium.connector.mongodb.MongoDbSnapshotChangeEventSource;
import io.debezium.connector.mongodb.MongoDbTaskContext;
import io.debezium.connector.mongodb.ReplicaSet;
import io.debezium.connector.mongodb.ReplicaSetOffsetContext;
import io.debezium.connector.mongodb.ReplicaSets;
import io.debezium.connector.mongodb.SourceInfo;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.apache.kafka.connect.errors.ConnectException;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link SnapshotChangeEventSource} that performs multi-threaded snapshots of replica sets.
 */
public class MongoDBCSSnapshotChangeEventSource extends MongoDbSnapshotChangeEventSource {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBCSSnapshotChangeEventSource.class);

	private static final String AUTHORIZATION_FAILURE_MESSAGE = "Command failed with error 13";

	private final MongoDbConnectorConfig connectorConfig;
	private final MongoDbTaskContext taskContext;
	private final MongoDbOffsetContext previousOffset;
	private final ConnectionContext connectionContext;
	private final ReplicaSets replicaSets;
	private final EventDispatcher<CollectionId> dispatcher;
	protected final Clock clock;
	private final SnapshotProgressListener snapshotProgressListener;
	private final ErrorHandler errorHandler;
	private AtomicBoolean aborted = new AtomicBoolean(false);

	public MongoDBCSSnapshotChangeEventSource(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext,
											ReplicaSets replicaSets, MongoDbOffsetContext previousOffset,
											EventDispatcher<CollectionId> dispatcher, Clock clock,
											SnapshotProgressListener snapshotProgressListener, ErrorHandler errorHandler) {
		super(connectorConfig, taskContext, replicaSets, previousOffset, dispatcher, clock, snapshotProgressListener, errorHandler);
		this.connectorConfig = connectorConfig;
		this.taskContext = taskContext;
		this.connectionContext = taskContext.getConnectionContext();
		this.previousOffset = previousOffset;
		this.replicaSets = replicaSets;
		this.dispatcher = dispatcher;
		this.clock = clock;
		this.snapshotProgressListener = snapshotProgressListener;
		this.errorHandler = errorHandler;
	}

	@Override
	protected SnapshotContext prepare(ChangeEventSourceContext sourceContext) throws Exception {
		return new MongoDBCSSnapshotChangeEventSource.MongoDbSnapshotContext();
	}

	@Override
	protected SnapshotResult doExecute(ChangeEventSourceContext context, SnapshotContext snapshotContext, SnapshottingTask snapshottingTask)
			throws Exception {
		final MongoDbSnapshottingTask mongoDbSnapshottingTask = (MongoDbSnapshottingTask) snapshottingTask;
		final MongoDbSnapshotContext mongoDbSnapshotContext = (MongoDbSnapshotContext) snapshotContext;

		LOGGER.info("Snapshot step 1 - Preparing");
		snapshotProgressListener.snapshotStarted();

		if (previousOffset != null && previousOffset.isSnapshotRunning()) {
			LOGGER.info("Previous snapshot was cancelled before completion; a new snapshot will be taken.");
		}

		LOGGER.info("Snapshot step 2 - Determining snapshot offsets");
		determineSnapshotOffsets(mongoDbSnapshotContext, replicaSets);

		List<ReplicaSet> replicaSetsToSnapshot = mongoDbSnapshottingTask.getReplicaSetsToSnapshot();

		final int threads = replicaSetsToSnapshot.size();
		final ExecutorService executor = Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.serverName(), "replicator-snapshot", threads);
		final CountDownLatch latch = new CountDownLatch(threads);

		LOGGER.info("Ignoring unnamed replica sets: {}", replicaSets.unnamedReplicaSets());
		LOGGER.info("Starting {} thread(s) to snapshot replica sets: {}", threads, replicaSetsToSnapshot);

		LOGGER.info("Snapshot step 3 - Snapshotting data");
		replicaSetsToSnapshot.forEach(replicaSet -> {
			executor.submit(() -> {
				try {
					taskContext.configureLoggingContext(replicaSet.replicaSetName());
					try {
						snapshotReplicaSet(context, mongoDbSnapshotContext, replicaSet);
					}
					finally {
						final MongoDbOffsetContext offset = (MongoDbOffsetContext) snapshotContext.offset;
						// todo: DBZ-1726 - this causes MongoDbConnectorIT#shouldEmitHeartbeatMessages to fail
						// omitted for now since it does not appear we did this in previous connector code.
						// dispatcher.alwaysDispatchHeartbeatEvent(offset.getReplicaSetOffsetContext(replicaSet));
					}
				}
				catch (Throwable t) {
					LOGGER.error("Snapshot for replica set {} failed", replicaSet.replicaSetName(), t);
					errorHandler.setProducerThrowable(t);
				}
				finally {
					latch.countDown();
				}
			});
		});

		// Wait for the executor service threads to end.
		try {
			latch.await();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			aborted.set(true);
		}

		// Shutdown executor and close connections
		try {
			executor.shutdown();
		}
		finally {
			LOGGER.info("Stopping mongodb connections");
			taskContext.getConnectionContext().shutdown();
		}

		if (aborted.get()) {
			return SnapshotResult.aborted();
		}

		snapshotProgressListener.snapshotCompleted();

		return SnapshotResult.completed(snapshotContext.offset);
	}

	private void snapshotReplicaSet(ChangeEventSourceContext sourceContext, MongoDbSnapshotContext ctx, ReplicaSet replicaSet) throws InterruptedException {
		MongoPrimary primaryClient = null;
		try {
			primaryClient = establishConnectionToPrimary(replicaSet);
			if (primaryClient != null) {
				createDataEvents(sourceContext, ctx, replicaSet, primaryClient);
			}
		}
		finally {
			if (primaryClient != null) {
				primaryClient.stop();
			}
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
				LOGGER.error("Error while attempting to {}: ", desc, error.getMessage(), error);
				throw new ConnectException("Error while attempting to " + desc, error);
			}
		});
	}

	protected void determineSnapshotOffsets(MongoDbSnapshotContext ctx, ReplicaSets replicaSets) {
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

		ctx.offset = new MongoDBCSOffsetContext(new SourceInfo(connectorConfig), new TransactionContext(), positions);
	}

	private void createDataEvents(ChangeEventSourceContext sourceContext, MongoDbSnapshotContext snapshotContext, ReplicaSet replicaSet,
								MongoPrimary primaryClient)
			throws InterruptedException {
		SnapshotReceiver snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();
		snapshotContext.offset.preSnapshotStart();

		createDataEventsForReplicaSet(sourceContext, snapshotContext, snapshotReceiver, replicaSet, primaryClient);

		snapshotContext.offset.preSnapshotCompletion();
		snapshotReceiver.completeSnapshot();
		snapshotContext.offset.postSnapshotCompletion();
	}

	/**
	 * Dispatches the data change events for the records of a single replica-set.
	 */
	private void createDataEventsForReplicaSet(ChangeEventSourceContext sourceContext, MongoDbSnapshotContext snapshotContext,
											SnapshotReceiver snapshotReceiver, ReplicaSet replicaSet, MongoPrimary primaryClient)
			throws InterruptedException {

		final String rsName = replicaSet.replicaSetName();

		final MongoDBCSOffsetContext offsetContext = (MongoDBCSOffsetContext) snapshotContext.offset;
		final ReplicaSetOffsetContext rsOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);

		snapshotContext.lastCollection = false;
		offsetContext.startReplicaSetSnapshot(replicaSet.replicaSetName());

		LOGGER.info("Beginning snapshot of '{}' at {}", rsName, rsOffsetContext.getOffset());

		final List<CollectionId> collections = primaryClient.collections();
		snapshotProgressListener.monitoredDataCollectionsDetermined(collections);
		if (connectionContext.maxNumberOfCopyThreads() > 1) {
			// Since multiple copy threads are to be used, create a thread pool and initiate the copy.
			// The current thread will wait until the copy threads either have completed or an error occurred.
			final int numThreads = Math.min(collections.size(), connectionContext.maxNumberOfCopyThreads());
			final Queue<CollectionId> collectionsToCopy = new ConcurrentLinkedQueue<>(collections);

			final String copyThreadName = "copy-" + (replicaSet.hasReplicaSetName() ? replicaSet.replicaSetName() : "main");
			final ExecutorService copyThreads = Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.serverName(),
					copyThreadName, connectionContext.maxNumberOfCopyThreads());
			final CountDownLatch latch = new CountDownLatch(numThreads);
			final AtomicBoolean aborted = new AtomicBoolean(false);
			final AtomicInteger threadCounter = new AtomicInteger(0);

			LOGGER.info("Preparing to use {} thread(s) to snapshot {} collection(s): {}", numThreads, collections.size(),
					Strings.join(", ", collections));

			for (int i = 0; i < numThreads; ++i) {
				copyThreads.submit(() -> {
					taskContext.configureLoggingContext(replicaSet.replicaSetName() + "-sync" + threadCounter.incrementAndGet());
					try {
						CollectionId id = null;
						while (!aborted.get() && (id = collectionsToCopy.poll()) != null) {
							if (!sourceContext.isRunning()) {
								throw new InterruptedException("Interrupted while snapshotting replica set " + replicaSet.replicaSetName());
							}

							if (collectionsToCopy.isEmpty()) {
								snapshotContext.lastCollection = true;
							}

							createDataEventsForCollection(
									sourceContext,
									snapshotContext,
									snapshotReceiver,
									replicaSet,
									id,
									primaryClient);
						}
					}
					catch (InterruptedException e) {
						// Do nothing so that this thread is stopped
						aborted.set(true);
					}
					finally {
						latch.countDown();
					}
				});
			}

			// wait for all copy threads to finish
			try {
				latch.await();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				aborted.set(true);
			}

			copyThreads.shutdown();
		}
		else {
			// Only 1 thread should be used for copying collections.
			// In this use case since the replica-set snapshot is already in a separate thread, there is not
			// a real reason to spawn additional threads but instead just run within the current thread.
			for (Iterator<CollectionId> it = collections.iterator(); it.hasNext();) {
				final CollectionId collectionId = it.next();

				if (!sourceContext.isRunning()) {
					throw new InterruptedException("Interrupted while snapshotting replica set " + replicaSet.replicaSetName());
				}

				if (!it.hasNext()) {
					snapshotContext.lastCollection = true;
				}

				createDataEventsForCollection(
						sourceContext,
						snapshotContext,
						snapshotReceiver,
						replicaSet,
						collectionId,
						primaryClient);
			}
		}

		offsetContext.stopReplicaSetSnapshot(replicaSet.replicaSetName());
	}

	private void createDataEventsForCollection(ChangeEventSourceContext sourceContext, MongoDbSnapshotContext snapshotContext, SnapshotReceiver snapshotReceiver,
											ReplicaSet replicaSet, CollectionId collectionId, MongoPrimary primaryClient)
			throws InterruptedException {

		long exportStart = clock.currentTimeInMillis();
		LOGGER.info("\t Exporting data for collection '{}'", collectionId);

		primaryClient.executeBlocking("sync '" + collectionId + "'", primary -> {
			final MongoDatabase database = primary.getDatabase(collectionId.dbName());
			final MongoCollection<Document> collection = database.getCollection(collectionId.name());

			final int batchSize = taskContext.getConnectorConfig().getSnapshotFetchSize();

			long docs = 0;
			try (MongoCursor<Document> cursor = collection.find().batchSize(batchSize).iterator()) {
				snapshotContext.lastRecordInCollection = false;
				if (cursor.hasNext()) {
					while (cursor.hasNext()) {
						if (!sourceContext.isRunning()) {
							throw new InterruptedException("Interrupted while snapshotting collection " + collectionId.name());
						}

						Document document = cursor.next();
						docs++;

						snapshotContext.lastRecordInCollection = !cursor.hasNext();

						if (snapshotContext.lastCollection && snapshotContext.lastRecordInCollection) {
							snapshotContext.offset.markLastSnapshotRecord();
						}

						dispatcher.dispatchSnapshotEvent(collectionId, getChangeRecordEmitter(snapshotContext, collectionId, document, replicaSet), snapshotReceiver);
					}
				}
				else if (snapshotContext.lastCollection) {
					// if the last collection does not contain any records we still need to mark the last processed event as last one
					snapshotContext.offset.markLastSnapshotRecord();
				}

				LOGGER.info("\t Finished snapshotting {} records for collection '{}'; total duration '{}'", docs, collectionId,
						Strings.duration(clock.currentTimeInMillis() - exportStart));
				snapshotProgressListener.dataCollectionSnapshotCompleted(collectionId, docs);
			}
		});
	}

	protected ChangeRecordEmitter getChangeRecordEmitter(SnapshotContext snapshotContext, CollectionId collectionId, Document document, ReplicaSet replicaSet) {
		final MongoDbOffsetContext offsetContext = ((MongoDbOffsetContext) snapshotContext.offset);

		final ReplicaSetOffsetContext replicaSetOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);
		replicaSetOffsetContext.readEvent(collectionId, getClock().currentTime());

		return new MongoDBCSChangeRecordEmitter(replicaSetOffsetContext, getClock(), document);
	}

	protected Clock getClock() {
		return clock;
	}

	/**
	 * Mutable context that is populated in the course of snapshotting.
	 */
	private static class MongoDbSnapshotContext extends SnapshotContext {
		public boolean lastCollection;
		public boolean lastRecordInCollection;
	}
}
