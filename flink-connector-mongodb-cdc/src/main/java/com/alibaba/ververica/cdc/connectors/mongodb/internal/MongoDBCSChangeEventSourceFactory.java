/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.alibaba.ververica.cdc.connectors.mongodb.internal;

import io.debezium.connector.mongodb.CollectionId;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbTaskContext;
import io.debezium.connector.mongodb.ReplicaSets;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;

/**
 * Factory for creating {@link ChangeEventSource}s specific for the MongoDb connector.
 */
public class MongoDBCSChangeEventSourceFactory implements ChangeEventSourceFactory {

	private final MongoDbConnectorConfig configuration;
	private final ErrorHandler errorHandler;
	private final EventDispatcher<CollectionId> dispatcher;
	private final Clock clock;
	private final ReplicaSets replicaSets;
	private final MongoDbTaskContext taskContext;

	public MongoDBCSChangeEventSourceFactory(MongoDbConnectorConfig configuration, ErrorHandler errorHandler, EventDispatcher<CollectionId> dispatcher,
				Clock clock, ReplicaSets replicaSets, MongoDbTaskContext taskContext) {
		this.configuration = configuration;
		this.errorHandler = errorHandler;
		this.dispatcher = dispatcher;
		this.clock = clock;
		this.replicaSets = replicaSets;
		this.taskContext = taskContext;
	}

	@Override
	public SnapshotChangeEventSource getSnapshotChangeEventSource(OffsetContext offsetContext, SnapshotProgressListener snapshotProgressListener) {
		return new MongoDBCSSnapshotChangeEventSource(
				configuration,
				taskContext,
				replicaSets,
				(MongoDBCSOffsetContext) offsetContext,
				dispatcher,
				clock,
				snapshotProgressListener,
				errorHandler);
	}

	@Override
	public StreamingChangeEventSource getStreamingChangeEventSource(OffsetContext offsetContext) {
		return new MongoDBCSStreamingChangeEventSource(
				configuration,
				taskContext,
				replicaSets,
				(MongoDBCSOffsetContext) offsetContext,
				dispatcher,
				errorHandler,
				clock);
	}
}
