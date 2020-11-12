/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.alibaba.ververica.cdc.connectors.mongodb.internal;

import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbOffsetContext;
import io.debezium.connector.mongodb.ReplicaSet;
import io.debezium.connector.mongodb.ReplicaSets;
import io.debezium.connector.mongodb.SourceInfo;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A context that facilitates the management of the current change streams offsets across a set of mongodb replica sets.
 */
public class MongoDBCSOffsetContext extends MongoDbOffsetContext implements OffsetContext {

	private final SourceInfo sourceInfo;
	private final TransactionContext transactionContext;
	private final Map<ReplicaSet, ReplicaSetCSOffsetContext> replicaSetOffsetContexts = new ConcurrentHashMap<>();

	public MongoDBCSOffsetContext(SourceInfo sourceInfo, TransactionContext transactionContext) {
		super(sourceInfo, transactionContext);
		this.sourceInfo = sourceInfo;
		this.transactionContext = transactionContext;
	}

	public MongoDBCSOffsetContext(SourceInfo sourceInfo, TransactionContext transactionContext, Map<ReplicaSet, Document> offsets) {
		super(sourceInfo, transactionContext, offsets);
		this.sourceInfo = sourceInfo;
		this.transactionContext = transactionContext;
	}

	/**
	 * Get a {@link ReplicaSetCSOffsetContext} instance for a given {@link ReplicaSet}.
	 *
	 * @param replicaSet the replica set; must not be null.
	 * @return a replica set offset context; never null.
	 */
	public ReplicaSetCSOffsetContext getReplicaSetCSOffsetContext(ReplicaSet replicaSet) {
		return replicaSetOffsetContexts.computeIfAbsent(replicaSet, rs -> new ReplicaSetCSOffsetContext(this, rs, sourceInfo));
	}

	/**
	 * Loader of MongoDBCSOffsetContext.
	 */
	public static class Loader {

		private final ReplicaSets replicaSets;
		private final SourceInfo sourceInfo;

		public Loader(MongoDbConnectorConfig connectorConfig, ReplicaSets replicaSets) {
			this.sourceInfo = new SourceInfo(connectorConfig);
			this.replicaSets = replicaSets;
		}

		public Collection<Map<String, String>> getPartitions() {
			// todo: DBZ-1726 - follow-up by removing partition management from SourceInfo
			final Collection<Map<String, String>> partitions = new ArrayList<>();
			replicaSets.onEachReplicaSet(replicaSet -> {
				final String name = replicaSet.replicaSetName(); // may be null for standalone servers
				if (name != null) {
					Map<String, String> partition = sourceInfo.partition(name);
					partitions.add(partition);
				}
			});
			return partitions;
		}

		public MongoDBCSOffsetContext loadOffsets(Map<Map<String, String>, Map<String, Object>> offsets) {
			// todo: DBZ-1726 - follow-up by removing offset management from SourceInfo
			offsets.forEach(sourceInfo::setOffsetFor);
			return new MongoDBCSOffsetContext(sourceInfo, new TransactionContext());
		}
	}

	@Override
	public String toString() {
		return "MongoDBCSOffsetContext [sourceInfo=" + sourceInfo + "]";
	}

	void startReplicaSetSnapshot(String replicaSetName) {
		sourceInfo.startInitialSync(replicaSetName);
	}

	void stopReplicaSetSnapshot(String replicaSetName) {
		sourceInfo.stopInitialSync(replicaSetName);
	}
}
