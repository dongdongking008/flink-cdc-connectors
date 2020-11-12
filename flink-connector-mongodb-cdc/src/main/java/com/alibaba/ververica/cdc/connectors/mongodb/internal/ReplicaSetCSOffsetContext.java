/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.alibaba.ververica.cdc.connectors.mongodb.internal;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.mongodb.CollectionId;
import io.debezium.connector.mongodb.ReplicaSet;
import io.debezium.connector.mongodb.ReplicaSetOffsetContext;
import io.debezium.connector.mongodb.SourceInfo;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Collect;
import org.bson.Document;

import java.util.Map;

/**
 * An {@link OffsetContext} implementation that is specific to a single {@link ReplicaSet}.
 *
 * <p>The mongodb connector operates multiple threads during snapshot and streaming modes where each {@link ReplicaSet}
 * is processed individually and the offsets that pertain to that {@link ReplicaSet} should be maintained in such a
 * way that is considered thread-safe.  This implementation offers such safety.
 */
@ThreadSafe
public class ReplicaSetCSOffsetContext extends ReplicaSetOffsetContext implements OffsetContext {

	private final MongoDBCSOffsetContext offsetContext;
	private final String replicaSetName;
	private final SourceInfo sourceInfo;

	public ReplicaSetCSOffsetContext(MongoDBCSOffsetContext offsetContext, ReplicaSet replicaSet, SourceInfo sourceInfo) {
		super(offsetContext, replicaSet, sourceInfo);
		this.offsetContext = offsetContext;
		this.replicaSetName = replicaSet.replicaSetName();
		this.sourceInfo = sourceInfo;
	}

	public void changeStreamEvent(CollectionId collectionId, ChangeStreamDocument<Document> event) {
		Map<String, Object> offset = Collect.hashMapOf(
				SourceInfo.TIMESTAMP, Integer.valueOf(event.getClusterTime().getTime()),
				SourceInfo.ORDER, event.getClusterTime().getInc(),
				SourceInfo.OPERATION_ID, 0,
				SourceInfo.SESSION_TXN_ID, event.getResumeToken().toJson());
		sourceInfo.setOffsetFor(replicaSetName, offset);
		sourceInfo.collectionEvent(replicaSetName, collectionId);
	}
}
