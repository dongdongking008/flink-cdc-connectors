package com.alibaba.ververica.cdc.connectors.mongodb.internal;

import io.debezium.connector.mongodb.MongoDbConnector;
import org.apache.kafka.connect.connector.Task;

/**
 * A Kafka Connect source connector that creates {@link MongoDBCSConnectorTask tasks} that replicate the context of one or more
 * MongoDB replica sets.
 */
public class MongoDBCSConnector extends MongoDbConnector {

	@Override
	public Class<? extends Task> taskClass() {
		return MongoDBCSConnectorTask.class;
	}

}
