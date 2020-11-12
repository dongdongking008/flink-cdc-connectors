package com.alibaba.ververica.cdc.connectors.mongodb.internal;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;

/**
 * The configuration properties.
 */
public class MongoDBCSConnectorConfig extends MongoDbConnectorConfig {

	public MongoDBCSConnectorConfig(Configuration config) {
		super(config);
	}

	public static Field getTaskId() {
		return MongoDbConnectorConfig.TASK_ID;
	}
}
