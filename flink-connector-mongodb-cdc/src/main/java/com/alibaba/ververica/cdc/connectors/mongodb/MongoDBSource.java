/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.mongodb;

import com.alibaba.ververica.cdc.connectors.mongodb.internal.MongoDBCSConnector;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder to build a SourceFunction which can read snapshot and continue to consume oplog.
 */
public class MongoDBSource {

	public static <T> Builder<T> builder() {
		return new Builder<>();
	}

	/**
	 * Builder class of {@link MongoDBSource}.
	 */
	public static class Builder<T> {

		private String hosts;
		private String[] databaseList;
		private String username;
		private String password;
		private String[] collectionList;
		private Properties dbzProperties;
		private DebeziumDeserializationSchema<T> deserializer;

		public Builder<T> hosts(String hosts) {
			this.hosts = hosts;
			return this;
		}

		/**
		 * An optional list of regular expressions that match database names to be monitored;
		 * any database name not included in the whitelist will be excluded from monitoring.
		 * By default all databases will be monitored.
		 */
		public Builder<T> databaseList(String... databaseList) {
			this.databaseList = databaseList;
			return this;
		}

		/**
		 * An optional list of regular expressions that match fully-qualified namespaces for MongoDB
		 * collections to be monitored; any collection not included in the whitelist is excluded from
		 * monitoring. Each identifier is of the form databaseName.collectionName.
		 * By default the connector will monitor all collections except those in the local and admin databases.
		 */
		public Builder<T> collectionList(String... collectionList) {
			this.collectionList = collectionList;
			return this;
		}

		/**
		 * Name of the database user to be used when connecting to MongoDB.
		 * This is required only when MongoDB is configured to use authentication.
		 */
		public Builder<T> username(String username) {
			this.username = username;
			return this;
		}

		/**
		 * Password to be used when connecting to MongoDB.
		 * This is required only when MongoDB is configured to use authentication.
		 */
		public Builder<T> password(String password) {
			this.password = password;
			return this;
		}

		/**
		 * The Debezium MongoDB connector properties. For example, "snapshot.mode".
		 */
		public Builder<T> debeziumProperties(Properties properties) {
			this.dbzProperties = properties;
			return this;
		}

		/**
		 * The deserializer used to convert from consumed {@link org.apache.kafka.connect.source.SourceRecord}.
		 */
		public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
			this.deserializer = deserializer;
			return this;
		}

		public DebeziumSourceFunction<T> build() {
			Properties props = new Properties();
			props.setProperty("connector.class", MongoDBCSConnector.class.getCanonicalName());
			// hard code server name, because we don't need to distinguish it, docs:
			// A unique name that identifies the connector and/or MongoDB replica set or sharded cluster
			// that this connector monitors. Each server should be monitored by at most one Debezium connector,
			// since this server name prefixes all persisted Kafka topics emanating from the MongoDB
			// replica set or cluster. Only alphanumeric characters and underscores should be used.
			props.setProperty("mongodb.name", "mongodb_change_stream_source");
			props.setProperty("mongodb.hosts", checkNotNull(hosts));
			if (username != null) {
				props.setProperty("mongodb.user", username);
				props.setProperty("mongodb.password", checkNotNull(password));
			}

			if (databaseList != null) {
				props.setProperty("database.whitelist", String.join(",", databaseList));
			}
			if (collectionList != null) {
				props.setProperty("collection.whitelist", String.join(",", collectionList));
			}

			if (dbzProperties != null) {
				dbzProperties.forEach(props::put);
			}

			return new DebeziumSourceFunction<>(
				deserializer,
				props);
		}
	}
}
