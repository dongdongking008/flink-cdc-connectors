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

package com.alibaba.ververica.cdc.connectors.mongodb.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import com.alibaba.ververica.cdc.debezium.table.DebeziumOptions;

import java.util.HashSet;
import java.util.Set;

import static com.alibaba.ververica.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;

/**
 * Factory for creating configured instance of {@link MongoDBTableSource}.
 */
public class MongoDBTableSourceFactory implements DynamicTableSourceFactory {

	private static final String IDENTIFIER = "mongodb-cdc";

	private static final ConfigOption<String> HOSTS = ConfigOptions.key("hosts")
		.stringType()
		.noDefaultValue()
		.withDescription("The comma-separated list of hostname and port pairs " +
				"(in the form 'host' or 'host:port') of the MongoDB servers in the replica set. The " +
				"list can contain a single hostname and port pair. If mongodb.members.auto.discover " +
				"is set to false, then the host and port pair should be prefixed with the replica set name (e.g., rs0/localhost:27017).");

	private static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
		.stringType()
		.noDefaultValue()
		.withDescription("Name of the database user to be used when connecting to MongoDB. " +
				"This is required only when MongoDB is configured to use authentication.");

	private static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
		.stringType()
		.noDefaultValue()
		.withDescription("Password to be used when connecting to MongoDB. " +
				"This is required only when MongoDB is configured to use authentication.");

	private static final ConfigOption<String> DATABASE_NAME = ConfigOptions.key("database-name")
		.stringType()
		.noDefaultValue()
		.withDescription("Database name of the Mongo server to monitor.");

	private static final ConfigOption<String> COLLECTION_NAME = ConfigOptions.key("collection-name")
		.stringType()
		.noDefaultValue()
		.withDescription("Collection name of the Mongo database to monitor.");

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		helper.validateExcept(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX);

		final ReadableConfig config = helper.getOptions();
		String hosts = config.get(HOSTS);
		String username = config.get(USERNAME);
		String password = config.get(PASSWORD);
		String databaseName = config.get(DATABASE_NAME);
		String collectionName = config.get(COLLECTION_NAME);
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

		return new MongoDBTableSource(
			physicalSchema,
			hosts,
			databaseName,
			collectionName,
			username,
			password,
			getDebeziumProperties(context.getCatalogTable().getOptions())
		);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(HOSTS);
		options.add(DATABASE_NAME);
		options.add(COLLECTION_NAME);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(USERNAME);
		options.add(PASSWORD);
		return options;
	}
}
