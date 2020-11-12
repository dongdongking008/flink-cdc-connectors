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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.alibaba.ververica.cdc.connectors.mongodb.MongoDBSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;

import java.time.ZoneId;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a MongoDB oplog source from a logical
 * description.
 */
public class MongoDBTableSource implements ScanTableSource {

	private final TableSchema physicalSchema;
	private final String hosts;
	private final String database;
	private final String username;
	private final String password;
	private final String collectionName;
	private final Properties dbzProperties;

	public MongoDBTableSource(
			TableSchema physicalSchema,
			String hosts,
			String database,
			String collectionName,
			String username,
			String password,
			Properties dbzProperties) {
		this.physicalSchema = physicalSchema;
		this.hosts = checkNotNull(hosts);
		this.database = checkNotNull(database);
		this.collectionName = checkNotNull(collectionName);
		this.username = username;
		this.password = password;
		this.dbzProperties = dbzProperties;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.newBuilder()
			.addContainedKind(RowKind.UPDATE_AFTER)
			.addContainedKind(RowKind.DELETE)
			.build();
	}

	@SuppressWarnings("unchecked")
	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
		RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
		TypeInformation<RowData> typeInfo = scanContext.createTypeInformation(physicalSchema.toRowDataType());
		DebeziumDeserializationSchema<RowData> deserializer = new RowDataDebeziumDeserializeSchema(
			rowType,
			typeInfo,
			((rowData, rowKind) -> {}),
			ZoneId.of("UTC"));
		MongoDBSource.Builder<RowData> builder = MongoDBSource.<RowData>builder()
			.hosts(hosts)
			.databaseList(database)
			.collectionList(database + "." + collectionName)
			.username(username)
			.password(password)
			.debeziumProperties(dbzProperties)
			.deserializer(deserializer);
		DebeziumSourceFunction<RowData> sourceFunction = builder.build();

		return SourceFunctionProvider.of(sourceFunction, false);
	}

	@Override
	public DynamicTableSource copy() {
		return new MongoDBTableSource(
			physicalSchema,
			hosts,
			database,
			collectionName,
			username,
			password,
			dbzProperties
		);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		MongoDBTableSource that = (MongoDBTableSource) o;
		return Objects.equals(physicalSchema, that.physicalSchema) &&
			Objects.equals(hosts, that.hosts) &&
			Objects.equals(database, that.database) &&
			Objects.equals(username, that.username) &&
			Objects.equals(password, that.password) &&
			Objects.equals(collectionName, that.collectionName) &&
			Objects.equals(dbzProperties, that.dbzProperties);
	}

	@Override
	public int hashCode() {
		return Objects.hash(physicalSchema, hosts, database, username, password, collectionName, dbzProperties);
	}

	@Override
	public String asSummaryString() {
		return "MongoDB-CDC";
	}
}
