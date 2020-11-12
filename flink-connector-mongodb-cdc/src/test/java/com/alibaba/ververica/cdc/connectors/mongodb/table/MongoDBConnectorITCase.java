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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import com.alibaba.ververica.cdc.connectors.mongodb.MongoDBTestBase;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.BsonDecimal128;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Integration tests for MySQL binlog SQL source.
 */
public class MongoDBConnectorITCase extends MongoDBTestBase {

	private static final Logger LOG = LoggerFactory.getLogger(MongoDBConnectorITCase.class);

	private final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	private final StreamTableEnvironment tEnv = StreamTableEnvironment.create(
		env,
		EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
	);

	@Before
	public void before() {
		TestValuesTableFactory.clearAllData();
		env.setParallelism(1);
	}

	@Test
	public void testConsumingAllEvents() throws SQLException, ExecutionException, InterruptedException {
		initializeMongoDBTable("inventory");
		String sourceDDL = String.format(
				"CREATE TABLE debezium_source (" +
				" _id INT NOT NULL," +
				" name STRING," +
				" description STRING," +
				" weight DECIMAL(10,3)," +
				" PRIMARY KEY (_id) NOT ENFORCED" +
				") WITH (" +
				" 'connector' = 'mongodb-cdc'," +
				" 'debezium.mongodb.members.auto.discover' = 'false'," +
				" 'hosts' = '%s'," +
				" 'database-name' = '%s'," +
				" 'collection-name' = '%s'" +
				")",
			getReplicaSetHosts(),
			"inventory",
			"products");
		String sinkDDL = "CREATE TABLE sink (" +
			" name STRING," +
			" weightSum DECIMAL(10,3)," +
			" PRIMARY KEY (name) NOT ENFORCED" +
			") WITH (" +
			" 'connector' = 'values'," +
			" 'sink-insert-only' = 'false'," +
			" 'sink-expected-messages-num' = '18'" +
			")";
		tEnv.executeSql(sourceDDL);
		tEnv.executeSql(sinkDDL);

		// async submit job
		TableResult result = tEnv.executeSql("INSERT INTO sink SELECT name, SUM(weight) FROM debezium_source GROUP BY name");

		waitForSnapshotStarted("sink");

		try (MongoClient client = MongoClients.create(getReplicaSetUrl())) {
			MongoCollection<Document> collection = client.getDatabase("inventory")
					.getCollection("products");

			collection.updateOne(Filters.eq("_id", 106),
					Updates.set("description", "18oz carpenter hammer"));
			collection.updateOne(Filters.eq("_id", 107),
					Updates.set("weight", 5.1));
			collection.insertOne(Document.parse("{ \"_id\": 110, \"name\": \"jacket\", \"description\": \"water resistent white wind breaker\", \"weight\": 0.2 }"));
			collection.insertOne(Document.parse("{ \"_id\": 111, \"name\": \"scooter\", \"description\": \"Big 2-wheel scooter \", \"weight\": 5.18 }"));
			collection.updateOne(Filters.eq("_id", 110),
					Updates.combine(Updates.set("description", "new water resistent white wind breaker"),
							Updates.set("weight", 0.5)));
			collection.updateOne(Filters.eq("_id", 111),
					Updates.set("weight", 5.17));
			collection.deleteOne(Filters.eq("_id", 111));
		}

		waitForSinkSize("sink", 18);

		// The final database table looks like this:
		//
		// > SELECT * FROM inventory.products;
		// +-----+--------------------+---------------------------------------------------------+--------+
		// | id  | name               | description                                             | weight |
		// +-----+--------------------+---------------------------------------------------------+--------+
		// | 101 | scooter            | Small 2-wheel scooter                                   |   3.14 |
		// | 102 | car battery        | 12V car battery                                         |    8.1 |
		// | 103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 |    0.8 |
		// | 104 | hammer             | 12oz carpenter's hammer                                 |   0.75 |
		// | 105 | hammer             | 14oz carpenter's hammer                                 |  0.875 |
		// | 106 | hammer             | 18oz carpenter hammer                                   |      1 |
		// | 107 | rocks              | box of assorted rocks                                   |    5.1 |
		// | 108 | jacket             | water resistent black wind breaker                      |    0.1 |
		// | 109 | spare tire         | 24 inch spare tire                                      |   22.2 |
		// | 110 | jacket             | new water resistent white wind breaker                  |    0.5 |
		// +-----+--------------------+---------------------------------------------------------+--------+

		String[] expected = new String[]{
			"scooter,3.140", "car battery,8.100", "12-pack drill bits,0.800",
			"hammer,2.625", "rocks,5.100", "jacket,0.600", "spare tire,22.200"};

		List<String> actual = TestValuesTableFactory.getResults("sink");
		assertThat(actual, containsInAnyOrder(expected));

		result.getJobClient().get().cancel().get();
	}

	@Test
	public void testAllTypes() throws Throwable {
		ObjectId objId = ObjectId.get();
		initializeAllTypesTable(objId);

		String sourceDDL = String.format(
				"CREATE TABLE full_types (\n" +
				"    _id STRING NOT NULL,\n" +
				"    bytea_c BYTES,\n" +
				"    int_c INTEGER,\n" +
				"    big_c BIGINT,\n" +
				"    double_precision DOUBLE,\n" +
				"    decimal_c DECIMAL(10, 1),\n" +
				"    boolean_c BOOLEAN,\n" +
				"    text_c STRING,\n" +
				"    timestamp3_c TIMESTAMP(3),\n" +
				"    date_c DATE,\n" +
				"    time_c TIME(0),\n" +
				"    default_numeric_c DECIMAL,\n" +
				"    int_array_c ARRAY<INTEGER>,\n" +
				"    bigint_array_c ARRAY<BIGINT>,\n" +
				"    double_array_c ARRAY<DOUBLE>,\n" +
				"    decimal_array_c ARRAY<DECIMAL(10, 1)>,\n" +
				"    boolean_array_c ARRAY<BOOLEAN>,\n" +
				"    text_array_c ARRAY<STRING>,\n" +
				"    timestam3_array_c ARRAY<TIMESTAMP(3)>,\n" +
				"    date_array_c ARRAY<DATE>,\n" +
				"    time_array_c ARRAY<TIME(0)>,\n" +
				"    num_array_c ARRAY<DECIMAL>,\n" +
				"    row_c ROW<\n" +
				"       bytea_c BYTES,\n" +
				"       int_c INTEGER,\n" +
				"       big_c BIGINT,\n" +
				"       double_precision DOUBLE,\n" +
				"       decimal_c DECIMAL(10, 1),\n" +
				"       boolean_c BOOLEAN,\n" +
				"       text_c STRING,\n" +
				"       timestamp3_c TIMESTAMP(3),\n" +
				"       date_c DATE,\n" +
				"       time_c TIME(0),\n" +
				"       default_numeric_c DECIMAL,\n" +
				"       int_array_c ARRAY<INTEGER>,\n" +
				"       bigint_array_c ARRAY<BIGINT>,\n" +
				"       double_array_c ARRAY<DOUBLE>,\n" +
				"       decimal_array_c ARRAY<DECIMAL(10, 1)>,\n" +
				"       boolean_array_c ARRAY<BOOLEAN>,\n" +
				"       text_array_c ARRAY<STRING>,\n" +
				"       timestam3_array_c ARRAY<TIMESTAMP(3)>,\n" +
				"       date_array_c ARRAY<DATE>,\n" +
				"       time_array_c ARRAY<TIME(0)>,\n" +
				"       num_array_c ARRAY<DECIMAL>,\n" +
				"       array_c ARRAY<ROW<\n" +
				"          bytea_c BYTES,\n" +
				"          int_c INTEGER,\n" +
				"          big_c BIGINT,\n" +
				"          double_precision DOUBLE,\n" +
				"          decimal_c DECIMAL(10, 1),\n" +
				"          boolean_c BOOLEAN,\n" +
				"          text_c STRING,\n" +
				"          timestamp3_c TIMESTAMP(3),\n" +
				"          date_c DATE,\n" +
				"          time_c TIME(0),\n" +
				"          default_numeric_c DECIMAL,\n" +
				"          int_array_c ARRAY<INTEGER>,\n" +
				"          bigint_array_c ARRAY<BIGINT>,\n" +
				"          double_array_c ARRAY<DOUBLE>,\n" +
				"          decimal_array_c ARRAY<DECIMAL(10, 1)>,\n" +
				"          boolean_array_c ARRAY<BOOLEAN>,\n" +
				"          text_array_c ARRAY<STRING>,\n" +
				"          timestam3_array_c ARRAY<TIMESTAMP(3)>,\n" +
				"          date_array_c ARRAY<DATE>,\n" +
				"          time_array_c ARRAY<TIME(0)>,\n" +
				"          num_array_c ARRAY<DECIMAL>\n" +
				"       >>\n" +
				"    >,\n" +
				"    array_c ARRAY<ROW<\n" +
				"       bytea_c BYTES,\n" +
				"       int_c INTEGER,\n" +
				"       big_c BIGINT,\n" +
				"       double_precision DOUBLE,\n" +
				"       decimal_c DECIMAL(10, 1),\n" +
				"       boolean_c BOOLEAN,\n" +
				"       text_c STRING,\n" +
				"       timestamp3_c TIMESTAMP(3),\n" +
				"       date_c DATE,\n" +
				"       time_c TIME(0),\n" +
				"       default_numeric_c DECIMAL,\n" +
				"       int_array_c ARRAY<INTEGER>,\n" +
				"       bigint_array_c ARRAY<BIGINT>,\n" +
				"       double_array_c ARRAY<DOUBLE>,\n" +
				"       decimal_array_c ARRAY<DECIMAL(10, 1)>,\n" +
				"       boolean_array_c ARRAY<BOOLEAN>,\n" +
				"       text_array_c ARRAY<STRING>,\n" +
				"       timestam3_array_c ARRAY<TIMESTAMP(3)>,\n" +
				"       date_array_c ARRAY<DATE>,\n" +
				"       time_array_c ARRAY<TIME(0)>,\n" +
				"       num_array_c ARRAY<DECIMAL>,\n" +
				"       row_c ROW<\n" +
				"          bytea_c BYTES,\n" +
				"          int_c INTEGER,\n" +
				"          big_c BIGINT,\n" +
				"          double_precision DOUBLE,\n" +
				"          decimal_c DECIMAL(10, 1),\n" +
				"          boolean_c BOOLEAN,\n" +
				"          text_c STRING,\n" +
				"          timestamp3_c TIMESTAMP(3),\n" +
				"          date_c DATE,\n" +
				"          time_c TIME(0),\n" +
				"          default_numeric_c DECIMAL,\n" +
				"          int_array_c ARRAY<INTEGER>,\n" +
				"          bigint_array_c ARRAY<BIGINT>,\n" +
				"          double_array_c ARRAY<DOUBLE>,\n" +
				"          decimal_array_c ARRAY<DECIMAL(10, 1)>,\n" +
				"          boolean_array_c ARRAY<BOOLEAN>,\n" +
				"          text_array_c ARRAY<STRING>,\n" +
				"          timestam3_array_c ARRAY<TIMESTAMP(3)>,\n" +
				"          date_array_c ARRAY<DATE>,\n" +
				"          time_array_c ARRAY<TIME(0)>,\n" +
				"          num_array_c ARRAY<DECIMAL>,\n" +
				"          array_c ARRAY<ROW<\n" +
				"             bytea_c BYTES,\n" +
				"             int_c INTEGER,\n" +
				"             big_c BIGINT,\n" +
				"             double_precision DOUBLE,\n" +
				"             decimal_c DECIMAL(10, 1),\n" +
				"             boolean_c BOOLEAN,\n" +
				"             text_c STRING,\n" +
				"             timestamp3_c TIMESTAMP(3),\n" +
				"             date_c DATE,\n" +
				"             time_c TIME(0),\n" +
				"             default_numeric_c DECIMAL,\n" +
				"             int_array_c ARRAY<INTEGER>,\n" +
				"             bigint_array_c ARRAY<BIGINT>,\n" +
				"             double_array_c ARRAY<DOUBLE>,\n" +
				"             decimal_array_c ARRAY<DECIMAL(10, 1)>,\n" +
				"             boolean_array_c ARRAY<BOOLEAN>,\n" +
				"             text_array_c ARRAY<STRING>,\n" +
				"             timestam3_array_c ARRAY<TIMESTAMP(3)>,\n" +
				"             date_array_c ARRAY<DATE>,\n" +
				"             time_array_c ARRAY<TIME(0)>,\n" +
				"             num_array_c ARRAY<DECIMAL>\n" +
				"          >>\n" +
				"       >\n" +
				"    >>,\n" +
				" PRIMARY KEY (_id) NOT ENFORCED\n" +
				") WITH (" +
						" 'connector' = 'mongodb-cdc'," +
						" 'hosts' = '%s'," +
						" 'debezium.mongodb.members.auto.discover' = 'false'," +
						" 'database-name' = '%s'," +
						" 'collection-name' = '%s'" +
						")",
				getReplicaSetHosts(),
				"public",
				"full_types");
		String sinkDDL =
				"CREATE TABLE sink (\n" +
				"    _id STRING NOT NULL,\n" +
				"    bytea_c BYTES,\n" +
				"    int_c INTEGER,\n" +
				"    big_c BIGINT,\n" +
				"    double_precision DOUBLE,\n" +
				"    decimal_c DECIMAL(10, 1),\n" +
				"    boolean_c BOOLEAN,\n" +
				"    text_c STRING,\n" +
				"    timestamp3_c TIMESTAMP(3),\n" +
				"    date_c DATE,\n" +
				"    time_c TIME(0),\n" +
				"    default_numeric_c DECIMAL,\n" +
				"    int_array_c ARRAY<INTEGER>,\n" +
				"    bigint_array_c ARRAY<BIGINT>,\n" +
				"    double_array_c ARRAY<DOUBLE>,\n" +
				"    decimal_array_c ARRAY<DECIMAL(10, 1)>,\n" +
				"    boolean_array_c ARRAY<BOOLEAN>,\n" +
				"    text_array_c ARRAY<STRING>,\n" +
				"    timestam3_array_c ARRAY<TIMESTAMP(3)>,\n" +
				"    date_array_c ARRAY<DATE>,\n" +
				"    time_array_c ARRAY<TIME(0)>,\n" +
				"    num_array_c ARRAY<DECIMAL>,\n" +
				"    row_c ROW<\n" +
				"       bytea_c BYTES,\n" +
				"       int_c INTEGER,\n" +
				"       big_c BIGINT,\n" +
				"       double_precision DOUBLE,\n" +
				"       decimal_c DECIMAL(10, 1),\n" +
				"       boolean_c BOOLEAN,\n" +
				"       text_c STRING,\n" +
				"       timestamp3_c TIMESTAMP(3),\n" +
				"       date_c DATE,\n" +
				"       time_c TIME(0),\n" +
				"       default_numeric_c DECIMAL,\n" +
				"       int_array_c ARRAY<INTEGER>,\n" +
				"       bigint_array_c ARRAY<BIGINT>,\n" +
				"       double_array_c ARRAY<DOUBLE>,\n" +
				"       decimal_array_c ARRAY<DECIMAL(10, 1)>,\n" +
				"       boolean_array_c ARRAY<BOOLEAN>,\n" +
				"       text_array_c ARRAY<STRING>,\n" +
				"       timestam3_array_c ARRAY<TIMESTAMP(3)>,\n" +
				"       date_array_c ARRAY<DATE>,\n" +
				"       time_array_c ARRAY<TIME(0)>,\n" +
				"       num_array_c ARRAY<DECIMAL>,\n" +
				"       array_c ARRAY<ROW<\n" +
				"          bytea_c BYTES,\n" +
				"          int_c INTEGER,\n" +
				"          big_c BIGINT,\n" +
				"          double_precision DOUBLE,\n" +
				"          decimal_c DECIMAL(10, 1),\n" +
				"          boolean_c BOOLEAN,\n" +
				"          text_c STRING,\n" +
				"          timestamp3_c TIMESTAMP(3),\n" +
				"          date_c DATE,\n" +
				"          time_c TIME(0),\n" +
				"          default_numeric_c DECIMAL,\n" +
				"          int_array_c ARRAY<INTEGER>,\n" +
				"          bigint_array_c ARRAY<BIGINT>,\n" +
				"          double_array_c ARRAY<DOUBLE>,\n" +
				"          decimal_array_c ARRAY<DECIMAL(10, 1)>,\n" +
				"          boolean_array_c ARRAY<BOOLEAN>,\n" +
				"          text_array_c ARRAY<STRING>,\n" +
				"          timestam3_array_c ARRAY<TIMESTAMP(3)>,\n" +
				"          date_array_c ARRAY<DATE>,\n" +
				"          time_array_c ARRAY<TIME(0)>,\n" +
				"          num_array_c ARRAY<DECIMAL>\n" +
				"       >>\n" +
				"    >,\n" +
				"    array_c ARRAY<ROW<\n" +
				"       bytea_c BYTES,\n" +
				"       int_c INTEGER,\n" +
				"       big_c BIGINT,\n" +
				"       double_precision DOUBLE,\n" +
				"       decimal_c DECIMAL(10, 1),\n" +
				"       boolean_c BOOLEAN,\n" +
				"       text_c STRING,\n" +
				"       timestamp3_c TIMESTAMP(3),\n" +
				"       date_c DATE,\n" +
				"       time_c TIME(0),\n" +
				"       default_numeric_c DECIMAL,\n" +
				"       int_array_c ARRAY<INTEGER>,\n" +
				"       bigint_array_c ARRAY<BIGINT>,\n" +
				"       double_array_c ARRAY<DOUBLE>,\n" +
				"       decimal_array_c ARRAY<DECIMAL(10, 1)>,\n" +
				"       boolean_array_c ARRAY<BOOLEAN>,\n" +
				"       text_array_c ARRAY<STRING>,\n" +
				"       timestam3_array_c ARRAY<TIMESTAMP(3)>,\n" +
				"       date_array_c ARRAY<DATE>,\n" +
				"       time_array_c ARRAY<TIME(0)>,\n" +
				"       num_array_c ARRAY<DECIMAL>,\n" +
				"       row_c ROW<\n" +
				"          bytea_c BYTES,\n" +
				"          int_c INTEGER,\n" +
				"          big_c BIGINT,\n" +
				"          double_precision DOUBLE,\n" +
				"          decimal_c DECIMAL(10, 1),\n" +
				"          boolean_c BOOLEAN,\n" +
				"          text_c STRING,\n" +
				"          timestamp3_c TIMESTAMP(3),\n" +
				"          date_c DATE,\n" +
				"          time_c TIME(0),\n" +
				"          default_numeric_c DECIMAL,\n" +
				"          int_array_c ARRAY<INTEGER>,\n" +
				"          bigint_array_c ARRAY<BIGINT>,\n" +
				"          double_array_c ARRAY<DOUBLE>,\n" +
				"          decimal_array_c ARRAY<DECIMAL(10, 1)>,\n" +
				"          boolean_array_c ARRAY<BOOLEAN>,\n" +
				"          text_array_c ARRAY<STRING>,\n" +
				"          timestam3_array_c ARRAY<TIMESTAMP(3)>,\n" +
				"          date_array_c ARRAY<DATE>,\n" +
				"          time_array_c ARRAY<TIME(0)>,\n" +
				"          num_array_c ARRAY<DECIMAL>,\n" +
				"          array_c ARRAY<ROW<\n" +
				"             bytea_c BYTES,\n" +
				"             int_c INTEGER,\n" +
				"             big_c BIGINT,\n" +
				"             double_precision DOUBLE,\n" +
				"             decimal_c DECIMAL(10, 1),\n" +
				"             boolean_c BOOLEAN,\n" +
				"             text_c STRING,\n" +
				"             timestamp3_c TIMESTAMP(3),\n" +
				"             date_c DATE,\n" +
				"             time_c TIME(0),\n" +
				"             default_numeric_c DECIMAL,\n" +
				"             int_array_c ARRAY<INTEGER>,\n" +
				"             bigint_array_c ARRAY<BIGINT>,\n" +
				"             double_array_c ARRAY<DOUBLE>,\n" +
				"             decimal_array_c ARRAY<DECIMAL(10, 1)>,\n" +
				"             boolean_array_c ARRAY<BOOLEAN>,\n" +
				"             text_array_c ARRAY<STRING>,\n" +
				"             timestam3_array_c ARRAY<TIMESTAMP(3)>,\n" +
				"             date_array_c ARRAY<DATE>,\n" +
				"             time_array_c ARRAY<TIME(0)>,\n" +
				"             num_array_c ARRAY<DECIMAL>\n" +
				"          >>\n" +
				"       >\n" +
				"    >>\n" +
				") WITH (" +
				" 'connector' = 'values'," +
				" 'sink-insert-only' = 'false'" +
				")";
		tEnv.executeSql(sourceDDL);
		tEnv.executeSql(sinkDDL);

		// async submit job
		TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM full_types");

		waitForSnapshotStarted("sink");

		try (MongoClient client = MongoClients.create(getReplicaSetUrl())) {
			MongoCollection<Document> collection = client.getDatabase("public")
					.getCollection("full_types");
			collection.updateOne(Filters.eq("_id", objId),
					Updates.combine(
							Updates.set("int_c", 0),
							Updates.unset("row_c"),
							Updates.push("array_c.0.int_array_c", 333)
							));
		}

		waitForSinkSize("sink", 4);

		List<String> expected = Arrays.asList(
			"+I(" + objId.toString() + ",[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]]," +
					"[[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]]])",
			"-U(" + objId.toString() + ",[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]]," +
					"[[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]]])",
			"+U(" + objId.toString() + ",[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]]," +
					"[[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
						"[[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]]])",
			"-U(" + objId.toString() + ",[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]]," +
					"[[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]]])",
			"+U(" + objId.toString() + ",[50],0,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"null," +
					//"[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					//"[[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]]," +
					"[[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535, 333],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]," +
					"[[50],65535,2147483647,6.6,404.4,true,Hello World,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,[65535],[2147483647],[6.6],[404.4],[true],[Hello World],[2020-07-17T18:00:22.123],[2020-07-17],[18:00:22],[500]]])"
		);
		List<String> actual = TestValuesTableFactory.getRawResults("sink");
		assertEquals(expected, actual);

		result.getJobClient().get().cancel().get();
	}

	private void initializeAllTypesTable(ObjectId objId) {
		try (MongoClient client = MongoClients.create(getReplicaSetUrl())) {
			MongoDatabase db = client.getDatabase("public");
			db.drop();
			db.getCollection("full_types").insertOne(
					appendAllTypes(new Document())
					.append("_id", objId)
					.append("row_c", appendAllTypes(new Document())
							.append("array_c", Arrays.asList(appendAllTypes(new Document())))
					)
					.append("array_c", Arrays.asList(appendAllTypes(new Document())
							.append("row_c", appendAllTypes(new Document())
									.append("array_c", Arrays.asList(appendAllTypes(new Document())))
							)
					))
			);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static Document appendAllTypes(Document doc) {
		return doc.append("bytea_c", new Binary(new byte[]{ '2' }))
			.append("int_c", 65535)
			.append("big_c", 2147483647L)
			.append("double_precision", 6.6D)
			.append("decimal_c", new BsonDecimal128(Decimal128.parse("404.4443")))
			.append("boolean_c", true)
			.append("text_c", "Hello World")
			.append("timestamp3_c", LocalDateTime.parse("2020-07-17 18:00:22.123", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")))
			.append("date_c", LocalDate.parse("2020-07-17", DateTimeFormatter.ofPattern("yyyy-MM-dd")))
			.append("time_c", LocalTime.parse("18:00:22", DateTimeFormatter.ofPattern("HH:mm:ss")))
			.append("default_numeric_c", 500)
			.append("int_array_c", Arrays.asList(65535))
			.append("bigint_array_c", Arrays.asList(2147483647L))
			.append("double_array_c", Arrays.asList(6.6D))
			.append("decimal_array_c", Arrays.asList(new BsonDecimal128(Decimal128.parse("404.4443"))))
			.append("boolean_array_c", Arrays.asList(true))
			.append("text_array_c", Arrays.asList("Hello World"))
			.append("timestam3_array_c", Arrays.asList(LocalDateTime.parse("2020-07-17 18:00:22.123", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))))
			.append("date_array_c", Arrays.asList(LocalDate.parse("2020-07-17", DateTimeFormatter.ofPattern("yyyy-MM-dd"))))
			.append("time_array_c", Arrays.asList(LocalTime.parse("18:00:22", DateTimeFormatter.ofPattern("HH:mm:ss"))))
			.append("num_array_c", Arrays.asList(500));
	}

	private static void waitForSnapshotStarted(String sinkName) throws InterruptedException {
		while (sinkSize(sinkName) == 0) {
			Thread.sleep(100);
		}
	}

	private static void waitForSinkSize(String sinkName, int expectedSize) throws InterruptedException {
		while (sinkSize(sinkName) < expectedSize) {
			LOG.info("sinkSize: {}", sinkSize(sinkName));
			Thread.sleep(100);
		}
	}

	private static int sinkSize(String sinkName) {
		synchronized (TestValuesTableFactory.class) {
			try {
				return TestValuesTableFactory.getRawResults(sinkName).size();
			} catch (IllegalArgumentException e) {
				// job is not started yet
				return 0;
			}
		}
	}

}
