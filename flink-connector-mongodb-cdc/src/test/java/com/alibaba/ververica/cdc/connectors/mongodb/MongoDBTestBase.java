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

import org.apache.flink.test.util.AbstractTestBase;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;

/**
 * Basic class for testing PostgresSQL source, this contains a PostgreSQL container which enables binlog.
 */
public abstract class MongoDBTestBase extends AbstractTestBase {
	private static final Logger LOG = LoggerFactory.getLogger(MongoDBTestBase.class);
	private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");


	protected static final MongoDBContainer MONGODB_CONTAINER = new MongoDBContainer("mongo:4.0.10")
//		.withFileSystemBind(MongoDBTestBase.class.getClassLoader().getResource(".dbshell").getPath(),
//				"/home/mongodb/.dbshell", BindMode.READ_WRITE)
		.withLogConsumer(new Slf4jLogConsumer(LOG));

	@BeforeClass
	public static void startContainers() {

//		MONGODB_CONTAINER.addEnv("MONGO_INITDB_ROOT_USERNAME", "mongoadmin");
//		MONGODB_CONTAINER.addEnv("MONGO_INITDB_ROOT_PASSWORD", "mongo");

		LOG.info("Starting containers...");
		Startables.deepStart(Stream.of(MONGODB_CONTAINER)).join();
		LOG.info("Containers are started.");
	}

	protected String getReplicaSetUrl() {
		return MONGODB_CONTAINER.getReplicaSetUrl();
	}

	protected String getReplicaSetHosts() {
		if (!MONGODB_CONTAINER.isRunning()) {
			throw new IllegalStateException("MongoDBContainer should be started first");
		}
		return String.format(
				"%s/%s:%d",
				"docker-rs",
				MONGODB_CONTAINER.getContainerIpAddress(),
				MONGODB_CONTAINER.getMappedPort(27017)
		);
	}

	/**
	 * Import JSON into a MongoDB database.
	 */
	protected void initializeMongoDBTable(String databaseName) {
		final String ddlFile = String.format("ddl/%s.json", databaseName);
		final URL ddlTestFile = MongoDBTestBase.class.getClassLoader().getResource(ddlFile);
		assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
		try (MongoClient client = MongoClients.create(getReplicaSetUrl())) {
			MongoDatabase db = client.getDatabase(databaseName);
			db.drop();
			Document document = Document.parse(
					new String(Files.readAllBytes(Paths.get(ddlTestFile.toURI()))));
			document.forEach((key, value) ->
				db.getCollection(key).insertMany(document.getList(key, Document.class))
			);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
