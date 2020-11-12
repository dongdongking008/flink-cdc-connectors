package com.alibaba.ververica.cdc.connectors.mongodb.internal;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.mongodb.CollectionId;
import io.debezium.connector.mongodb.Module;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbErrorHandler;
import io.debezium.connector.mongodb.MongoDbEventMetadataProvider;
import io.debezium.connector.mongodb.MongoDbSchema;
import io.debezium.connector.mongodb.MongoDbTaskContext;
import io.debezium.connector.mongodb.ReplicaSets;
import io.debezium.connector.mongodb.metrics.MongoDbChangeEventSourceMetricsFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A Kafka Connect source task that replicates the changes from one or more MongoDB replica sets.
 *
 * <p>Generally, the {@link MongoDBCSConnector} assigns each replica set to a separate task, although multiple
 * replica sets will be assigned to each task when the maximum number of tasks is limited. Regardless, every task will use a
 * separate thread to replicate the contents of each replica set, and each replication thread may use multiple threads
 * to perform an initial sync of the replica set.
 *
 * @see MongoDBCSConnector
 * @see MongoDbConnectorConfig
 */
@ThreadSafe
public final class MongoDBCSConnectorTask extends BaseSourceTask {

	private static final String CONTEXT_NAME = "mongodb-connector-task";

	private final Logger logger = LoggerFactory.getLogger(getClass());

	// These are all effectively constants between start(...) and stop(...)
	private volatile ChangeEventQueue<DataChangeEvent> queue;
	private volatile String taskName;
	private volatile MongoDbTaskContext taskContext;
	private volatile ErrorHandler errorHandler;
	private volatile MongoDbSchema schema;

	@Override
	public String version() {
		return Module.version();
	}

	@Override
	public ChangeEventSourceCoordinator start(Configuration config) {
		final MongoDbConnectorConfig connectorConfig = new MongoDbConnectorConfig(config);
		final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create(logger);

		this.taskName = "task" + config.getInteger(MongoDBCSConnectorConfig.getTaskId());
		this.taskContext = new MongoDbTaskContext(config);

		final Schema structSchema = connectorConfig.getSourceInfoStructMaker().schema();
		this.schema = new MongoDbSchema(taskContext.filters(), taskContext.topicSelector(), structSchema);

		final ReplicaSets replicaSets = getReplicaSets(config);
		final MongoDBCSOffsetContext previousOffsets = getPreviousOffsets(connectorConfig, replicaSets);
		final Clock clock = Clock.system();

		LoggingContext.PreviousContext previousLogContext = taskContext.configureLoggingContext(taskName);

		try {

			this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
					.pollInterval(connectorConfig.getPollInterval())
					.maxBatchSize(connectorConfig.getMaxBatchSize())
					.maxQueueSize(connectorConfig.getMaxQueueSize())
					.loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
					.build();

			errorHandler = new MongoDbErrorHandler(connectorConfig.getLogicalName(), queue);

			final MongoDbEventMetadataProvider metadataProvider = new MongoDbEventMetadataProvider();

			final EventDispatcher<CollectionId> dispatcher = new EventDispatcher<>(
					connectorConfig,
					taskContext.topicSelector(),
					schema,
					queue,
					taskContext.filters().collectionFilter()::test,
					DataChangeEvent::new,
					metadataProvider,
					schemaNameAdjuster);

			ChangeEventSourceCoordinator coordinator = new ChangeEventSourceCoordinator(
					previousOffsets,
					errorHandler,
					MongoDBCSConnector.class,
					connectorConfig,
					new MongoDBCSChangeEventSourceFactory(
							connectorConfig,
							errorHandler,
							dispatcher,
							clock,
							replicaSets,
							taskContext),
					new MongoDbChangeEventSourceMetricsFactory(),
					dispatcher,
					schema);

			coordinator.start(taskContext, this.queue, metadataProvider);

			return coordinator;
		} finally {
			previousLogContext.restore();
		}
	}

	@Override
	public List<SourceRecord> doPoll() throws InterruptedException {
		List<DataChangeEvent> records = queue.poll();
		return records.stream().map(DataChangeEvent::getRecord).collect(Collectors.toList());
	}

	@Override
	public void doStop() {
		LoggingContext.PreviousContext previousLogContext = this.taskContext.configureLoggingContext(taskName);
		try {
			if (schema != null) {
				schema.close();
			}
		} finally {
			previousLogContext.restore();
		}
	}

	@Override
	protected Iterable<Field> getAllConfigurationFields() {
		return MongoDbConnectorConfig.ALL_FIELDS;
	}

	private MongoDBCSOffsetContext getPreviousOffsets(MongoDbConnectorConfig connectorConfig, ReplicaSets replicaSets) {
		MongoDBCSOffsetContext.Loader loader = new MongoDBCSOffsetContext.Loader(connectorConfig, replicaSets);
		Collection<Map<String, String>> partitions = loader.getPartitions();

		Map<Map<String, String>, Map<String, Object>> offsets = context.offsetStorageReader().offsets(partitions);
		if (offsets != null && !offsets.values().stream().filter(Objects::nonNull).collect(Collectors.toList()).isEmpty()) {
			MongoDBCSOffsetContext offsetContext = loader.loadOffsets(offsets);
			logger.info("Found previous offsets {}", offsetContext);
			return offsetContext;
		} else {
			return null;
		}
	}

	private ReplicaSets getReplicaSets(Configuration config) {
		final String hosts = config.getString(MongoDbConnectorConfig.HOSTS);
		final ReplicaSets replicaSets = ReplicaSets.parse(hosts);
		if (replicaSets.validReplicaSetCount() == 0) {
			throw new ConnectException("Unable to start MongoDB connector task since no replica sets were found at " + hosts);
		}
		return replicaSets;
	}
}
