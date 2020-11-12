package com.alibaba.ververica.cdc.connectors.mongodb.utils;

/**
 * The constants for the names of the fields in the message envelope.
 */
public final class EnvelopeFieldName {
	/**
	 * The {@code patch} field is a string field that contains the JSON representation of the idempotent update operation.
	 */
	public static final String PATCH = "patch";
	/**
	 * The {@code filter} field is a string field that contains the JSON representation of the selection criteria for the update.
	 * The filter string can include multiple shard key fields for sharded collections.
	 */
	public static final String FILTER = "filter";
}
