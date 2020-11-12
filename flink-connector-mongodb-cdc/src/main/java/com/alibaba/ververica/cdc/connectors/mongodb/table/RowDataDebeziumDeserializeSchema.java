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
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.Envelope;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

/**
 * Deserialization schema from Debezium object to Flink Table/SQL internal data structure {@link RowData}.
 */
public final class RowDataDebeziumDeserializeSchema implements DebeziumDeserializationSchema<RowData> {
	private static final long serialVersionUID = -4952184966051743776L;

	/**
	 * Custom validator to validate the row value.
	 */
	public interface ValueValidator extends Serializable {
		void validate(RowData rowData, RowKind rowKind) throws Exception;
	}

	/** TypeInformation of the produced {@link RowData}. **/
	private final TypeInformation<RowData> resultTypeInfo;

	/**
	 * Runtime converter that converts {@link JsonNode}s into
	 * objects of Flink SQL internal data structures. **/
	private final DeserializationRuntimeConverter runtimeConverter;

	/**
	 * Time zone of the database server.
	 */
	private final ZoneId serverTimeZone;

	/**
	 * Validator to validate the row value.
	 */
	private final ValueValidator validator;

	public RowDataDebeziumDeserializeSchema(RowType rowType, TypeInformation<RowData> resultTypeInfo, ValueValidator validator, ZoneId serverTimeZone) {
		this.runtimeConverter = createConverter(rowType);
		this.resultTypeInfo = resultTypeInfo;
		this.validator = validator;
		this.serverTimeZone = serverTimeZone;
	}

	@Override
	public void deserialize(SourceRecord record, Collector<RowData> out) throws Exception {
		Envelope.Operation op = Envelope.operationFor(record);
		Struct value = (Struct) record.value();
		Schema valueSchema = record.valueSchema();
		if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
			GenericRowData insert = extractAfterRow(value, valueSchema);
			validator.validate(insert, RowKind.UPDATE_AFTER);
			insert.setRowKind(RowKind.UPDATE_AFTER);
			out.collect(insert);
		} else if (op == Envelope.Operation.DELETE) {
			GenericRowData delete = extractDeleteRow(value, valueSchema);
			validator.validate(delete, RowKind.DELETE);
			delete.setRowKind(RowKind.DELETE);
			out.collect(delete);
		} else {
			GenericRowData after = extractAfterRow(value, valueSchema);
			validator.validate(after, RowKind.UPDATE_AFTER);
			after.setRowKind(RowKind.UPDATE_AFTER);
			out.collect(after);
		}
	}

	private GenericRowData extractAfterRow(Struct value, Schema valueSchema) throws Exception {
		String after = value.getString(Envelope.FieldName.AFTER);
		return (GenericRowData) runtimeConverter.convert(after);
	}

	private GenericRowData extractDeleteRow(Struct value, Schema valueSchema) throws Exception {
		String filter = value.getString("filter");
		return (GenericRowData) runtimeConverter.convert(filter);
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return resultTypeInfo;
	}

	// -------------------------------------------------------------------------------------
	// Runtime Converters
	// -------------------------------------------------------------------------------------

	/**
	 * Runtime converter that converts objects of Debezium into objects of Flink Table & SQL internal data structures.
	 */
	@FunctionalInterface
	private interface DeserializationRuntimeConverter extends Serializable {
		Object convert(Object dbzObj) throws Exception;
	}

	/**
	 * Creates a runtime converter which is null safe.
	 */
	private DeserializationRuntimeConverter createConverter(LogicalType type) {
		return wrapIntoNullableConverter(createNotNullConverter(type));
	}

	/**
	 * Creates a runtime converter which assuming input object is not null.
	 */
	private DeserializationRuntimeConverter createNotNullConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case NULL:
				return (dbzObj) -> null;
			case BOOLEAN:
				return this::convertToBoolean;
			case TINYINT:
				return (dbzObj) -> Byte.parseByte(dbzObj.toString());
			case SMALLINT:
				return (dbzObj) -> Short.parseShort(dbzObj.toString());
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
				return this::convertToInt;
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return this::convertToLong;
			case DATE:
				return this::convertToDate;
			case TIME_WITHOUT_TIME_ZONE:
				return this::convertToTime;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return this::convertToTimestamp;
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return this::convertToLocalTimeZoneTimestamp;
			case FLOAT:
				return this::convertToFloat;
			case DOUBLE:
				return this::convertToDouble;
			case CHAR:
			case VARCHAR:
				return this::convertToString;
			case BINARY:
			case VARBINARY:
				return this::convertToBinary;
			case DECIMAL:
				return createDecimalConverter((DecimalType) type);
			case ROW:
				return createRowConverter((RowType) type);
			case ARRAY:
				return createArrayConverter((ArrayType) type);
			case MAP:
			case MULTISET:
			case RAW:
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}

	private boolean convertToBoolean(Object dbzObj) {
		if (dbzObj instanceof Boolean) {
			return (boolean) dbzObj;
		} else {
			return Boolean.parseBoolean(dbzObj.toString());
		}
	}

	private int convertToInt(Object dbzObj) {
		if (dbzObj instanceof Integer) {
			return (int) dbzObj;
		} else if (dbzObj instanceof Long) {
			return ((Long) dbzObj).intValue();
		} else {
			return Integer.parseInt(dbzObj.toString());
		}
	}

	private long convertToLong(Object dbzObj) {
		if (dbzObj instanceof Integer) {
			return (long) dbzObj;
		} else if (dbzObj instanceof Long) {
			return (long) dbzObj;
		} else {
			return Long.parseLong(dbzObj.toString());
		}
	}

	private double convertToDouble(Object dbzObj) {
		if (dbzObj instanceof Float) {
			return (double) dbzObj;
		} else if (dbzObj instanceof Double) {
			return (double) dbzObj;
		} else {
			return Double.parseDouble(dbzObj.toString());
		}
	}

	private float convertToFloat(Object dbzObj) {
		if (dbzObj instanceof Float) {
			return (float) dbzObj;
		} else if (dbzObj instanceof Double) {
			return ((Double) dbzObj).floatValue();
		} else {
			return Float.parseFloat(dbzObj.toString());
		}
	}

	private int convertToDate(Object dbzObj) {
		return (int) TemporalConversions.toLocalDate(dbzObj).toEpochDay();
	}

	private int convertToTime(Object dbzObj) {
		if (dbzObj instanceof Long) {
			return (int) ((long) dbzObj / 1000);
		} else if (dbzObj instanceof Integer) {
			return (int) dbzObj;
		} else if (dbzObj instanceof java.util.Date) {
			java.util.Date date = (java.util.Date) dbzObj;
			// get number of milliseconds of the day
			return LocalDateTime.ofInstant(date.toInstant(), serverTimeZone).toLocalTime().toSecondOfDay() * 1000;
		}
		// get number of milliseconds of the day
		return TemporalConversions.toLocalTime(dbzObj).toSecondOfDay() * 1000;
	}

	private TimestampData convertToTimestamp(Object dbzObj) {
		if (dbzObj instanceof Long) {
			return TimestampData.fromEpochMillis((Long) dbzObj);
		} else if (dbzObj instanceof java.util.Date) {
			java.util.Date date = (java.util.Date) dbzObj;
			return TimestampData.fromLocalDateTime(LocalDateTime.ofInstant(date.toInstant(), serverTimeZone));
		}
		LocalDateTime localDateTime = TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone);
		return TimestampData.fromLocalDateTime(localDateTime);
	}

	private TimestampData convertToLocalTimeZoneTimestamp(Object dbzObj) {
		if (dbzObj instanceof String) {
			String str = (String) dbzObj;
			// TIMESTAMP type is encoded in string type
			Instant instant = Instant.parse(str);
			return TimestampData.fromLocalDateTime(LocalDateTime.ofInstant(instant, serverTimeZone));
		}
		throw new IllegalArgumentException("Unable to convert to TimestampData from unexpected value '" + dbzObj + "' of type " + dbzObj.getClass().getName());
	}

	private StringData convertToString(Object dbzObj) {
		return StringData.fromString(dbzObj.toString());
	}

	private byte[] convertToBinary(Object dbzObj) {
		if (dbzObj instanceof Binary) {
			return ((Binary) dbzObj).getData();
		} else if (dbzObj instanceof byte[]) {
			return (byte[]) dbzObj;
		} else {
			throw new UnsupportedOperationException("Unsupported BYTES value type: " + dbzObj.getClass().getSimpleName());
		}
	}

	private DeserializationRuntimeConverter createDecimalConverter(DecimalType decimalType) {
		final int precision = decimalType.getPrecision();
		final int scale = decimalType.getScale();
		return (dbzObj) -> {
			BigDecimal bigDecimal;
			if (dbzObj instanceof Integer) {
				bigDecimal = new BigDecimal(((Integer) dbzObj).intValue());
			} else if (dbzObj instanceof Decimal128) {
				bigDecimal = ((Decimal128) dbzObj).bigDecimalValue();
			} else if (dbzObj instanceof String) {
				// decimal.handling.mode=string
				bigDecimal = new BigDecimal((String) dbzObj);
			} else if (dbzObj instanceof Double) {
				// decimal.handling.mode=double
				bigDecimal = BigDecimal.valueOf((Double) dbzObj);
			} else {
				SpecialValueDecimal decimal = VariableScaleDecimal.toLogical((Struct) dbzObj);
				bigDecimal = decimal.getDecimalValue().orElse(BigDecimal.ZERO);
			}
			return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
		};
	}

	private DeserializationRuntimeConverter createRowConverter(RowType rowType) {
		final DeserializationRuntimeConverter[] fieldConverters = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.map(this::createConverter)
			.toArray(DeserializationRuntimeConverter[]::new);
		final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
		final List<RowType.RowField> fields = rowType.getFields();

		return (dbzObj) -> {
			Document struct;
			if (dbzObj instanceof Document) {
				struct = (Document) dbzObj;
			} else {
				struct = Document.parse((String) dbzObj);
			}
			int arity = fieldNames.length;
			GenericRowData row = new GenericRowData(arity);
			for (int i = 0; i < arity; i++) {
				String fieldName = fieldNames[i];
				Object fieldValue = struct.get(fieldName);
				Object convertedField = convertField(fieldConverters[i], fieldValue);
				row.setField(i, convertedField);
			}
			return row;
		};
	}

	private DeserializationRuntimeConverter createArrayConverter(ArrayType arrayType) {

		DeserializationRuntimeConverter elementConverter = this.createNotNullConverter(arrayType.getElementType());
		Class<?> elementClass = LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
		return (dbzObj) -> {
			if (!(dbzObj instanceof List)) {
				throw new UnsupportedOperationException("Unsupported ARRAY value type: " + dbzObj.getClass().getSimpleName());
			}
			List objList = (List) dbzObj;
			Object[] array = ((Object[]) Array.newInstance(elementClass, objList.size()));

			for (int i = 0; i < objList.size(); ++i) {
				array[i] = elementConverter.convert(objList.get(i));
			}
			return new GenericArrayData(array);
		};
	}

	private Object convertField(
			DeserializationRuntimeConverter fieldConverter,
			Object fieldValue) throws Exception {
		if (fieldValue == null) {
			return null;
		} else {
			return fieldConverter.convert(fieldValue);
		}
	}

	private DeserializationRuntimeConverter wrapIntoNullableConverter(
		DeserializationRuntimeConverter converter) {
		return (dbzObj) -> {
			if (dbzObj == null) {
				return null;
			}
			return converter.convert(dbzObj);
		};
	}
}
