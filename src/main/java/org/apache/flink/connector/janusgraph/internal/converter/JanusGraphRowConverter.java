package org.apache.flink.connector.janusgraph.internal.converter;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/** JanusGraph row converter. */
public class JanusGraphRowConverter implements Serializable {

    private final RowType rowType;

    private final SerializationConverter[] toExternalConverters;

    public JanusGraphRowConverter(RowType rowType) {
        this.rowType = Preconditions.checkNotNull(rowType);
        LogicalType[] logicalTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        this.toExternalConverters = new SerializationConverter[rowType.getFieldCount()];

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.toExternalConverters[i] = createToExternalConverter(logicalTypes[i]);
        }
    }

    public Object[] toExternal(RowData rowData) {
        int len = rowData.getArity();
        Object[] objects = new Object[len];
        for (int index = 0; index < len; index++) {
            if (!rowData.isNullAt(index)) {
                objects[index] = toExternalConverters[index].serialize(rowData, index);
            }
        }
        return objects;
    }

    private SerializationConverter createToExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return RowData::getBoolean;
            case FLOAT:
                return RowData::getFloat;
            case DOUBLE:
                return RowData::getDouble;
            case INTERVAL_YEAR_MONTH:
            case INTEGER:
                return RowData::getInt;
            case INTERVAL_DAY_TIME:
            case BIGINT:
                return RowData::getLong;
            case TINYINT:
                return RowData::getByte;
            case SMALLINT:
                return RowData::getShort;
            case CHAR:
            case VARCHAR:
                return (val, index) -> val.getString(index).toString();
            case BINARY:
            case VARBINARY:
                return RowData::getBinary;
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index) -> JanusGraphConverterUtil.toExternal(val.getInt(index), type);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, index) ->
                        JanusGraphConverterUtil.toExternal(
                                val.getTimestamp(index, timestampPrecision), type);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int localZonedTimestampPrecision =
                        ((LocalZonedTimestampType) type).getPrecision();
                return (val, index) ->
                        JanusGraphConverterUtil.toExternal(
                                val.getTimestamp(index, localZonedTimestampPrecision), type);
            case ARRAY:
                return (val, index) ->
                        JanusGraphConverterUtil.toExternal(val.getArray(index), type);
            case MAP:
                return (val, index) -> JanusGraphConverterUtil.toExternal(val.getMap(index), type);
            case ROW:
                return (val, index) ->
                        JanusGraphConverterUtil.toExternal(
                                val.getRow(index, ((RowType) type).getFieldCount()), type);
            case DECIMAL:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @FunctionalInterface
    interface SerializationConverter extends Serializable {
        /** Convert an internal field to JanusGraph object. */
        Object serialize(RowData rowData, int index);
    }
}
