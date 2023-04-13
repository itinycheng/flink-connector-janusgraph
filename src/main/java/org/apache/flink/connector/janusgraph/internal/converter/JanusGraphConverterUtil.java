package org.apache.flink.connector.janusgraph.internal.converter;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.types.Row;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.janusgraph.util.JanusGraphUtil.getFlinkZoneId;

/** Convert between internal and external data types. */
public class JanusGraphConverterUtil {
    public static Object toExternal(Object value, LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
            case BIGINT:
            case INTERVAL_DAY_TIME:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case VARBINARY:
                return value;
            case CHAR:
            case VARCHAR:
                return value.toString();
            case DATE:
                ZonedDateTime zonedDateTime =
                        LocalDate.ofEpochDay((Integer) value).atStartOfDay(getFlinkZoneId());
                return Date.from(zonedDateTime.toInstant());
            case TIME_WITHOUT_TIME_ZONE:
                return new Date((Integer) value);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Date.from(((TimestampData) value).toInstant());
            case ARRAY:
                LogicalType elementType = ((ArrayType) type).getChildren().get(0);
                ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
                ArrayData arrayData = ((ArrayData) value);
                Object[] objectArray = new Object[arrayData.size()];
                for (int i = 0; i < arrayData.size(); i++) {
                    objectArray[i] =
                            toExternal(elementGetter.getElementOrNull(arrayData, i), elementType);
                }
                return objectArray;
            case MAP:
                LogicalType keyType = ((MapType) type).getKeyType();
                LogicalType valueType = ((MapType) type).getValueType();
                ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
                ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
                MapData mapData = (MapData) value;
                ArrayData keyArrayData = mapData.keyArray();
                ArrayData valueArrayData = mapData.valueArray();
                Map<Object, Object> objectMap = new HashMap<>(keyArrayData.size());
                for (int i = 0; i < keyArrayData.size(); i++) {
                    objectMap.put(
                            toExternal(keyGetter.getElementOrNull(keyArrayData, i), keyType),
                            toExternal(valueGetter.getElementOrNull(valueArrayData, i), valueType));
                }
                return objectMap;
            case ROW:
                RowData rowData = (RowData) value;
                List<LogicalType> childrenTypes = type.getChildren();
                Row row = new Row(childrenTypes.size());
                for (int i = 0; i < rowData.getArity(); i++) {
                    LogicalType childType = childrenTypes.get(i);
                    RowData.FieldGetter fieldGetter = RowData.createFieldGetter(childType, i);
                    row.setField(i, toExternal(fieldGetter.getFieldOrNull(rowData), childType));
                }
                return row;
            case DECIMAL:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
