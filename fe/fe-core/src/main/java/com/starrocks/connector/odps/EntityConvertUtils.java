// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.odps;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.table.optimizer.predicate.CompoundPredicate;
import com.aliyun.odps.table.optimizer.predicate.Predicate;
import com.aliyun.odps.table.optimizer.predicate.RawPredicate;
import com.aliyun.odps.table.optimizer.predicate.UnaryPredicate;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EntityConvertUtils {

    public static Type convertType(TypeInfo typeInfo) {
        switch (typeInfo.getOdpsType()) {
            case BIGINT:
                return Type.BIGINT;
            case INT:
                return Type.INT;
            case SMALLINT:
                return Type.SMALLINT;
            case TINYINT:
                return Type.TINYINT;
            case FLOAT:
                return Type.FLOAT;
            case DECIMAL:
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
                return ScalarType.createUnifiedDecimalType(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
            case DOUBLE:
                return Type.DOUBLE;
            case CHAR:
                CharTypeInfo charTypeInfo = (CharTypeInfo) typeInfo;
                return ScalarType.createCharType(charTypeInfo.getLength());
            case VARCHAR:
                VarcharTypeInfo varcharTypeInfo = (VarcharTypeInfo) typeInfo;
                return ScalarType.createVarcharType(varcharTypeInfo.getLength());
            case STRING:
            case JSON:
                return ScalarType.createDefaultCatalogString();
            case BINARY:
                return Type.VARBINARY;
            case BOOLEAN:
                return Type.BOOLEAN;
            case DATE:
                return Type.DATE;
            case TIMESTAMP:
            case DATETIME:
                return Type.DATETIME;
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                return new MapType(convertType(mapTypeInfo.getKeyTypeInfo()),
                        convertType(mapTypeInfo.getValueTypeInfo()));
            case ARRAY:
                ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) typeInfo;
                return new ArrayType(convertType(arrayTypeInfo.getElementTypeInfo()));
            case STRUCT:
                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                List<Type> fieldTypeList =
                        structTypeInfo.getFieldTypeInfos().stream().map(EntityConvertUtils::convertType)
                                .collect(Collectors.toList());
                return new StructType(fieldTypeList);
            default:
                return Type.VARCHAR;
        }
    }

    public static Column convertColumn(com.aliyun.odps.Column column) {
        return new Column(column.getName(), convertType(column.getTypeInfo()), true);
    }

    public static List<Column> getFullSchema(com.aliyun.odps.Table odpsTable) {
        TableSchema tableSchema = odpsTable.getSchema();
        List<com.aliyun.odps.Column> columns = new ArrayList<>(tableSchema.getColumns());
        columns.addAll(tableSchema.getPartitionColumns());
        return columns.stream().map(EntityConvertUtils::convertColumn).collect(
                Collectors.toList());
    }

    public static Predicate convertPredicate(ScalarOperator predicate) {
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicateOperator = (BinaryPredicateOperator) predicate;
            return new RawPredicate(convertPredicate(binaryPredicateOperator.getChild(0)) +
                    binaryPredicateOperator.getBinaryType().toString() +
                    convertPredicate(binaryPredicateOperator.getChild(1)));
        } else if (predicate instanceof ColumnRefOperator) {
            return new RawPredicate(((ColumnRefOperator) predicate).getName());
        } else if (predicate instanceof ConstantOperator) {
            if (predicate.getType().isStringType() || predicate.getType().isChar() ||
                    predicate.getType().isVarchar()) {
                return new RawPredicate("'" + predicate + "'");
            }
            return new RawPredicate(predicate.toString());
        } else if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicate compoundPredicate;
            switch (((CompoundPredicateOperator) predicate).getCompoundType()) {
                case AND:
                    compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.AND);
                    break;
                case OR:
                    compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.OR);
                    break;
                case NOT:
                    compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.NOT);
                    break;
                default:
                    return Predicate.NO_PREDICATE;
            }
            for (ScalarOperator child : predicate.getChildren()) {
                compoundPredicate.addPredicate(convertPredicate(child));
            }
            return compoundPredicate;
        } else if (predicate instanceof IsNullPredicateOperator) {
            return new UnaryPredicate(
                    ((IsNullPredicateOperator) predicate).isNotNull() ? UnaryPredicate.Operator.NOT_NULL :
                            UnaryPredicate.Operator.IS_NULL, convertPredicate(predicate.getChild(0)));
        } else {
            return Predicate.NO_PREDICATE;
        }
    }
}
